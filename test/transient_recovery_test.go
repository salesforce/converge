package test

// TestTransientRecovery proves the two ENDS of the transient-failure path over the
// real Connect broker↔worker tier:
//
//  1. RECOVERY — a task that fails transiently a few times then succeeds must
//     EVENTUALLY reconcile (the reaper re-pends after RetryAfter; each re-claim
//     gives the handler another try). No dead-letter, no stranding.
//
//  2. POISON-PILL — a task that fails transiently FOREVER must NOT retry
//     forever: at MaxTransientAttempts the broker escalates the transient failure
//     to TERMINAL (dead-letter → failure_terminal, phase='failed'), so the
//     scheduler stops re-queuing it and it can't burn cluster capacity indefinitely.
//
// Both run through the production seams: broker claims → Connect WorkStream → worker
// handler → Connect Complete → fenced AppendOutbox → drainer applies. The only
// test-specific piece is a handler whose per-resource failure budget the test
// controls.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/broker"
	"github.com/salesforce/converge/internal/engine"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/sdk-go/converge"
	"github.com/salesforce/converge/sdk-go/workerpb/workerpbconnect"
)

// noopLikeManifest is the inline CRD for a Worker-only test kind: a single "work"
// reaction on a spec change emitting status (no children/rollup/finalizer). Used by
// the focused HA/ops/transient tests that register ad-hoc kinds via reg.AddKind.
func noopLikeManifest(kind model.Kind) model.KindManifest {
	return model.KindManifest{
		Kind:        kind,
		KindVersion: 1,
		SpecSchema:  json.RawMessage(`{"type":"object"}`),
		Reactions: []model.ReactionDecl{
			{Name: "work", Trigger: model.TriggerSpecChange, Emits: model.OutcomeMask{model.OutcomeStatus}},
		},
	}
}

// requireEventually polls cond every 250ms until it returns true or the timeout
// elapses, failing the test with msg on timeout. A small shared helper for the
// focused tests' DB-state assertions.
func requireEventually(t *testing.T, ctx context.Context, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			t.Fatalf("context cancelled while waiting: %s", msg)
		}
		if cond() {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timeout after %v: %s", timeout, msg)
}

// flakyCountingHandler is a converge.Provider that fails the first `failFor` Work
// calls per resource with a TRANSIENT error, then succeeds. failFor = a huge number →
// always fails (the dead-letter case). Counts are per-resource so two roots are
// independent. It serves the single (kind, v1) pair captured at construction.
type flakyCountingHandler struct {
	kind    model.Kind
	failFor int
	mu      sync.Mutex
	calls   map[string]int
}

// compile-time proof it satisfies the one provider contract.
var _ converge.Provider = (*flakyCountingHandler)(nil)

func newFlakyCountingHandler(kind model.Kind, failFor int) *flakyCountingHandler {
	return &flakyCountingHandler{kind: kind, failFor: failFor, calls: map[string]int{}}
}

// Kind is the single (kind, version) this provider serves.
func (h *flakyCountingHandler) Kind() converge.KindVersion {
	return converge.KindVersion{Kind: converge.Kind(h.kind), Version: 1}
}

// OnConfig is a no-op: this test provider reads no default providerconfig.
func (*flakyCountingHandler) OnConfig(converge.ProviderConfig) {}

// Ready is always true: a pure in-process test provider has no downstream to dial.
func (*flakyCountingHandler) Ready() bool { return true }

func (h *flakyCountingHandler) Work(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	h.mu.Lock()
	n := h.calls[req.Resource.ID.String()]
	h.calls[req.Resource.ID.String()] = n + 1
	h.mu.Unlock()
	if n < h.failFor {
		// plain (non-terminal) error → the core records a TRANSIENT failure and the
		// reaper re-pends after RetryAfter, so the same work retries.
		return converge.Outcome{}, &transientTestErr{}
	}
	return converge.Outcome{Status: json.RawMessage(`{}`)}, nil
}

type transientTestErr struct{}

func (*transientTestErr) Error() string {
	return "transient-recovery-test: simulated transient failure"
}

func TestTransientRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	pool := setupPostgres(t, ctx)

	// A pair of controllable flaky providers, one per kind, so the two roots have
	// independent, unambiguous behavior:
	//   recover → fails 3× then succeeds
	//   poison  → fails forever (> the cap)
	const capN = 5 // low cap so the poison case dead-letters fast
	recoverH := newFlakyCountingHandler("recoverkind", 3)
	poisonH := newFlakyCountingHandler("poisonkind", 1<<30) // effectively always fails

	// One kind can only have one provider, so use TWO kinds sharing the noop manifest
	// shape via inline registration: "recoverkind" and "poisonkind".
	reg := newTReg()
	reg.AddKind(recoverH, noopLikeManifest("recoverkind"))
	reg.AddKind(poisonH, noopLikeManifest("poisonkind"))

	migrateForSeed(t, ctx, pool)
	require.NoError(t, reg.seed(ctx, pool))

	// Control plane: fast RetryAfter so a transient re-pends quickly (keeps the test
	// short); fast stale window so nothing lingers.
	controlDuties, err := engine.DutiesFromConfig(engine.EngineConfig{
		RunControl:        true,
		SweeperStaleAfter: 15 * time.Second,
		SweeperInterval:   500 * time.Millisecond,
		RetryAfter:        1 * time.Second, // re-pend a transient failure quickly
	})
	require.NoError(t, err)
	control := engine.NewEngine(controlDuties, engine.Deps{Pool: pool})
	require.NoError(t, control.Start(ctx))
	t.Cleanup(func() { _ = control.Stop(context.Background()) })

	mc := runtime.NewKindManifestCache(pool)
	require.NoError(t, mc.Load(ctx))
	mc.Start(ctx)
	t.Cleanup(mc.Stop)

	// Lower the poison-pill cap to capN (per-kind, in kind_config — the DB-side
	// drain gate reads it) so the poison case dead-letters after a handful of attempts
	// instead of the default 20. Set AFTER seed (which derived kind_config from the
	// manifest with the default cap).
	_, err = pool.Exec(ctx,
		`UPDATE kind_config SET max_transient_attempts=$1 WHERE kind = ANY($2)`,
		capN, []string{"recoverkind", "poisonkind"})
	require.NoError(t, err)

	srv := broker.NewDispatch(pool, mc, "broker-transient", 16, nil)
	mux := http.NewServeMux()
	mux.Handle(srv.Handler())
	httpSrv := httptest.NewUnstartedServer(mux)
	httpSrv.EnableHTTP2 = true
	httpSrv.StartTLS()
	t.Cleanup(httpSrv.Close)
	brokerCtx, brokerCancel := context.WithCancel(ctx)
	t.Cleanup(brokerCancel)
	go func() { _ = srv.Dispatcher().Run(brokerCtx) }()

	// One worker running both kinds — the SAME provider instances the registry holds,
	// so their per-resource call counts back the test's retry assertions.
	client := workerpbconnect.NewWorkerServiceClient(httpSrv.Client(), httpSrv.URL)
	workerCtx, workerCancel := context.WithCancel(ctx)
	t.Cleanup(workerCancel)
	go func() {
		_ = converge.RunWorker(workerCtx, client, []converge.Provider{recoverH, poisonH}, converge.RunOptions{MaxInflight: 4})
	}()

	te := &testEngine{Pool: pool}

	// ── case 1: RECOVERY — fails 3×, then succeeds → must reconcile ──────────────
	recoverID, err := te.CreateRoot(ctx, "recoverkind", "recover-root", json.RawMessage(`{}`), nil)
	require.NoError(t, err)

	// ── case 2: POISON — fails forever → must dead-letter at the cap ────────────
	poisonID, err := te.CreateRoot(ctx, "poisonkind", "poison-root", json.RawMessage(`{}`), nil)
	require.NoError(t, err)

	// RECOVERY assertion: recoverkind root eventually synced (generation caught).
	requireEventually(t, ctx, 60*time.Second, func() bool {
		var gen, sg int64
		if err := pool.QueryRow(ctx,
			`SELECT generation, synced_gen FROM resources WHERE id=$1`, recoverID).Scan(&gen, &sg); err != nil {
			return false
		}
		return sg >= gen && gen > 0
	}, "recoverkind root never reconciled after its transient failures cleared")

	// It should have taken ≥4 handler calls (3 fails + 1 success) — proves the
	// retries actually happened, not that it succeeded first try.
	recoverH.mu.Lock()
	rc := recoverH.calls[recoverID.String()]
	recoverH.mu.Unlock()
	require.GreaterOrEqualf(t, rc, 4, "recover root should have retried (≥4 calls); got %d", rc)
	t.Logf("RECOVERY: recover-root reconciled after %d handler calls (3 transient fails + success)", rc)

	// POISON assertion: poisonkind root dead-letters — failure_terminal set with
	// failure_gen == generation (phase='failed'), and it STOPS retrying (bounded
	// attempts near the cap, not unbounded).
	requireEventually(t, ctx, 60*time.Second, func() bool {
		var terminal bool
		var failGen, gen int64
		if err := pool.QueryRow(ctx,
			`SELECT failure_terminal, failure_gen, generation FROM resources WHERE id=$1`, poisonID).
			Scan(&terminal, &failGen, &gen); err != nil {
			return false
		}
		return terminal && failGen == gen
	}, "poisonkind root never dead-lettered at the transient cap (poison-pill missing?)")

	// After dead-lettering, attempts must be BOUNDED near the cap — not runaway.
	// Give it a moment to ensure no further retries sneak in, then check the count.
	time.Sleep(3 * time.Second)
	poisonH.mu.Lock()
	pc := poisonH.calls[poisonID.String()]
	poisonH.mu.Unlock()
	require.LessOrEqualf(t, pc, capN+2, "poison root retried %d× — expected ~cap (%d); the poison-pill didn't stop it", pc, capN)
	t.Logf("POISON: poison-root dead-lettered after %d handler calls (cap=%d) — retries STOPPED", pc, capN)
}
