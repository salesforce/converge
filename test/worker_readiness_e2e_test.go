package test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/broker"
	"github.com/salesforce/converge/internal/engine"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/sdk-go/converge"
	"github.com/salesforce/converge/sdk-go/workerpb/workerpbconnect"
)

// readinessWorker is a Worker-only test provider whose Ready() reads a controllable
// *atomic.Bool, so a test can advertise a kind as UNREADY (its downstream is "down")
// and later flip it healthy. Work sleeps a small delay then writes a name status —
// enough to make a claim observable while settling quickly. It serves the single
// (kind, v1) pair captured at construction and exposes Manifest() for reg.AddKind.
type readinessWorker struct {
	kind  model.Kind
	delay time.Duration
	ready *atomic.Bool // Ready() returns ready.Load(); the SDK polls it for RS-/RS+
}

// compile-time proof it satisfies the one provider contract.
var _ converge.Provider = (*readinessWorker)(nil)

// Kind is the single (kind, version) this provider serves.
func (w *readinessWorker) Kind() converge.KindVersion {
	return converge.KindVersion{Kind: converge.Kind(w.kind), Version: 1}
}

// OnConfig is a no-op: this test worker reads no default providerconfig.
func (*readinessWorker) OnConfig(converge.ProviderConfig) {}

// Ready reflects the controllable atomic: false → the SDK advertises RS- (the broker
// stops claiming this kind); true → RS+ resumes it.
func (w *readinessWorker) Ready() bool { return w.ready.Load() }

// Work sleeps the small delay (respecting cancellation) then writes a name status.
func (w *readinessWorker) Work(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	if w.delay > 0 {
		select {
		case <-ctx.Done():
			return converge.Outcome{}, ctx.Err()
		case <-time.After(w.delay):
		}
	}
	out, err := json.Marshal(map[string]string{"name": nameFromSpec(req.Resource.Spec)})
	if err != nil {
		return converge.Outcome{}, err
	}
	return converge.Outcome{Status: out}, nil
}

// Manifest is the inline CRD a test seeds via reg.AddKind: a "work" reaction
// (spec change → status).
func (w *readinessWorker) Manifest() model.KindManifest {
	return model.KindManifest{
		Kind:        w.kind,
		KindVersion: 1,
		Reactions: []model.ReactionDecl{
			{Name: "work", Trigger: model.TriggerSpecChange, Emits: model.OutcomeMask{model.OutcomeStatus}},
		},
	}
}

// TestWorkerReadinessGatesOneKind proves the per-kind readiness path end to end
// against a real Postgres + broker + control drainer: ONE worker serves TWO kinds over
// ONE WorkStream, and the "sick" kind reports itself UNREADY via its provider's Ready().
// The broker must then refuse to CLAIM the sick kind's work (its root never syncs)
// while the healthy kind flows normally on the same stream — the "provider 7 of 10's
// downstream (Kafka) is down" scenario a shared stream could not otherwise express.
// When the provider flips healthy, the sick kind's backlog claims + syncs.
//
// This is the E2E companion to internal/broker/readiness_test.go (which proves the
// gate/credit/mesh mechanics directly, without a DB).
func TestWorkerReadinessGatesOneKind(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	// Two fast worker-only kinds served by ONE worker: readyk is always healthy;
	// sickk's readiness is driven by a togglable atomic (the "Kafka" downstream). Both
	// sleep a tiny delay so a claim is observable but settle is quick.
	const readyKind, sickKind = "readyk", "sickk"
	const fast = 20 * time.Millisecond
	alwaysReady := &atomic.Bool{}
	alwaysReady.Store(true)
	sickReady := &atomic.Bool{} // starts false → sickk advertised UNREADY at Subscribe
	readyW := &readinessWorker{kind: readyKind, delay: fast, ready: alwaysReady}
	sickW := &readinessWorker{kind: sickKind, delay: fast, ready: sickReady}

	reg := newTReg()
	reg.AddKind(readyW, readyW.Manifest())
	reg.AddKind(sickW, sickW.Manifest())

	// Control duty only (drainer applies the outbox → synced_gen; reaper backstops
	// leases). No in-process dispatch — all execution goes worker→broker→DB.
	migrateForSeed(t, ctx, pool)
	require.NoError(t, reg.seed(ctx, pool))
	controlDuties, err := engine.DutiesFromConfig(engine.EngineConfig{RunControl: true})
	require.NoError(t, err)
	control := engine.NewEngine(controlDuties, engine.Deps{Pool: pool})
	require.NoError(t, control.Start(ctx))
	t.Cleanup(func() { _ = control.Stop(context.Background()) })

	mc := runtime.NewKindManifestCache(pool)
	require.NoError(t, mc.Load(ctx))
	mc.Start(ctx)
	t.Cleanup(mc.Stop)

	// The broker owns all shards; claims the reconcile pair for BOTH kinds.
	const brokerID = "broker-readiness-e2e"
	cl := broker.New(pool, mc, brokerID, 16)
	cl.Dispatcher().AddPair(readyKind, 1, store.TaskReconcile)
	cl.Dispatcher().AddPair(sickKind, 1, store.TaskReconcile)
	mux := http.NewServeMux()
	mux.Handle(cl.Handler())
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	claimCtx, claimCancel := context.WithCancel(ctx)
	t.Cleanup(claimCancel)
	go func() { _ = cl.Dispatcher().Run(claimCtx) }()

	// ONE worker (the dumb-worker client) serves BOTH kinds over ONE stream. sickk
	// starts UNREADY (its downstream is down): the SDK's readiness poller derives
	// InitialUnready from Ready()=false and advertises the RS- in the Subscribe burst,
	// so the broker's claim gate refuses sickk before any task. On recovery the test
	// flips its atomic ready and the poller sends RS+ within the poll interval.
	client := workerpbconnect.NewWorkerServiceClient(srv.Client(), srv.URL)
	workerCtx, workerCancel := context.WithCancel(ctx)
	t.Cleanup(workerCancel)
	go func() {
		_ = converge.RunWorker(workerCtx, client, []converge.Provider{readyW, sickW}, converge.RunOptions{MaxInflight: 8})
	}()

	te := &testEngine{Pool: pool}
	readyRoot, err := te.CreateRoot(ctx, readyKind, "ready-root", json.RawMessage(`{}`), nil)
	require.NoError(t, err)
	sickRoot, err := te.CreateRoot(ctx, sickKind, "sick-root", json.RawMessage(`{}`), nil)
	require.NoError(t, err)

	// (1) The healthy kind settles: proof the worker is connected + serving, and that
	// the sick kind's RS- does NOT gate its healthy sibling on the same stream.
	requireSynced(t, ctx, pool, readyRoot, 30*time.Second, "healthy kind (readyk) must sync")

	// (2) The sick kind must NOT sync while unready: the broker's claim gate
	// (readySubscribers) refuses to pull sickk work off Postgres. Give it a window
	// well past when readyk synced — if the gate leaked, sickk would have synced too.
	requireNotSynced(t, ctx, pool, sickRoot, 4*time.Second, "sick kind (sickk) must NOT sync while its worker is RS-")

	// (3) Recover: flip sickk healthy. The SDK's readiness poller observes Ready()=true
	// and sends RS+ up the stream within the poll interval; the gate opens, the broker
	// claims the queued sickk root, and it syncs.
	sickReady.Store(true)
	requireSynced(t, ctx, pool, sickRoot, 30*time.Second, "sick kind (sickk) must sync after RS+ recovery")
}

// requireSynced polls until the resource's synced_gen has caught up to its generation
// (the chain settled) or the deadline passes.
func requireSynced(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, within time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		gen, synced := genSynced(t, ctx, pool, id)
		if gen > 0 && synced >= gen {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	gen, synced := genSynced(t, ctx, pool, id)
	t.Fatalf("%s: not synced within %s (generation=%d synced_gen=%d)", msg, within, gen, synced)
}

// requireNotSynced asserts the resource stays UNSYNCED for the whole window (its work
// was never claimed) — the gate-closed proof.
func requireNotSynced(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, window time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		gen, synced := genSynced(t, ctx, pool, id)
		if gen > 0 && synced >= gen {
			t.Fatalf("%s: unexpectedly synced (generation=%d synced_gen=%d) — the readiness gate leaked", msg, gen, synced)
		}
		time.Sleep(150 * time.Millisecond)
	}
}

// genSynced reads a resource's (generation, synced_gen).
func genSynced(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) (gen, synced int64) {
	t.Helper()
	err := pool.QueryRow(ctx, `SELECT generation, synced_gen FROM resources WHERE id = $1`, id).Scan(&gen, &synced)
	require.NoError(t, err)
	return gen, synced
}
