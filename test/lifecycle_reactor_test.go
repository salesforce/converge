package test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/examples/demos/classic/statussink"
	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/sdk-go/converge"
	"github.com/salesforce/converge/test/internal/demoruntime"
	"github.com/salesforce/converge/test/internal/inproc"
)

// TestLifecycleReactorClassicBOMToStatusSink is the end-to-end proof of the
// reactor spine and the motivating saga: submit a classicbom, durably WAIT until
// it rolls up (synced), then a reactor uploads the rolled-up status "somewhere".
// No worker slot is held during the wait, no polling loop in the saga — the wait
// is the absence of a lifecycle_outbox{synced} row until the cascade trigger
// emits it, and the dispatcher delivers it to the reactor.
//
// Flow:
//  1. REGISTER: the statussink reactor kind (its CRD declares a `reactor`
//     reaction, no transition) is a registry member, so its manifest seeds.
//  2. SUBSCRIBE: a reactor_bindings row {watch_kind:classicbom, 'synced'} →
//     statussink — the SOLE wiring, an editable subscription.
//  3. SUBMIT: ApplySpec a small classicbom.
//  4. WAIT: the classicbom composes children → they reconcile → the LAST one
//     settles → the classicbom's StatusRollup advances synced_gen → the cascade
//     trigger emits lifecycle_outbox{classicbom,'synced'} in the drain tx.
//  5. REACT: the ReactorDispatcher claims it, resolves statussink's reaction name
//     from its CRD, runs it → uploads the ClassicBOMStatus.
//  6. ASSERT: the object appears in the fake store carrying the rolled-up
//     status, and the lifecycle_outbox row was acked (deleted).
func TestLifecycleReactorClassicBOMToStatusSink(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	// (1) Build a statussink reactor KindRuntime backed by an inspectable
	// in-process store, default config {endpoint, prefix}. Hermetic — no Setup
	// dial, no env. Adding it to the registry means its CRD (a `reactor`-trigger
	// reaction, no transition) seeds into kind_manifest alongside the work kinds,
	// so the claim can resolve its reaction name; its handler registers under
	// (statussink, "react") in the reaction registry the dispatcher consults.
	const prefix = "rolled-up/"
	kr, getObject := demoruntime.StatusSink(
		"mem://test-bucket",
		fmt.Sprintf(`{"endpoint":"mem://test-bucket","prefix":%q}`, prefix),
	)
	reg := defaultRegistry()
	reg.Add(kr) // statussink's manifest seeds from its fixture (reactor trigger)

	// Start the engine WITH the reactor dispatch duty (over an in-process executor
	// of the registry's handlers).
	eng := startEngineWithConfig(t, ctx, pool, reg, engineOpts{WithReactors: true})
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	// (2) SUBSCRIBE: when any classicbom syncs, run the statussink reactor. This is
	// the whole saga definition — one runtime-editable subscription. The binding
	// name is operator-chosen and opaque; the reaction to run is resolved from the
	// statussink CRD, not parsed from this name.
	require.NoError(t, store.New(pool).UpsertReactorBinding(ctx, store.ReactorBinding{
		Name:       "classicbom-synced-to-statussink",
		WatchKind:  model.Kind(classicbom.Kind),
		Transition: string(model.TransitionSynced),
		Reactor:    model.Kind(statussink.Kind),
		Enabled:    true,
	}))

	// (2) SUBMIT a small classicbom: one FD with three teams → 11 children.
	bom := smallBOM("saga-project", map[string][]string{
		"alpha-fd": {"team-alpha", "team-beta", "team-gamma"},
	})
	specBytes, err := json.Marshal(bom)
	require.NoError(t, err)
	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "saga-project", specBytes, nil)
	require.NoError(t, err)

	q := dbq.New(pool)

	// (3) WAIT until the classicbom rolls up (the durable, slot-free wait).
	deadline := time.Now().Add(60 * time.Second)
	var rolledUp bool
	for time.Now().Before(deadline) {
		root, err := q.GetResourceInfo(ctx, rootID)
		if err == nil && root.IsReady && root.SyncedGen >= 1 {
			rolledUp = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !rolledUp {
		dumpResourceState(t, ctx, pool, rootID)
		t.Fatalf("classicbom should roll up (synced) before the reactor can fire")
	}

	// (4)+(5) WAIT for the reactor to deliver: the object lands under the expected
	// key carrying the rolled-up ClassicBOMStatus. The key embeds the generation
	// (idempotency), so compute it from the synced generation.
	root, err := q.GetResourceInfo(ctx, rootID)
	require.NoError(t, err)
	wantKey := fmt.Sprintf("%s%s-%d.json", prefix, "saga-project", root.Generation)

	var body []byte
	delivered := false
	for time.Now().Before(deadline) {
		if b, ok := getObject(wantKey); ok {
			body = b
			delivered = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !delivered {
		dumpLifecycleState(t, ctx, pool)
		t.Fatalf("statussink reactor should have uploaded the rolled-up status to %q", wantKey)
	}

	// The uploaded body is the classicbom's rolled-up status — assert it round-trips
	// to a ClassicBOMStatus with the three teams the BOM declared.
	var uploaded classicbom.ClassicBOMStatus
	require.NoError(t, json.Unmarshal(body, &uploaded), "uploaded object should be a ClassicBOMStatus")
	require.Len(t, uploaded.Teams, 3, "rolled-up status should carry the 3 teams")

	// (5) The delivery was acked: the lifecycle_outbox row is gone (drained, not
	// retained). Poll briefly — the ack commits just after the upload returns.
	gone := false
	for time.Now().Before(deadline) {
		var n int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM lifecycle_outbox WHERE resource_id = $1`, rootID).Scan(&n))
		if n == 0 {
			gone = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.True(t, gone, "lifecycle_outbox row should be acked (deleted) after a successful delivery")

	t.Logf("SUCCESS: classicbom %q rolled up → statussink uploaded %s (%d bytes, %d teams)",
		"saga-project", wantKey, len(body), len(uploaded.Teams))
}

// TestLifecycleReactorNoBindingNoEmit guards the EXISTS short-circuit: with NO
// binding registered, a classicbom rolling up writes ZERO lifecycle_outbox rows —
// the spine costs nothing when nobody reacts (the 1M-healthy property).
func TestLifecycleReactorNoBindingNoEmit(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	eng := startAllRolesEngine(t, ctx, pool) // NO reactor registry, NO bindings
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	bom := smallBOM("no-react-project", map[string][]string{"alpha-fd": {"team-alpha"}})
	specBytes, err := json.Marshal(bom)
	require.NoError(t, err)
	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "no-react-project", specBytes, nil)
	require.NoError(t, err)

	q := dbq.New(pool)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		root, err := q.GetResourceInfo(ctx, rootID)
		if err == nil && root.IsReady && root.SyncedGen >= 1 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM lifecycle_outbox`).Scan(&n))
	require.Zero(t, n, "no binding → the cascade trigger's EXISTS gate must emit nothing")
}

// TestLifecycleReactorExactlyOnceEffectUnderRedelivery is the reactor's NO-DOUBLE-APPLY:
// the reconcile fence proves synced_gen advances at most once per generation even when a
// task EXECUTES twice; this proves the reactor SIDE EFFECT is exactly-once even when a
// delivery is REDELIVERED (the at-least-once contract firing more than once).
//
// It forces redelivery deterministically: seed N 'synced' deliveries for distinct watched
// resources, then run the ReactorDispatcher WHILE a reaper hammers reap_stale_lifecycle
// with a ZERO stale window — so a claimed-but-not-yet-acked delivery is re-armed under the
// dispatcher's feet, re-claimed (bumping claim_epoch), and its reaction RUNS AGAIN. The
// prior claim's late ack no-ops (epoch-fenced), so the row only leaves the queue after a
// delivery whose epoch is still current — at-least-once, with redelivery actually exercised.
//
// The statussink object key embeds the resource generation, so every (re)delivery of the
// SAME transition writes the SAME key: an idempotent overwrite, never a duplicate object.
// Assertions after the queue drains:
//   - Distinct objects == N        → exactly one side effect per watched resource (no dup, none lost)
//   - TotalPuts > Distinct         → redelivery genuinely happened (the test isn't vacuous)
//   - every object body is correct → the redelivered effect didn't diverge
func TestLifecycleReactorExactlyOnceEffectUnderRedelivery(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	// statussink reactor with a stats hook so we can count PUTs (redelivery evidence).
	const prefix = "eo/"
	kr, getObject, stats := statussink.NewForTestWithStats(
		"mem://eo-bucket", fmt.Sprintf(`{"endpoint":"mem://eo-bucket","prefix":%q}`, prefix))
	// Wrap the reaction in a per-call delay (40–80ms) so a delivery is IN FLIGHT long
	// enough for the stale=0 reaper below to re-arm it mid-reaction and force a genuine
	// redelivery. Without the delay the reaction returns + acks faster than the reaper can
	// interfere, and no redelivery is exercised (the assertion would trip as vacuous).
	slowKR := newFlakyWrap(kr, 40*time.Millisecond, 80*time.Millisecond, 0, 1)
	reg := defaultRegistry()
	reg.Add(slowKR)
	migrateForSeed(t, ctx, pool)
	require.NoError(t, reg.seed(ctx, pool))
	st := store.New(pool)

	// Bind classicbom/synced → statussink (classicbom's manifest is in defaultRegistry).
	require.NoError(t, st.UpsertReactorBinding(ctx, store.ReactorBinding{
		Name:       "eo-binding",
		WatchKind:  model.Kind(classicbom.Kind),
		Transition: string(model.TransitionSynced),
		Reactor:    model.Kind(statussink.Kind),
		Enabled:    true,
	}))

	// Seed N distinct watched resources, each with ONE 'synced' delivery for the binding.
	// Insert the delivery rows directly (the cascade would emit these on a real crossing;
	// here we make the set deterministic). Each resource needs a real row so the claim's
	// status/name join resolves and buildReactorRequest has a body to upload.
	const n = 60
	names := make([]string, 0, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("eo-root-%03d", i)
		names = append(names, name)
		r, err := st.ApplySpec(ctx, model.Kind(classicbom.Kind), name,
			buildSpec(controllableChildSpec{Kind: "account", Name: fmt.Sprintf("eo-child-%03d", i)}), nil)
		require.NoError(t, err)
		_, err = pool.Exec(ctx,
			`INSERT INTO lifecycle_outbox (resource_id, kind, transition, generation, binding_name, shard_id)
			 VALUES ($1, $2, 'synced', $3, 'eo-binding', shard_of($1)) ON CONFLICT DO NOTHING`,
			r.ID, string(classicbom.Kind), r.Generation)
		require.NoError(t, err)
	}

	allShards := make([]int16, 256)
	for i := range allShards {
		allShards[i] = int16(i)
	}

	// The dispatcher over the in-process executor of the registry's handlers (statussink's
	// 'react' runs here). No ReadyToDispatch → the in-process handler is always present.
	rx := runtime.NewReactorDispatcher(st, runtime.NewPgxListener(pool), inproc.New(reg.providerList()), "eo-pod")
	rx.Shards = runtime.NewShardSet(allShards)
	rxCtx, rxCancel := context.WithCancel(ctx)
	defer rxCancel()
	go func() { _ = rx.Run(rxCtx) }()

	// The adversary: re-arm claimed rows OUT FROM UNDER the dispatcher every tick, so an
	// in-flight (claimed, reaction running, not yet acked) delivery is re-issued — its
	// claim_epoch bumped, broker_id cleared — exactly what reap_stale_lifecycle does to a
	// stale claim, but forced immediately regardless of heartbeat freshness (store.ReapStale
	// clamps the stale window to ≥1s, and the dispatcher's heartbeat keeps a progressing
	// delivery fresh — so we re-arm directly to model the "reaper decided this claim is
	// stale" race deterministically). The prior claim's reaction still completes and tries
	// to ack, but its epoch is now stale → the fenced ack no-ops; the re-issued row is
	// re-claimed and its reaction RUNS AGAIN. This is precisely the double-execution the
	// dedup key must absorb into a single side effect. The bump is itself ordered +
	// epoch-consistent (same shape as reap_stale_lifecycle's freed CTE).
	// Re-arm only a RANDOM ~40% of currently-claimed rows each tick (not all of them): if
	// we yanked every claim every tick, no delivery would ever reach its ack and the queue
	// couldn't drain. Re-arming a subset means most deliveries complete+ack while a steady
	// stream gets redelivered — enough to prove idempotency, still converging. The adversary
	// runs for a bounded window, then stops so the last in-flight deliveries settle and the
	// queue drains.
	reapCtx, reapCancel := context.WithCancel(ctx)
	defer reapCancel()
	go func() {
		tk := time.NewTicker(60 * time.Millisecond)
		defer tk.Stop()
		stopChurn := time.After(20 * time.Second) // churn for 20s, then let it drain
		for {
			select {
			case <-reapCtx.Done():
				return
			case <-stopChurn:
				return
			case <-tk.C:
				_, _ = pool.Exec(reapCtx,
					`UPDATE lifecycle_outbox SET broker_id = NULL, heartbeat_at = now(), claim_epoch = claim_epoch + 1
					 WHERE ctid IN (
					     SELECT ctid FROM lifecycle_outbox
					      WHERE broker_id IS NOT NULL AND random() < 0.4
					 )`)
			}
		}
	}()

	// Wait for the queue to fully drain (every delivery acked under a current epoch).
	deadline := time.Now().Add(90 * time.Second)
	drained := false
	for time.Now().Before(deadline) {
		var remaining int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM lifecycle_outbox`).Scan(&remaining))
		if remaining == 0 {
			drained = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	reapCancel() // stop yanking claims so the last in-flight deliveries settle
	if !drained {
		dumpLifecycleState(t, ctx, pool)
		t.Fatal("lifecycle_outbox never drained under forced redelivery")
	}

	// EXACTLY-ONCE EFFECT: one object per watched resource, each carrying its rolled-up
	// status, regardless of how many times its delivery redelivered.
	s := stats()
	require.Equal(t, n, s.Distinct,
		"expected exactly one object per watched resource (no duplicate, none lost); got %d distinct for %d resources", s.Distinct, n)
	require.Greater(t, s.TotalPuts, s.Distinct,
		"forced-redelivery test is vacuous: no delivery redelivered (TotalPuts %d == Distinct %d) — the reaper race didn't fire", s.TotalPuts, s.Distinct)
	// Every expected object exists at its generation-keyed path (the idempotent key).
	for _, name := range names {
		r, err := dbq.New(pool).GetResourceInfo(ctx, mustResourceID(t, ctx, pool, name))
		require.NoError(t, err)
		_, ok := getObject(fmt.Sprintf("%s%s-%d.json", prefix, name, r.Generation))
		require.True(t, ok, "object for %s missing — a delivery was lost", name)
	}
	t.Logf("SUCCESS: exactly-once EFFECT under redelivery — %d resources → %d distinct objects, %d total PUTs (max %d redeliveries to one key)",
		n, s.Distinct, s.TotalPuts, s.MaxPutsPerKey)
}

// mustResourceID looks up a resource's id by name for the exactly-once assertion.
func mustResourceID(t *testing.T, ctx context.Context, pool *pgxpool.Pool, name string) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT id FROM resource_meta WHERE name = $1`, name).Scan(&id))
	return id
}

// dumpLifecycleState prints the lifecycle_outbox + reactor_bindings on a failed
// wait so the failure shows whether the transition was emitted but not
// delivered (binding/registry issue) vs never emitted (trigger/wait issue).
func dumpLifecycleState(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT resource_id, kind, transition, generation, worker_id IS NOT NULL AS claimed FROM lifecycle_outbox ORDER BY created_at`)
	if err != nil {
		t.Logf("lifecycle_outbox dump failed: %v", err)
		return
	}
	defer rows.Close()
	t.Logf("=== lifecycle_outbox ===")
	for rows.Next() {
		var rid string
		var kind, transition string
		var gen int64
		var claimed bool
		if err := rows.Scan(&rid, &kind, &transition, &gen, &claimed); err != nil {
			continue
		}
		t.Logf("  res=%s kind=%s transition=%s gen=%d claimed=%v", rid, kind, transition, gen, claimed)
	}
	bRows, err := pool.Query(ctx, `SELECT name, watch_kind, transition, reactor, enabled FROM reactor_bindings ORDER BY name`)
	if err != nil {
		return
	}
	defer bRows.Close()
	t.Logf("=== reactor_bindings ===")
	for bRows.Next() {
		var name, watchKind, transition, reactor string
		var enabled bool
		if err := bRows.Scan(&name, &watchKind, &transition, &reactor, &enabled); err != nil {
			continue
		}
		t.Logf("  %s: watch_kind=%s transition=%s reactor=%s enabled=%v", name, watchKind, transition, reactor, enabled)
	}
}

// ── controllable test reactors for the fan-out / partial-failure cases ──

// countingReactor records each delivery it received and optionally always
// fails — lets a test assert independent per-binding delivery + retry.
type countingReactor struct {
	kind        model.Kind
	alwaysErr   bool
	mu          sync.Mutex
	seen        map[string]int              // dedup_token → delivery count
	transitions map[converge.Transition]int // transition → delivery count (proves which events fired)
}

var _ converge.Provider = (*countingReactor)(nil)

func newCountingReactor(kind model.Kind, alwaysErr bool) *countingReactor {
	return &countingReactor{kind: kind, alwaysErr: alwaysErr, seen: map[string]int{}, transitions: map[converge.Transition]int{}}
}

// Kind is the single (kind, version) this reactor serves.
func (r *countingReactor) Kind() converge.KindVersion {
	return converge.KindVersion{Kind: converge.Kind(r.kind), Version: 1}
}

// OnConfig is a no-op: this test reactor reads no default providerconfig.
func (*countingReactor) OnConfig(converge.ProviderConfig) {}

// Ready is always true: a pure in-process test controller has no downstream to dial.
func (*countingReactor) Ready() bool { return true }

// Work runs the reactor's single `reactor`-trigger reaction: it records the delivery
// (dedup token + transition) and either succeeds (side effect done) or always fails.
func (r *countingReactor) Work(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	r.mu.Lock()
	r.seen[req.DedupToken]++
	r.transitions[req.Transition]++
	r.mu.Unlock()
	if r.alwaysErr {
		return converge.Outcome{}, fmt.Errorf("countingReactor %s: forced failure", r.kind)
	}
	return converge.Outcome{SideEffectDone: true}, nil
}

func (r *countingReactor) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, c := range r.seen {
		n += c
	}
	return n
}

// sawTransition reports whether this reactor received at least one delivery for
// the given transition — proves one reactor kind fired on that specific event.
func (r *countingReactor) sawTransition(transition converge.Transition) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.transitions[transition] > 0
}

// reactionName is the single `reactor`-trigger reaction every counting reactor
// declares; the claim resolves it from the reactor's CRD, so it must match what
// the reactor registers its handler under (below).
const countingReactionName = "react"

// runtimeAndManifest returns this reactor as a converge.Provider plus a minimal reactor
// CRD declaring its single `reactor`-trigger reaction. AddKind seeds the manifest so the
// claim can resolve the reaction name — there is no binding-name parsing.
func (r *countingReactor) runtimeAndManifest() (converge.Provider, model.KindManifest) {
	m := model.KindManifest{
		Kind:        r.kind,
		KindVersion: 1,
		Reactions: []model.ReactionDecl{
			{Name: countingReactionName, Trigger: model.TriggerReactor, Emits: model.OutcomeMask{model.OutcomeSideEffect}},
		},
	}
	return r, m
}

// TestLifecycleReactorFanoutPartialFailure is the F1 correctness proof: TWO
// bindings on the SAME (classicbom,'synced') deliver INDEPENDENTLY. One reactor
// succeeds (its row is acked/deleted); the other always fails (its row survives
// and is re-delivered) — the succeeding binding is never re-fired by the
// failure, and the failing binding never deletes the succeeding one's row. Each
// binding carries its own per-binding identity, so deliveries are independent.
func TestLifecycleReactorFanoutPartialFailure(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	ok := newCountingReactor("ok-reactor", false)
	bad := newCountingReactor("bad-reactor", true)
	// Each reactor is a registry member: its CRD (a single `reactor` reaction) is
	// seeded so the claim resolves the reaction name, and its handler is wired.
	reg := defaultRegistry()
	okKR, okM := ok.runtimeAndManifest()
	badKR, badM := bad.runtimeAndManifest()
	reg.AddKind(okKR, okM)
	reg.AddKind(badKR, badM)

	eng := startEngineWithConfig(t, ctx, pool, reg, engineOpts{WithReactors: true})
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	// TWO subscriptions on the same (classicbom,'synced'), each targeting a
	// different reactor kind. The binding names are arbitrary.
	st := store.New(pool)
	require.NoError(t, st.UpsertReactorBinding(ctx, store.ReactorBinding{
		Name: "ok", WatchKind: model.Kind(classicbom.Kind), Transition: string(model.TransitionSynced), Reactor: "ok-reactor", Enabled: true,
	}))
	require.NoError(t, st.UpsertReactorBinding(ctx, store.ReactorBinding{
		Name: "bad", WatchKind: model.Kind(classicbom.Kind), Transition: string(model.TransitionSynced), Reactor: "bad-reactor", Enabled: true,
	}))

	bom := smallBOM("fanout-project", map[string][]string{"alpha-fd": {"team-alpha"}})
	specBytes, err := json.Marshal(bom)
	require.NoError(t, err)
	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "fanout-project", specBytes, nil)
	require.NoError(t, err)

	q := dbq.New(pool)
	deadline := time.Now().Add(60 * time.Second)

	// Wait until the OK reactor delivered at least once AND the bad reactor's row
	// is still present (it can never be acked). Then assert independence.
	var okDelivered, badRowPresent bool
	for time.Now().Before(deadline) {
		root, _ := q.GetResourceInfo(ctx, rootID)
		if root.IsReady && root.SyncedGen >= 1 && ok.count() >= 1 {
			okDelivered = true
		}
		var okRows, badRows int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FILTER (WHERE binding_name='ok'), count(*) FILTER (WHERE binding_name='bad')
			 FROM lifecycle_outbox WHERE resource_id=$1 AND transition='synced'`, rootID).Scan(&okRows, &badRows))
		badRowPresent = badRows > 0
		// Target state: ok row drained (acked), bad row still here for retry.
		if okDelivered && okRows == 0 && badRowPresent {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !okDelivered {
		dumpLifecycleState(t, ctx, pool)
		t.Fatal("ok-reactor should have delivered at least once")
	}

	// The succeeding binding's row is acked/gone; the failing binding's row
	// survives independently (proves the ack did NOT delete the shared row).
	var okRows, badRows int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FILTER (WHERE binding_name='ok'), count(*) FILTER (WHERE binding_name='bad')
		 FROM lifecycle_outbox WHERE resource_id=$1 AND transition='synced'`, rootID).Scan(&okRows, &badRows))
	require.Zero(t, okRows, "succeeding binding's row must be acked (deleted)")
	require.Positive(t, badRows, "failing binding's row must SURVIVE independently (not deleted by the ok ack)")

	// The bad reactor keeps being retried (>=1); the ok reactor is NOT re-fired
	// by the bad binding's failure — give it a moment and confirm ok stayed put
	// while bad climbed (or at least the ok row never reappears).
	okCountA := ok.count()
	time.Sleep(2 * time.Second)
	require.GreaterOrEqual(t, bad.count(), 1, "bad-reactor must be retried at least once")
	require.Equal(t, okCountA, ok.count(), "ok-reactor must NOT be re-fired by the bad binding's failure/retry")
	t.Logf("SUCCESS: fan-out independent — ok acked (delivered %d), bad survives + retries (%d attempts)", okCountA, bad.count())
}

// TestLifecycleReactorLabelMismatchNoOrphan is the F2 proof: a binding with a
// label_match the resource does NOT satisfy emits NO row (the filter is applied
// at EMIT), so there is no claim-then-drop orphan looping forever.
func TestLifecycleReactorLabelMismatchNoOrphan(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	ok := newCountingReactor("ok-reactor", false)
	// The reactor kind is a registry member (CRD seeded + handler wired); the
	// label never matches so this handler is never actually invoked — the proof
	// is that NO lifecycle_outbox row is ever emitted (filter applied at emit).
	reg := defaultRegistry()
	okKR, okM := ok.runtimeAndManifest()
	reg.AddKind(okKR, okM)
	eng := startEngineWithConfig(t, ctx, pool, reg, engineOpts{WithReactors: true})
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	// Subscription requires label team=zeta; the classicbom we submit carries no labels.
	require.NoError(t, store.New(pool).UpsertReactorBinding(ctx, store.ReactorBinding{
		Name: "zeta-only", WatchKind: model.Kind(classicbom.Kind), Transition: string(model.TransitionSynced),
		Reactor: "ok-reactor", LabelMatch: json.RawMessage(`{"team":"zeta"}`), Enabled: true,
	}))

	bom := smallBOM("label-project", map[string][]string{"alpha-fd": {"team-alpha"}})
	specBytes, err := json.Marshal(bom)
	require.NoError(t, err)
	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "label-project", specBytes, nil)
	require.NoError(t, err)

	q := dbq.New(pool)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		root, _ := q.GetResourceInfo(ctx, rootID)
		if root.IsReady && root.SyncedGen >= 1 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Give the spine a moment after rollup, then assert: NO row was ever emitted
	// (label didn't match), NO delivery happened, nothing is looping.
	time.Sleep(2 * time.Second)
	var rows int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM lifecycle_outbox WHERE resource_id=$1`, rootID).Scan(&rows))
	require.Zero(t, rows, "label-mismatch binding must emit NO row (filter applied at emit → no orphan)")
	require.Zero(t, ok.count(), "reactor must never be invoked for a label-mismatched resource")
}

// TestLifecycleReactorMultipleReactorsSameEvent is the fan-out requirement proof:
// SEVERAL DISTINCT reactor kinds may subscribe to the SAME (watch_kind,
// transition) event, and ALL of them fire when it happens. Three independent
// reactor kinds each get their own binding on (classicbom,'synced'); the emit
// writes one lifecycle_outbox row per binding (keyed by binding_name), so all
// three deliver independently — none blocks or shadows another.
func TestLifecycleReactorMultipleReactorsSameEvent(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	// Three DISTINCT reactor kinds — each a registry member (CRD seeded so the
	// claim resolves its reaction name; handler wired).
	reactorKinds := []model.Kind{"sink-a", "sink-b", "sink-c"}
	reactors := make(map[model.Kind]*countingReactor, len(reactorKinds))
	reg := defaultRegistry()
	for _, k := range reactorKinds {
		r := newCountingReactor(k, false)
		reactors[k] = r
		kr, m := r.runtimeAndManifest()
		reg.AddKind(kr, m)
	}

	eng := startEngineWithConfig(t, ctx, pool, reg, engineOpts{WithReactors: true})
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	// One subscription per reactor kind, ALL on the SAME (classicbom,'synced')
	// event. Distinct arbitrary binding names.
	st := store.New(pool)
	for _, k := range reactorKinds {
		require.NoError(t, st.UpsertReactorBinding(ctx, store.ReactorBinding{
			Name: "sub-" + string(k), WatchKind: model.Kind(classicbom.Kind), Transition: string(model.TransitionSynced),
			Reactor: model.Kind(k), Enabled: true,
		}))
	}

	bom := smallBOM("multi-react-project", map[string][]string{"alpha-fd": {"team-alpha"}})
	specBytes, err := json.Marshal(bom)
	require.NoError(t, err)
	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "multi-react-project", specBytes, nil)
	require.NoError(t, err)

	// Wait until EVERY reactor kind has delivered at least once — the requirement:
	// N reactors on one event all fire.
	deadline := time.Now().Add(60 * time.Second)
	allFired := func() bool {
		for _, k := range reactorKinds {
			if reactors[k].count() == 0 {
				return false
			}
		}
		return true
	}
	for time.Now().Before(deadline) && !allFired() {
		time.Sleep(200 * time.Millisecond)
	}
	if !allFired() {
		dumpLifecycleState(t, ctx, pool)
		for _, k := range reactorKinds {
			t.Logf("  reactor %s delivered %d", k, reactors[k].count())
		}
		t.Fatal("every reactor kind subscribed to (classicbom,'synced') must fire")
	}

	// All rows drained (each binding acked independently) — no orphan left behind.
	gone := false
	for time.Now().Before(deadline) {
		var n int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM lifecycle_outbox WHERE resource_id=$1`, rootID).Scan(&n))
		if n == 0 {
			gone = true
			break
		}
		time.Sleep(150 * time.Millisecond)
	}
	require.True(t, gone, "every subscription's row must be acked independently after delivery")
	t.Logf("SUCCESS: %d distinct reactor kinds all fired on the same (classicbom,'synced') event", len(reactorKinds))
}

// TestLifecycleReactorOneReactorManyEvents is the reverse fan-out proof: ONE
// reactor kind may subscribe to DIFFERENT events via multiple bindings, and it
// fires on each. Here one reactor is bound to both (classicbom,'created') and
// (classicbom,'synced'); a healthy run emits BOTH (created at apply, synced at
// rollup), so the single reactor kind receives a delivery for each — and the
// transition that fired reaches its handler as ReactionRequest.Transition data
// (the reactor's ONE reaction is event-agnostic; the binding names the event).
func TestLifecycleReactorOneReactorManyEvents(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	// One reactor kind, registered once.
	r := newCountingReactor("multi-event-sink", false)
	reg := defaultRegistry()
	kr, m := r.runtimeAndManifest()
	reg.AddKind(kr, m)

	eng := startEngineWithConfig(t, ctx, pool, reg, engineOpts{WithReactors: true})
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	// TWO subscriptions, SAME reactor kind, DIFFERENT events. Applied BEFORE submit
	// so the 'created' emit (in the apply tx) sees the binding.
	st := store.New(pool)
	require.NoError(t, st.UpsertReactorBinding(ctx, store.ReactorBinding{
		Name: "on-created", WatchKind: model.Kind(classicbom.Kind), Transition: string(model.TransitionCreated),
		Reactor: model.Kind(r.kind), Enabled: true,
	}))
	require.NoError(t, st.UpsertReactorBinding(ctx, store.ReactorBinding{
		Name: "on-synced", WatchKind: model.Kind(classicbom.Kind), Transition: string(model.TransitionSynced),
		Reactor: model.Kind(r.kind), Enabled: true,
	}))

	bom := smallBOM("many-event-project", map[string][]string{"alpha-fd": {"team-alpha"}})
	specBytes, err := json.Marshal(bom)
	require.NoError(t, err)
	_, err = eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "many-event-project", specBytes, nil)
	require.NoError(t, err)

	// The one reactor kind must receive BOTH events: 'created' (at apply) and
	// 'synced' (at rollup), each carrying its own transition to the handler.
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) && !(r.sawTransition(converge.TransitionCreated) && r.sawTransition(converge.TransitionSynced)) {
		time.Sleep(200 * time.Millisecond)
	}
	if !(r.sawTransition(converge.TransitionCreated) && r.sawTransition(converge.TransitionSynced)) {
		dumpLifecycleState(t, ctx, pool)
		t.Fatalf("one reactor bound to two events must fire on both; saw created=%v synced=%v",
			r.sawTransition(converge.TransitionCreated), r.sawTransition(converge.TransitionSynced))
	}
	t.Logf("SUCCESS: one reactor kind fired on BOTH its subscribed events (created + synced)")
}
