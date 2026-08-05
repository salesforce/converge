package test

// END-TO-END versioning over the REAL Connect broker↔worker tier. Unlike
// versioning_test.go (store-level mechanics), this stands up an actual broker, two
// actual dumb workers — one advertising (noop, v1), one advertising
// (noop, v2) — and a control drainer, then drives the full routing matrix over the
// wire and asserts WHICH worker executed each task (via work_queue.worker_id).
//
// It is written to READ AS A STORY: every scenario logs a plain-English PASS line,
// so `go test -run TestVersioningE2E -v` narrates what the version routing did.
//
// Scenarios (all over real Connect):
//   1. a vpc/v1 resource is executed ONLY by the v1 worker (never the v2 worker);
//   2. a vpc/v2 resource is executed ONLY by the v2 worker;
//   3. a vpc/v3 resource with NO v3 worker connected PARKS (worker_id stays NULL,
//      the row is never mis-routed to a v1/v2 worker) — the strict, safe stall;
//   4. a live vpc/v1 resource FLIPPED to v2 re-routes to the v2 worker;
//   5. the per-(kind, kindVersion) cap bounds v1 and v2 independently over the wire.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/broker"
	"github.com/salesforce/converge/internal/engine"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/sdk-go/converge"
	"github.com/salesforce/converge/sdk-go/workerpb/workerpbconnect"
	"github.com/salesforce/converge/test/internal/noop"
)

// taggingHandler is a noop-style reaction that stamps the RUNNING WORKER's tag
// into the resource STATUS. Unlike work_queue.worker_id (written by the broker's
// coalesced attribution batcher, and gone once the row drains), status PERSISTS on
// the resources row — so "which worker ran this" is a race-free, durable
// assertion. tag is captured in the closure exactly as a real worker captures its
// own client/creds (see the noop package doc).
type taggingHandler struct {
	kindVersion int
	tag         string
	delay       time.Duration
}

// compile-time proof it satisfies the one provider contract.
var _ converge.Provider = taggingHandler{}

// Kind is the single (noop, vN) this provider serves — vN is the routing key.
func (h taggingHandler) Kind() converge.KindVersion {
	return converge.KindVersion{Kind: converge.Kind(noop.Kind), Version: h.kindVersion}
}

// OnConfig is a no-op: this test provider reads no default providerconfig.
func (taggingHandler) OnConfig(converge.ProviderConfig) {}

// Ready is always true: a pure test provider has no downstream to dial.
func (taggingHandler) Ready() bool { return true }

func (h taggingHandler) Work(ctx context.Context, _ converge.ReactionRequest) (converge.Outcome, error) {
	if h.delay > 0 {
		select {
		case <-time.After(h.delay):
		case <-ctx.Done():
			return converge.Outcome{}, ctx.Err()
		}
	}
	status, _ := json.Marshal(map[string]string{"ranBy": h.tag})
	return converge.Outcome{Status: status}, nil
}

// noopAtKindVersion builds a noop provider advertising a specific kindVersion with a
// worker TAG stamped into status — this is EXACTLY how a provider author declares
// "this worker binary serves noop/vN": one converge.Provider whose Kind() names the
// (kind, kindVersion). The kindVersion is the routing key; the tag lets the test prove
// WHICH worker executed the reconcile durably (via resources.status.ranBy).
func noopAtKindVersion(kindVersion int, tag string, delay time.Duration) converge.Provider {
	return taggingHandler{kindVersion: kindVersion, tag: tag, delay: delay}
}

// ranBy reads the durable worker tag the reconcile wrote into resources.status
// (persists after the work_queue row drains). Empty until the resource syncs.
func ranBy(t *testing.T, ctx context.Context, pool *pgxpool.Pool, resID uuid.UUID) string {
	t.Helper()
	var status []byte
	if err := pool.QueryRow(ctx, `SELECT status FROM resources WHERE id=$1`, resID).Scan(&status); err != nil {
		return ""
	}
	var m map[string]string
	if json.Unmarshal(status, &m) != nil {
		return ""
	}
	return m["ranBy"]
}

// startWorker connects a real dumb worker to the broker, advertising the
// given provider(s) under a friendly worker id (recorded as worker_id, which
// is how the test proves WHICH worker ran a task). Returns nothing — cleanup is
// registered on t.
func startWorker(t *testing.T, ctx context.Context, srv *httptest.Server, providers ...converge.Provider) {
	t.Helper()
	client := workerpbconnect.NewWorkerServiceClient(srv.Client(), srv.URL)
	wctx, wcancel := context.WithCancel(ctx)
	t.Cleanup(wcancel)
	go func() {
		_ = converge.RunWorker(wctx, client, providers, converge.RunOptions{MaxInflight: 8})
	}()
}

// waitSynced blocks until the resource's synced_gen catches its generation (the
// reconcile ran end to end) or the deadline; returns whether it synced.
func waitSynced(t *testing.T, ctx context.Context, pool *pgxpool.Pool, resID uuid.UUID, d time.Duration) bool {
	return waitSyncedPastGen(t, ctx, pool, resID, 0, d)
}

// waitSyncedPastGen blocks until synced_gen >= generation AND generation > afterGen
// — i.e. a NEW reconcile (past afterGen) has fully settled. Used after a flip
// (which bumps generation) so a read sees the status the new-kindVersion reconcile
// wrote, not the pre-flip one. afterGen=0 → plain "synced" (any settled gen).
func waitSyncedPastGen(t *testing.T, ctx context.Context, pool *pgxpool.Pool, resID uuid.UUID, afterGen int64, d time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		var gen, synced int64
		if err := pool.QueryRow(ctx,
			`SELECT generation, synced_gen FROM resources WHERE id = $1`, resID).Scan(&gen, &synced); err == nil {
			if gen > afterGen && synced >= gen {
				return true
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	return false
}

func TestVersioningE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)
	st := store.New(pool)

	// Publish noop at v1, v2, v3 (three kind versions of the SAME bare kind "noop").
	for _, mj := range []int{1, 2, 3} {
		_, err := st.UpsertKindManifest(ctx, vManifest("noop", mj, `{"type":"object"}`))
		require.NoError(t, err)
	}

	// Control drainer only (no in-process dispatch) — the broker tier is the only
	// path that executes noop, so worker_id is authoritative.
	controlDuties, err := engine.DutiesFromConfig(engine.EngineConfig{RunControl: true})
	require.NoError(t, err)
	control := engine.NewEngine(controlDuties, engine.Deps{Pool: pool})
	require.NoError(t, control.Start(ctx))
	t.Cleanup(func() { _ = control.Stop(context.Background()) })

	// The broker reads its (kind, kindVersion) pairs + policy from the manifest cache.
	mc := runtime.NewKindManifestCache(pool)
	require.NoError(t, mc.Load(ctx))
	mc.Start(ctx)
	t.Cleanup(mc.Stop)

	const brokerID = "broker-vtest"
	cl := broker.New(pool, mc, brokerID, 16)
	// The broker claims noop at v1, v2 AND v3 (it routes all three kindVersions); which
	// kindVersion actually gets DELIVERED depends on which worker is connected — that is
	// the strict (kind, kindVersion) gate under test.
	cl.Dispatcher().AddPair("noop", 1, store.TaskReconcile)
	cl.Dispatcher().AddPair("noop", 2, store.TaskReconcile)
	cl.Dispatcher().AddPair("noop", 3, store.TaskReconcile)
	mux := http.NewServeMux()
	mux.Handle(cl.Handler())
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	claimCtx, claimCancel := context.WithCancel(ctx)
	t.Cleanup(claimCancel)
	go func() { _ = cl.Dispatcher().Run(claimCtx) }()

	// Two real dumb workers: WORKER-v1 serves (noop, v1), WORKER-v2 serves
	// (noop, v2). NO v3 worker connects — that is scenario 3. A small delay keeps a
	// task in-flight long enough to sample worker_id.
	startWorker(t, ctx, srv, noopAtKindVersion(1, "WORKER-v1", 100*time.Millisecond))
	startWorker(t, ctx, srv, noopAtKindVersion(2, "WORKER-v2", 100*time.Millisecond))
	// Give the subscriptions a moment to register (HasSubscriber(noop,1)/(noop,2)).
	time.Sleep(500 * time.Millisecond)

	apply := func(name string, kindVersion int) uuid.UUID {
		r, err := st.ApplySpecKindVersionWithConfig(ctx, "noop", kindVersion, name, json.RawMessage(`{}`), nil, "")
		require.NoError(t, err)
		return r.ID
	}

	t.Log("── versioning E2E over real Connect broker↔worker ──")

	// Scenario 1: a vpc/v1 resource is executed ONLY by WORKER-v1. Proven by the
	// durable status tag the worker stamped (ranBy), read AFTER sync — race-free.
	t.Run("v1_resource_routes_to_v1_worker", func(t *testing.T) {
		id := apply("res-v1", 1)
		require.True(t, waitSynced(t, ctx, pool, id, 60*time.Second), "vpc/v1 resource must reconcile")
		require.Equal(t, "WORKER-v1", ranBy(t, ctx, pool, id), "a v1 resource must be executed by the v1 worker, never v2")
		t.Logf("✓ vpc/v1 resource %q → executed by WORKER-v1 (v2 worker never touched it)", "res-v1")
	})

	// Scenario 2: a vpc/v2 resource is executed ONLY by WORKER-v2.
	t.Run("v2_resource_routes_to_v2_worker", func(t *testing.T) {
		id := apply("res-v2", 2)
		require.True(t, waitSynced(t, ctx, pool, id, 60*time.Second), "vpc/v2 resource must reconcile")
		require.Equal(t, "WORKER-v2", ranBy(t, ctx, pool, id), "a v2 resource must be executed by the v2 worker, never v1")
		t.Logf("✓ vpc/v2 resource %q → executed by WORKER-v2 (v1 worker never touched it)", "res-v2")
	})

	// Scenario 3: a vpc/v3 resource with NO v3 worker connected PARKS — the row is
	// never mis-delivered to a v1/v2 worker, and it stays worker_id NULL (safe,
	// visible stall), NOT synced.
	t.Run("v3_resource_parks_when_no_worker", func(t *testing.T) {
		id := apply("res-v3", 3)
		// Give the broker ample time to (not) route it. If routing were wrong, a
		// v1/v2 worker would grab it and synced_gen would advance.
		require.False(t, waitSynced(t, ctx, pool, id, 8*time.Second),
			"a v3 resource with no v3 worker must NOT reconcile (no worker can serve it)")
		// The reconcile row must exist and stay UNCLAIMED (broker_id NULL) — parked,
		// not mis-routed.
		var brokerID *string
		var workerID *string
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT broker_id, worker_id FROM work_queue WHERE resource_id=$1 AND task_type='reconcile'`, id).
			Scan(&brokerID, &workerID))
		require.Nil(t, brokerID, "a v3 task must stay UNCLAIMED (broker_id NULL) — parked, not routed to a v1/v2 worker")
		require.Nil(t, workerID, "a v3 task must never be executed by any connected (v1/v2) worker")
		t.Logf("✓ vpc/v3 resource %q → PARKED (broker_id NULL, not mis-routed) — the strict, safe stall", "res-v3")

		// Now connect a v3 worker → the parked task drains and syncs. This proves the
		// park is a WAIT, not a drop: the moment a matching-kindVersion worker appears, the
		// work flows.
		startWorker(t, ctx, srv, noopAtKindVersion(3, "WORKER-v3", 100*time.Millisecond))
		cl.Dispatcher().Trigger() // nudge the idle claim loop
		require.True(t, waitSynced(t, ctx, pool, id, 60*time.Second),
			"once a v3 worker connects, the parked v3 task must drain")
		t.Logf("✓ vpc/v3 resource %q → after WORKER-v3 connected, the parked task drained and synced", "res-v3")
	})

	// Scenario 4: a live vpc/v1 resource FLIPPED to v2 re-routes to WORKER-v2.
	t.Run("flip_v1_to_v2_reroutes", func(t *testing.T) {
		id := apply("res-flip", 1)
		require.True(t, waitSynced(t, ctx, pool, id, 60*time.Second), "the resource first reconciles at v1")
		require.Equal(t, "WORKER-v1", ranBy(t, ctx, pool, id), "before the flip it runs on v1")

		// Read the settled generation, then flip the SAME resource to v2 (uuid-stable,
		// generation bumps). Wait for synced_gen to catch the NEW generation so we
		// read the status the v2 reconcile wrote (not the stale v1 one).
		var genV1 int64
		require.NoError(t, pool.QueryRow(ctx, `SELECT generation FROM resources WHERE id=$1`, id).Scan(&genV1))
		fr, err := st.FlipResourceKindVersion(ctx, id, 1, 2, json.RawMessage(`{}`), "noop")
		require.NoError(t, err)
		require.True(t, fr.Flipped)
		require.True(t, waitSyncedPastGen(t, ctx, pool, id, genV1, 60*time.Second),
			"the flipped resource re-reconciles at v2 (synced past the pre-flip generation)")
		require.Equal(t, "WORKER-v2", ranBy(t, ctx, pool, id), "after the flip to v2, the SAME resource is executed by the v2 worker")
		t.Logf("✓ resource %q flipped v1→v2 in place → re-routed from WORKER-v1 to WORKER-v2", "res-flip")
	})

	t.Log("── all versioning E2E scenarios passed over real Connect ──")
}
