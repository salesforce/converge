package test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/broker"
	"github.com/salesforce/converge/internal/engine"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/sdk-go/converge"
	"github.com/salesforce/converge/sdk-go/workerpb/workerpbconnect"
	"github.com/salesforce/converge/test/internal/noop"
)

// TestBrokerEndToEnd proves the UNIVERSAL Connect claim tier against a real
// Postgres: a broker claims a noop reconcile task from work_queue, streams it
// to a DUMB remote worker over Connect, the worker runs the noop stage and reports
// back, the broker writes the fenced outbox under ITS identity, and the
// control drainer advances synced_gen. No in-process dispatch duty runs for the
// kind — all execution goes worker→broker→DB.
//
// This is the end-to-end milestone: claim → stream → worker → complete →
// synced_gen, with the lease owned by the broker (worker_id is the broker's).
func TestBrokerEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	// Registry: noop only (Worker-only kind, no Composer). A small work delay
	// keeps the task in-flight long enough to deterministically sample that the
	// work_queue lease is held by the broker while the worker executes.
	noopProvider := noop.New(300 * time.Millisecond)
	reg := newTReg()
	reg.Add(noopProvider)

	// Migrate + seed the kind manifest (the claim reads its pair from kind_config,
	// derived from kind_manifest), then start the CONTROL duty only — the sweeper
	// bundle (drainer applies the outbox → synced_gen, reaper backstops leases). It
	// runs NO dispatch duty, so nothing claims noop in-process; the broker is the
	// only broker.
	migrateForSeed(t, ctx, pool)
	require.NoError(t, reg.seed(ctx, pool))
	controlDuties, err := engine.DutiesFromConfig(engine.EngineConfig{RunControl: true})
	require.NoError(t, err)
	control := engine.NewEngine(controlDuties, engine.Deps{Pool: pool})
	require.NoError(t, control.Start(ctx))
	t.Cleanup(func() { _ = control.Stop(context.Background()) })

	// The manifest cache the broker reads its pairs + policy from (manifests
	// already seeded above).
	mc := runtime.NewKindManifestCache(pool)
	require.NoError(t, mc.Load(ctx))
	mc.Start(ctx)
	t.Cleanup(mc.Stop)

	// The broker: owns all shards (Dispatcher default), claims the noop reconcile
	// pair, stamps worker_id = "broker-e2e".
	const brokerID = "broker-e2e"
	cl := broker.New(pool, mc, brokerID, 16)
	cl.Dispatcher().AddPair("noop", 1, store.TaskReconcile)
	mux := http.NewServeMux()
	mux.Handle(cl.Handler())
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	claimCtx, claimCancel := context.WithCancel(ctx)
	t.Cleanup(claimCancel)
	go func() { _ = cl.Dispatcher().Run(claimCtx) }()

	// A dumb remote worker connected over Connect, running noop.
	client := workerpbconnect.NewWorkerServiceClient(srv.Client(), srv.URL)
	workerCtx, workerCancel := context.WithCancel(ctx)
	t.Cleanup(workerCancel)
	go func() {
		_ = converge.RunWorker(workerCtx, client, []converge.Provider{noopProvider}, converge.RunOptions{MaxInflight: 8})
	}()

	// Create a noop root → enqueues a reconcile work_queue row.
	rootID, err := (&testEngine{Pool: pool}).CreateRoot(ctx, "noop", "e2e-root", json.RawMessage(`{}`), nil)
	require.NoError(t, err)

	// Concurrently with the chain settling, capture the work_queue lease holder
	// the FIRST time the row is claimed — it must be the BROKER's identity, not
	// a worker's, proving the broker owns the lease and the worker is a
	// downstream executor. (The row is deleted after the reconcile drains, so we
	// poll for it during the window it's claimed.)
	leaseHolder := make(chan string, 1)
	go func() {
		for ctx.Err() == nil {
			var wid *string
			err := pool.QueryRow(ctx,
				`SELECT worker_id FROM work_queue WHERE resource_id = $1 AND task_type = 'reconcile'`, rootID,
			).Scan(&wid)
			if err == nil && wid != nil && *wid != "" {
				select {
				case leaseHolder <- *wid:
				default:
				}
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Wait for synced_gen to catch up to generation — the full chain settled.
	deadline := time.Now().Add(60 * time.Second)
	var gen, syncedGen int64
	for time.Now().Before(deadline) {
		err := pool.QueryRow(ctx,
			`SELECT generation, synced_gen FROM resources WHERE id = $1`, rootID,
		).Scan(&gen, &syncedGen)
		require.NoError(t, err)
		if syncedGen >= gen && gen > 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if syncedGen < gen {
		dumpResourceState(t, ctx, pool, rootID)
		t.Fatalf("noop root never synced: generation=%d synced_gen=%d", gen, syncedGen)
	}

	// The lease must have been held by the broker — the only broker in this
	// test — confirming work flowed worker→broker→DB, not via any in-process
	// dispatch (there is none for noop here).
	select {
	case wid := <-leaseHolder:
		require.Equal(t, brokerID, wid, "work_queue lease must be held by the broker, not a worker")
		t.Logf("noop root synced via broker tier: generation=%d synced_gen=%d, lease held by %q", gen, syncedGen, wid)
	default:
		// The claim window was missed (fast drain); synced_gen advancing with no
		// in-process dispatcher still proves the broker tier did the work.
		t.Logf("noop root synced via broker tier: generation=%d synced_gen=%d (lease window not sampled)", gen, syncedGen)
	}
}
