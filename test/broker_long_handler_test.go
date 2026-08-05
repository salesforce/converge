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

// TestBrokerLongTaskKeepsLease guards the WORKER-LONG-HANDLER liveness path: a worker whose
// Work() runs LONGER than both the worker-attestation window (RequireWithin) and the reaper's
// StaleAfter keeps its lease fresh and completes on the FIRST attempt. Two independent
// mechanisms keep it fresh (either suffices for a worker-long handler): the SDK attests every
// still-running (!done) task, AND the broker self-attests a local task (inFlightRecord.local).
//
// NOTE the complementary case this does NOT cover: a task whose worker returns FAST but whose
// broker-side apply is the long pole (the 1M classicbom compose — the worker's React returns in
// µs, then ApplyComposeResult runs ~1 min on the broker). There the SDK stops attesting (task
// is done) and ONLY the `local` self-attest keeps the lease alive. That path is guarded by the
// 1M compose E2E (a noop handler has no broker-apply phase, so it can't reproduce it here).
//
// It runs the REAL broker→worker Connect path with the windows shrunk so the test is fast and
// DISCRIMINATING: RequireWithin=2s, StaleAfter=5s, and a 12s handler (6× RequireWithin, 2.4×
// StaleAfter). Pre-fix, the worker's attestation frame dropped a handler that didn't call
// Env.Heartbeat within ~12s AND a local task's claim-time stamp aged past RequireWithin, so
// the lease went stale, the reaper reclaimed it mid-work, the late result was epoch-fenced
// out, and it looped forever — this test would TIME OUT. Post-fix it syncs with attempts=1.
func TestBrokerLongTaskKeepsLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	// A noop worker whose Work() sleeps 12s in ONE call — well past RequireWithin (2s) and
	// StaleAfter (5s), with no Env.Heartbeat progress calls (the common straight-line shape).
	const workDelay = 12 * time.Second
	noopProvider := noop.New(workDelay)
	reg := newTReg()
	reg.Add(noopProvider)

	// Control bundle with a SHORT reaper StaleAfter (5s) so a genuinely stranded lease WOULD
	// be reclaimed quickly — the test fails fast (loops) if the fix regresses.
	migrateForSeed(t, ctx, pool)
	require.NoError(t, reg.seed(ctx, pool))
	controlDuties, err := engine.DutiesFromConfig(engine.EngineConfig{
		RunControl:        true,
		SweeperStaleAfter: 5 * time.Second,
	})
	require.NoError(t, err)
	control := engine.NewEngine(controlDuties, engine.Deps{Pool: pool})
	require.NoError(t, control.Start(ctx))
	t.Cleanup(func() { _ = control.Stop(context.Background()) })

	mc := runtime.NewKindManifestCache(pool)
	require.NoError(t, mc.Load(ctx))
	mc.Start(ctx)
	t.Cleanup(mc.Stop)

	// Broker: shrink RequireWithin to 2s and heartbeat every 500ms, so the worker-attestation
	// window is well inside the 12s handler and the fix (keep refreshing a still-running local
	// task) is exercised, not just the claim-time stamp.
	const brokerID = "broker-long-task"
	cl := broker.New(pool, mc, brokerID, 16)
	cl.Dispatcher().RequireWithin = 2 * time.Second
	cl.Dispatcher().HeartbeatEvery = 500 * time.Millisecond
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

	client := workerpbconnect.NewWorkerServiceClient(srv.Client(), srv.URL)
	workerCtx, workerCancel := context.WithCancel(ctx)
	t.Cleanup(workerCancel)
	go func() {
		_ = converge.RunWorker(workerCtx, client, []converge.Provider{noopProvider}, converge.RunOptions{MaxInflight: 8})
	}()

	rootID, err := (&testEngine{Pool: pool}).CreateRoot(ctx, "noop", "long-root", json.RawMessage(`{}`), nil)
	require.NoError(t, err)

	// Wait past the 12s handler + drain, then require synced AND attempts==1 — a reaped-and-
	// retried task would show attempts>1 (or never sync at all).
	deadline := time.Now().Add(50 * time.Second)
	var gen, syncedGen int64
	var attempts int32
	for time.Now().Before(deadline) {
		err := pool.QueryRow(ctx,
			`SELECT r.generation, r.synced_gen,
			        COALESCE((SELECT wq.attempts FROM work_queue wq
			                   WHERE wq.resource_id = r.id AND wq.task_type = 'reconcile'), 1)
			   FROM resources r WHERE r.id = $1`, rootID,
		).Scan(&gen, &syncedGen, &attempts)
		require.NoError(t, err)
		if syncedGen >= gen && gen > 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if syncedGen < gen {
		dumpResourceState(t, ctx, pool, rootID)
		t.Fatalf("long task never synced: generation=%d synced_gen=%d — the lease was likely reaped mid-work and its result epoch-fenced out (the work-plane liveness regression)", gen, syncedGen)
	}
	t.Logf("long (%s) task synced on the real broker path: generation=%d synced_gen=%d attempts=%d", workDelay, gen, syncedGen, attempts)
}
