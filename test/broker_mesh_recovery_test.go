package test

// broker_mesh_recovery_test.go — DETERMINISTIC coverage for the two broker-mesh
// fault edges the chaos-coverage audit flagged: a task PUSHED to a peer when that
// peer (or the worker running it) is lost mid-flight. Mesh recovery is UNIFIED on
// the reaper: a broker holds the work_queue lease for a row it claimed and pushed,
// heartbeating it while the remote executor runs; if that executor departs or its
// worker crashes, the owner's lease heartbeat lapses (or the bounded remote-fanout
// ceiling fires), the reaper reclaims the stale row and BUMPS its claim_epoch, and
// the row is re-dispatched to a healthy executor. The claim_epoch fence makes the
// interrupted attempt's late result inert, so recovery strands nothing and never
// double-applies.
//
//   peer departure  — a task pushed to a broker that then DEPARTS the cluster must be
//                      re-claimed and re-run once a healthy worker is available; the
//                      parked lease must not strand.
//   worker crash    — a worker executing a peer's pushed (foreign) task crashes while
//                      its broker stays up; the row must be re-claimed and re-run once
//                      a worker reconnects.
//
// The chaos soak (broker_chaos_test.go) injects these faults too, but its timing is
// probabilistic — it can't deterministically catch the narrow window where a task is
// parked-pushed at the instant of the fault. These tests use the proven
// workers<brokers relay harness (see TestBrokerRelayFewerWorkersThanBrokers) with a
// short reaper StaleAfter + remote-fanout ceiling so recovery is observable within the
// test window, and assert the fleet converges (every root synced, no work_queue row
// stranded, no double-apply) after the fault. A regression that stranded the pushed
// row would fail here, unlike the soak.

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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

// meshRecoveryBroker is one broker in the two-broker recovery harness.
type meshRecoveryBroker struct {
	id     string
	srv    *httptest.Server
	cl     *broker.Server
	lohi   [2]int16
	beat   func()
	cancel context.CancelFunc // stops THIS broker's dispatcher/relay (models departure/crash)
}

// meshRecoveryFleet is a minimal, DETERMINISTIC workers<brokers mesh: broker-0 owns
// the FIRST half of the shard space and has the only worker; broker-1 owns the
// SECOND half and is worker-LESS, so a root in broker-1's tile is claimed by broker-1
// and PUSHED over the mesh to broker-0. A short RemoteFanoutCeiling + reaper
// StaleAfter make recovery from a mid-push fault observable within the test window.
type meshRecoveryFleet struct {
	t          *testing.T
	pool       *pgxpool.Pool
	st         *store.Store
	owner      *meshRecoveryBroker   // broker-1: worker-less, claims tile-1, PUSHES (parks the lease)
	executor   *meshRecoveryBroker   // broker-0: has the worker, RUNS the pushed task
	workerStop context.CancelFunc    // cancels the current worker goroutine
	workerGate *faultGate            // the worker's transport gate — cut it to model a hard crash (no clean Complete)
	newWorker  func()                // (re)start a worker on the executor
	newWorkerA func(dialAddr string) // (re)start a worker dialing an arbitrary broker
}

// crashWorker models a HARD worker crash (OOM / node loss): CUT its transport first
// so no clean Complete can escape mid-task, THEN cancel it. Its executor broker
// stays up, so the pushed row's lease keeps being held by the owner until the reaper
// reclaims it. A graceful cancel alone would let the in-flight handler Complete
// normally within DrainGrace — exactly the wrong fault for this edge.
func (f *meshRecoveryFleet) crashWorker() {
	if f.workerGate != nil {
		f.workerGate.set(faultCut)
	}
	if f.workerStop != nil {
		f.workerStop()
	}
}

// meshRecoveryCeiling is the in-test RemoteFanoutCeiling: short enough that a parked
// push whose executor vanished is bounded (cancelled + re-armed) within the test
// window rather than pinning the slot for the production default.
const meshRecoveryCeiling = 8 * time.Second

// meshRecoveryStaleAfter is the in-test reaper lease-expiry window: short so a stale
// lease (whose owner departed, or whose remote execution was interrupted) is reclaimed
// and its claim_epoch bumped within the test window.
const meshRecoveryStaleAfter = 3 * time.Second

// startMeshRecoveryFleet stands up the two-broker workers<brokers mesh and returns a
// handle. The handler delay keeps a pushed task in flight long enough to observe the
// claim + inject the fault. Everything is cleaned up via t.Cleanup.
func startMeshRecoveryFleet(t *testing.T, ctx context.Context, pool *pgxpool.Pool, handlerDelay time.Duration) *meshRecoveryFleet {
	t.Helper()

	noopProvider := noop.New(handlerDelay)
	reg := newTReg()
	reg.Add(noopProvider)
	migrateForSeed(t, ctx, pool)
	require.NoError(t, reg.seed(ctx, pool))

	// Control duties with a SHORT reaper cadence so a stale lease is reclaimed +
	// re-armed within the test window (the production defaults are minutes/tens of
	// seconds). The reaper is the sole recovery driver: it bumps claim_epoch on
	// reclaim, fencing the interrupted attempt's late result.
	controlDuties, err := engine.DutiesFromConfig(engine.EngineConfig{
		RunControl:        true,
		SweeperStaleAfter: meshRecoveryStaleAfter,
		SweeperInterval:   1 * time.Second,
		RetryAfter:        500 * time.Millisecond,
	})
	require.NoError(t, err)
	control := engine.NewEngine(controlDuties, engine.Deps{Pool: pool})
	require.NoError(t, control.Start(ctx))
	t.Cleanup(func() { _ = control.Stop(context.Background()) })

	mc := runtime.NewKindManifestCache(pool)
	require.NoError(t, mc.Load(ctx))
	mc.Start(ctx)
	t.Cleanup(mc.Stop)

	st := store.New(pool)

	// broker-0 = executor (has the worker); broker-1 = owner (worker-less → pushes).
	tiles := [][2]int16{{0, 127}, {128, 255}}
	mk := func(i int) *meshRecoveryBroker {
		id := fmt.Sprintf("mrb-%d", i)
		ss := runtime.NewShardSet(shardRangeSlice(tiles[i][0], tiles[i][1]))
		cl := broker.NewDispatch(pool, mc, id, 64, ss)
		cl.SetRemoteFanoutCeiling(meshRecoveryCeiling)
		cl.EnableRelay(id, h2TestClient(), st, runtime.NewPgxListener(pool), 200*time.Millisecond, time.Hour)
		mux := http.NewServeMux()
		mux.Handle(cl.Handler())
		srv := h2Server(t, mux)
		b := &meshRecoveryBroker{id: id, srv: srv, cl: cl, lohi: tiles[i]}
		b.beat = func() {
			require.NoError(t, st.UpsertClusterMember(ctx, store.ClusterMemberInfo{
				MemberID: id, Role: "broker", Shards: &b.lohi,
				Config: broker.AdvertiseAddr(nil, srv.URL), StartedAt: time.Unix(0, 0),
			}, 0, nil))
		}
		return b
	}
	executor := mk(0)
	owner := mk(1)

	// Heartbeat + run both brokers until their own cancel (so we can stop ONE to model
	// a departure/crash while the other keeps running).
	for _, b := range []*meshRecoveryBroker{executor, owner} {
		b.beat() // register before the first peer-refresh so discovery is immediate
		bctx, bcancel := context.WithCancel(ctx)
		b.cancel = bcancel
		t.Cleanup(bcancel)
		beatB := b
		go func() {
			tick := time.NewTicker(200 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-bctx.Done():
					return
				case <-tick.C:
					beatB.beat()
				}
			}
		}()
		go func() { _ = beatB.cl.Dispatcher().Run(bctx) }()
		go func() { beatB.cl.StartRelay(bctx) }()
	}

	f := &meshRecoveryFleet{t: t, pool: pool, st: st, owner: owner, executor: executor}
	// newWorkerA starts a worker dialing an ARBITRARY broker through a FAULT-INJECTING
	// transport so a test can CUT it (crashWorker) — a plain client can only cancel,
	// which drains gracefully. It replaces any prior worker's stop/gate handle so the
	// most recent worker is the one crashWorker targets.
	f.newWorkerA = func(dialAddr string) {
		wctx, wcancel := context.WithCancel(ctx)
		f.workerStop = wcancel
		t.Cleanup(wcancel)
		gate := &faultGate{}
		f.workerGate = gate
		dialer := newFaultDialer(gate)
		// Match h2TestClient's ALPN-h2 + TLS-skip-verify (the brokers serve h2 over TLS
		// via h2Server/StartTLS) but route the underlying dial through the fault gate so
		// the test can CUT the connection. The stdlib negotiates TLS+h2 over our conn.
		var protos http.Protocols
		protos.SetHTTP2(true)
		httpClient := &http.Client{Transport: &http.Transport{
			DialContext:         dialer.dial,
			Protocols:           &protos,
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h2"}}, //nolint:gosec // test-only
			MaxIdleConns:        16,
			MaxIdleConnsPerHost: 8,
			IdleConnTimeout:     30 * time.Second,
		}}
		client := workerpbconnect.NewWorkerServiceClient(httpClient, dialAddr)
		go func() {
			_ = converge.RunWorker(wctx, client, []converge.Provider{noopProvider}, converge.RunOptions{MaxInflight: 16})
		}()
	}
	f.newWorker = func() { f.newWorkerA(executor.srv.URL) }
	f.newWorker()
	return f
}

// submitRootInOwnerTile creates a noop root and REQUIRES it landed in the owner
// (worker-less) broker's tile, so it is claimed by the owner and pushed to the
// executor. Retries names until one hashes into the owner's tile (deterministic
// coverage without depending on hash luck of a single name).
//
// A retry name that hashes into the EXECUTOR's tile instead is DELETED before the
// next attempt: such a root is served only by the executor (the sole broker owning
// that tile), so once the fault departs the executor and the recovery worker joins
// the OWNER (whose dispatcher claims only the owner tile), no surviving broker
// covers the executor tile and the throwaway root can never be re-claimed — a
// permanent strand that is an ARTIFACT of the hash search, not the fault under test.
// Deleting it (the AFTER-DELETE cleanup_orphaned_deps trigger also sweeps its
// work_queue row) keeps the fleet-convergence assertion scoped to exactly the one
// owner-tile root the test intends to recover.
func (f *meshRecoveryFleet) submitRootInOwnerTile(ctx context.Context, namePrefix string) {
	te := &testEngine{Pool: f.pool}
	lo, hi := f.owner.lohi[0], f.owner.lohi[1]
	for i := 0; i < 200; i++ {
		name := fmt.Sprintf("%s-%d", namePrefix, i)
		id, err := te.CreateRoot(ctx, "noop", name, json.RawMessage(`{}`), nil)
		require.NoError(f.t, err)
		var shard int16
		require.NoError(f.t, f.pool.QueryRow(ctx, `SELECT shard_id FROM resources WHERE id = $1`, id).Scan(&shard))
		if shard >= lo && shard <= hi {
			return // landed in the owner's tile → will be claimed by owner + pushed
		}
		// Landed in the executor's tile: drop it so it can't strand post-fault (see doc).
		_, err = f.pool.Exec(ctx, `DELETE FROM resources WHERE id = $1`, id)
		require.NoError(f.t, err)
	}
	f.t.Fatalf("could not place a root in the owner tile [%d,%d] after 200 tries", lo, hi)
}

// waitOwnerClaimedNoop confirms the mesh precondition via the DB: the owner
// (worker-less) broker has CLAIMED a noop row — since it hosts no worker, a claimed
// row is one it is pushing over the mesh to the executor. Replaces the removed
// in-memory push counters with the durable lease state.
func (f *meshRecoveryFleet) waitOwnerClaimedNoop(ctx context.Context, within time.Duration) bool {
	return waitUntil(ctx, within, func() bool {
		var n int
		if err := f.pool.QueryRow(ctx,
			`SELECT count(*) FROM work_queue wq JOIN resources r ON r.id = wq.resource_id
			  WHERE r.kind = 'noop' AND wq.broker_id = $1`, f.owner.id).Scan(&n); err != nil {
			return false
		}
		return n > 0
	})
}

// waitUntil polls cond every 50ms until true or within elapses; returns cond's final value.
func waitUntil(ctx context.Context, within time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(within)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return false
		}
		select {
		case <-time.After(50 * time.Millisecond):
		case <-ctx.Done():
			return false
		}
	}
}

// waitAllNoopSynced polls until every noop root's synced_gen catches its generation
// (the fleet converged) or the deadline. Mirrors the completion predicate the relay
// tests use (broker_relay_test.go). On timeout it dumps the stranded rows' claim
// state so a failure shows WHETHER the row was re-claimed and by whom.
func (f *meshRecoveryFleet) waitAllNoopSynced(ctx context.Context, within time.Duration) {
	f.t.Helper()
	if waitUntil(ctx, within, func() bool {
		var pending int
		if err := f.pool.QueryRow(ctx,
			`SELECT count(*) FROM resources WHERE kind = 'noop' AND synced_gen < generation`).Scan(&pending); err != nil {
			return false
		}
		return pending == 0
	}) {
		return
	}
	rows, _ := f.pool.Query(ctx,
		`SELECT r.id, r.shard_id, wq.broker_id, wq.claim_epoch
		   FROM resources r LEFT JOIN work_queue wq ON wq.resource_id = r.id
		  WHERE r.kind = 'noop' AND r.synced_gen < r.generation LIMIT 8`)
	if rows != nil {
		defer rows.Close()
		for rows.Next() {
			var id string
			var shard int16
			var brokerID *string
			var epoch *int64
			_ = rows.Scan(&id, &shard, &brokerID, &epoch)
			f.t.Logf("STRANDED noop root %s shard=%d claimed_by=%v claim_epoch=%v", id, shard, brokerID, epoch)
		}
	}
	f.t.Fatal("fleet did not converge: some noop roots never synced after the mesh fault")
}

// assertNoNoopStrand asserts the no-strand invariant the relay tests use: after full
// sync every work_queue row for the kind is gone (a fenced AppendOutbox from the
// re-claiming broker drained it). A leftover row is a stranded lease.
func (f *meshRecoveryFleet) assertNoNoopStrand(ctx context.Context) {
	f.t.Helper()
	var leftover int
	require.NoError(f.t, f.pool.QueryRow(ctx,
		`SELECT count(*) FROM work_queue wq JOIN resources r ON r.id = wq.resource_id WHERE r.kind = 'noop'`).Scan(&leftover))
	require.Zerof(f.t, leftover, "work_queue rows remain after full sync — a lease stranded (mesh recovery bug)")
}

// TestBrokerMeshPeerDepartureRecovers (GAP 1): a task PUSHED to a peer that then
// DEPARTS the cluster (taking its only worker with it) must not strand — the owner's
// lease heartbeat lapses, the reaper reclaims the row (bumping its claim_epoch), and
// once a worker is available again the row is re-run and converges. We confirm the
// precondition (the owner claimed + is pushing a noop row), depart the executor, bring
// a worker onto the surviving owner, and assert the fleet converges with no strand and
// no double-apply.
func TestBrokerMeshPeerDepartureRecovers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	pool := setupPostgres(t, ctx)

	// Handler delay long enough to observe the claim + push before injecting the fault.
	f := startMeshRecoveryFleet(t, ctx, pool, 3*time.Second)

	// Submit work into the worker-less owner's tile → owner claims + pushes to executor.
	f.submitRootInOwnerTile(ctx, "opg")

	// PRECONDITION: the owner claimed the row (worker-less → it is pushing over the mesh).
	require.Truef(t, f.waitOwnerClaimedNoop(ctx, 30*time.Second),
		"owner never claimed the noop row — precondition not met (mesh push didn't happen)")
	t.Logf("precondition met: owner %s claimed + is pushing a noop row to executor %s", f.owner.id, f.executor.id)

	// FAULT: the executor DEPARTS the cluster (graceful: delete its member row + stop
	// its loops). Its worker goes with it, so the owner's parked lease can only recover
	// once a fresh worker appears; the reaper reclaims the stale lease meanwhile.
	require.NoError(t, f.st.DeleteClusterMember(ctx, f.executor.id))
	f.executor.cancel()

	// RECOVERY: bring a worker onto the surviving owner broker so the reaper-reclaimed
	// row can be executed locally (no peer left to push to).
	f.newWorkerA(f.owner.srv.URL)

	// ASSERT: the fleet converges — every root synced, no lease stranded, no double-apply.
	f.waitAllNoopSynced(ctx, 90*time.Second)
	f.assertNoNoopStrand(ctx)
	assertNoDoubleApply(t, ctx, pool)
	t.Logf("peer-departure recovery confirmed: reaper re-claimed the pushed row and it converged after the executor departed")
}

// TestBrokerMeshWorkerCrashRecovers (GAP 2): when the WORKER executing a peer's
// pushed (foreign) task crashes while the executing broker stays UP, the owner's
// parked lease must not strand — the reaper reclaims the row (bumping its claim_epoch,
// so the crashed attempt's late result is inert) and it re-runs once a worker
// reconnects. We confirm the precondition, hard-crash the worker, restart a worker,
// and assert the fleet converges with no strand and no double-apply.
func TestBrokerMeshWorkerCrashRecovers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	pool := setupPostgres(t, ctx)

	f := startMeshRecoveryFleet(t, ctx, pool, 3*time.Second)
	f.submitRootInOwnerTile(ctx, "dfg")

	// PRECONDITION: the owner claimed the row and is pushing it to the executor, whose
	// worker is running it.
	require.Truef(t, f.waitOwnerClaimedNoop(ctx, 30*time.Second),
		"owner never claimed the noop row — precondition not met (mesh push didn't happen)")
	t.Logf("precondition met: owner %s pushing a noop row to executor %s's worker", f.owner.id, f.executor.id)

	// FAULT: hard-CRASH the executor's WORKER (cut its transport so no clean Complete
	// escapes, then cancel) while its broker stays up. The pushed row's lease is held
	// by the owner and goes stale; the reaper reclaims it.
	f.crashWorker()

	// RECOVERY: reconnect a worker to the executor so the reaper-reclaimed row re-runs.
	f.newWorker()

	// ASSERT: the fleet converges — every root synced, no lease stranded, no double-apply.
	f.waitAllNoopSynced(ctx, 90*time.Second)
	f.assertNoNoopStrand(ctx)
	assertNoDoubleApply(t, ctx, pool)
	t.Logf("worker-crash recovery confirmed: reaper re-claimed the foreign row and it converged after the worker crashed")
}
