package test

// TestControlPlaneHA proves the CONTROL tier is highly available: it runs MULTIPLE
// control pods that shard the keyspace leaderless (each registers role=control in
// cluster_members, a Resharder assigns it a contiguous tile via assign_member_shards,
// and the sweepers — drainer/reaper/specgc — claim only their tile via FOR UPDATE
// SKIP LOCKED). A chaos driver then churns the control pods (crash / graceful drain /
// join / rolling-restart) WHILE a broker+worker fleet processes the classicbom
// workload, and asserts the control tier keeps making progress with no double-apply
// and no permanent stranding.
//
// This is the control-tier HA proof the infra-fault chaos test (TestBrokerChaos)
// does NOT cover — that one runs a single control pod. Here the SWEEPERS themselves
// (drainer applying outbox, reaper reclaiming leases) are the thing under chaos, and
// resharding correctness (scale-event debounce, in-flight spanning a control reshard)
// is exercised as a side effect of the churn.
//
// Run:
//   go test -count=1 -timeout 40m ./test/ -run TestControlPlaneHA -v -args -chaos-ha
//   ( -chaos-ha-seed to reproduce; -chaos-ha-budget to bound )

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/broker"
	"github.com/salesforce/converge/internal/engine"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/sdk-go/converge"
	"github.com/salesforce/converge/sdk-go/workerpb/workerpbconnect"
	"github.com/salesforce/converge/test/internal/noop"
)

var (
	chaosHAEnabled = flag.Bool("chaos-ha", false, "Run the control-plane HA chaos test (default: skip)")
	chaosHASeed    = flag.Int64("chaos-ha-seed", 1, "RNG seed")
	chaosHABudget  = flag.Duration("chaos-ha-budget", 8*time.Minute, "max wall time before failing")
	chaosHATick    = flag.Duration("chaos-ha-tick", 2500*time.Millisecond, "control-pod chaos cadence")
	chaosHANoop    = flag.Int("chaos-ha-noop-count", 500, "noop root count")
)

func TestControlPlaneHA(t *testing.T) {
	if !*chaosHAEnabled {
		t.Skip("control-plane HA chaos disabled; run with -args -chaos-ha")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *chaosHABudget+3*time.Minute)
	defer cancel()

	// Wide pool: N control pods × (drainer bands + reaper + specgc + resharder +
	// reporter) all touch the DB concurrently.
	basePool := setupPostgres(t, ctx)
	bigCfg, err := pgxpool.ParseConfig(basePool.Config().ConnString())
	require.NoError(t, err)
	bigCfg.MaxConns = 60
	basePool.Close()
	pool, err := pgxpool.NewWithConfig(ctx, bigCfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	rng := rand.New(rand.NewSource(*chaosHASeed))

	// noop workload — the point is control-tier chaos, not provider chaos. A
	// deliberate per-call delay keeps the run long enough that the chaos driver
	// actually fires many control-pod faults (otherwise 250 instant noops finish in
	// ~1s and no chaos runs — the churn is the whole point).
	nr := noop.New(300 * time.Millisecond)
	reg := newTReg()
	reg.Add(nr)
	migrateForSeed(t, ctx, pool)
	require.NoError(t, reg.seed(ctx, pool))

	mc := runtime.NewKindManifestCache(pool)
	require.NoError(t, mc.Load(ctx))
	mc.Start(ctx)
	t.Cleanup(mc.Stop)

	// ── ONE broker + ONE worker (the data plane is stable; the CONTROL tier churns).
	// The broker owns the whole keyspace (it's the only broker); control pods shard
	// among THEMSELVES for the sweepers.
	bsrv := broker.NewDispatch(pool, mc, "broker-ha", 16, runtime.NewShardSet(runtime.AllShards()))
	mux := http.NewServeMux()
	mux.Handle(bsrv.Handler())
	bhttp := httptest.NewUnstartedServer(mux)
	bhttp.EnableHTTP2 = true
	bhttp.StartTLS()
	t.Cleanup(bhttp.Close)
	dpCtx, dpCancel := context.WithCancel(ctx)
	t.Cleanup(dpCancel)
	go func() { _ = bsrv.Dispatcher().Run(dpCtx) }()

	client := workerpbconnect.NewWorkerServiceClient(bhttp.Client(), bhttp.URL)
	go func() {
		_ = converge.RunWorker(dpCtx, client, []converge.Provider{nr}, converge.RunOptions{MaxInflight: 16})
	}()

	// ── the supervised CONTROL fleet ────────────────────────────────────────────
	cf := &controlFleet{t: t, ctx: ctx, pool: pool, rng: rng}
	cf.bootstrap(3) // 3 control pods, sharding the keyspace among themselves
	t.Cleanup(cf.shutdown)

	// ── submit workload ─────────────────────────────────────────────────────────
	te := &testEngine{Pool: pool}
	expected := *chaosHANoop
	for i := 0; i < expected; i++ {
		_, err := te.CreateRoot(ctx, model.Kind(noop.Kind), fmt.Sprintf("ha-noop-%d", i), json.RawMessage(`{}`), nil)
		require.NoError(t, err)
	}
	t.Logf("=== control-plane HA: %d roots, 3 control pods (sharding), seed=%d budget=%v ===",
		expected, *chaosHASeed, *chaosHABudget)

	// ── run control chaos + invariant checker until complete or budget ──────────
	start := time.Now()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	var wg sync.WaitGroup

	viol := newViolations()
	wg.Add(1)
	go func() { defer wg.Done(); cf.checkInvariants(runCtx, expected, viol) }()
	wg.Add(1)
	go func() { defer wg.Done(); cf.runChaos(runCtx, *chaosHATick) }()

	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)
		waitReadyOrCtx(runCtx, pool, expected, func(c context.Context, p *pgxpool.Pool) int {
			var n int
			_ = p.QueryRow(c, `SELECT count(*) FROM resources WHERE kind='noop' AND synced_gen>=generation AND generation>0`).Scan(&n)
			return n
		})
	}()

	select {
	case <-done:
		t.Logf("=== all %d roots reconciled in %v through %d control-pod chaos actions ===",
			expected, time.Since(start).Round(time.Millisecond), cf.actions)
	case <-time.After(*chaosHABudget):
		runCancel()
		wg.Wait()
		t.Fatalf("budget %v exceeded before all %d roots reconciled — control tier failed to make progress under chaos", *chaosHABudget, expected)
	}
	runCancel()
	wg.Wait()
	viol.assert(t)

	// Fence at rest: no synced_gen over-advance (a double-apply from two control pods
	// draining the same outbox row would show here).
	var over int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM resources WHERE synced_gen > generation`).Scan(&over))
	require.Zero(t, over, "synced_gen > generation — two control pods double-applied an outbox row")
	t.Logf("=== control-plane HA PASSED: sharded sweepers self-healed through %d chaos actions ===", cf.actions)
}

// ── managed control pod ─────────────────────────────────────────────────────────

// controlPod is one restartable control-plane process: a ShardSet assigned by its
// own Resharder (reading cluster_members role=control), a ClusterMemberReporter
// (role=control) so it participates in the sharding, and the control engine
// (drainer/reaper/specgc) scoped to that ShardSet. stop() drains + deregisters;
// kill() crashes (no deregister → the ClusterMemberGC/liveness must re-tile it out).
type controlPod struct {
	id   string
	pool *pgxpool.Pool

	mu       sync.Mutex
	shards   *runtime.ShardSet
	eng      *engine.Engine
	reporter *runtime.ClusterMemberReporter
	watch    *runtime.TopologyWatcher
	cancel   context.CancelFunc
	running  bool
}

func (p *controlPod) start(t *testing.T, parent context.Context) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.running {
		return
	}
	p.shards = runtime.NewShardSet(nil) // Resharder assigns the tile
	duties, _ := engine.DutiesFromConfig(engine.EngineConfig{
		RunControl:        true,
		SweeperStaleAfter: 15 * time.Second,
		SweeperInterval:   500 * time.Millisecond,
		RetryAfter:        2 * time.Second,
	})
	eng := engine.NewEngine(duties, engine.Deps{Pool: p.pool, Shards: p.shards})

	ctx, cancel := context.WithCancel(parent)
	p.cancel = cancel

	// reporter: role=control so assign_member_shards tiles this pod among the other
	// control pods. Fast heartbeat so a crashed pod ages out quickly.
	rep := runtime.NewClusterMemberReporter(store.New(p.pool), store.ClusterMemberInfo{
		MemberID: p.id, Role: "control", StartedAt: time.Unix(0, 0),
	})
	rep.Shards = p.shards
	rep.Interval = 1 * time.Second
	rep.Start(ctx)
	p.reporter = rep

	// TopologyWatcher drives the resharder reactor; tighten the failsafe/liveness/
	// debounce for test-speed re-tiling. A crashed pod is excluded within LivenessWindow.
	resh := runtime.NewResharder(store.New(p.pool), p.id, p.shards)
	resh.LivenessWindow = 6 * time.Second
	watch := runtime.NewTopologyWatcher(runtime.NewPgxListener(p.pool))
	watch.Interval = 2 * time.Second
	watch.LivenessWindow = 6 * time.Second
	watch.DebounceWindow = 500 * time.Millisecond
	watch.Register("resharder", resh.Reactor(), rep.Trigger)
	watch.Start(ctx)
	p.watch = watch

	require.NoError(t, eng.Start(ctx))
	p.eng = eng
	p.running = true
}

// stop drains gracefully: stop the engine, stop the resharder/reporter, deregister.
func (p *controlPod) stop() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	p.running = false
	cancel, eng, rep, watch := p.cancel, p.eng, p.reporter, p.watch
	p.mu.Unlock()

	sctx, sc := context.WithTimeout(context.Background(), 5*time.Second)
	_ = eng.Stop(sctx)
	sc()
	watch.Stop()
	rep.Stop()
	dctx, dc := context.WithTimeout(context.Background(), 3*time.Second)
	_ = rep.Deregister(dctx)
	dc()
	cancel()
}

// kill crashes: cancel everything WITHOUT deregistering — the topology watcher's
// liveness window (+ ClusterMemberGC eventually) must re-tile this pod's shards onto survivors.
func (p *controlPod) kill() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	p.running = false
	cancel, eng, rep, watch := p.cancel, p.eng, p.reporter, p.watch
	p.mu.Unlock()
	// stop the in-process loops (the process is "gone") but DON'T deregister the row.
	sctx, sc := context.WithTimeout(context.Background(), 2*time.Second)
	_ = eng.Stop(sctx)
	sc()
	watch.Stop()
	rep.Stop()
	cancel()
}

func (p *controlPod) isRunning() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

// ── control fleet supervisor ─────────────────────────────────────────────────────

type controlFleet struct {
	t    *testing.T
	ctx  context.Context
	pool *pgxpool.Pool
	rng  *rand.Rand

	mu      sync.Mutex
	pods    []*controlPod
	next    int
	actions int
}

func (cf *controlFleet) bootstrap(n int) {
	for i := 0; i < n; i++ {
		cf.addPod()
		time.Sleep(150 * time.Millisecond) // stagger so the resharder tiles cleanly
	}
}

func (cf *controlFleet) addPod() {
	cf.mu.Lock()
	// prune dead
	kept := cf.pods[:0]
	for _, p := range cf.pods {
		if p.isRunning() {
			kept = append(kept, p)
		}
	}
	cf.pods = kept
	if len(cf.pods) >= 5 { // cap
		cf.mu.Unlock()
		return
	}
	id := fmt.Sprintf("control-%d", cf.next)
	cf.next++
	p := &controlPod{id: id, pool: cf.pool}
	cf.pods = append(cf.pods, p)
	cf.mu.Unlock()
	p.start(cf.t, cf.ctx)
}

func (cf *controlFleet) liveCount() int {
	cf.mu.Lock()
	defer cf.mu.Unlock()
	n := 0
	for _, p := range cf.pods {
		if p.isRunning() {
			n++
		}
	}
	return n
}

func (cf *controlFleet) pickLive() *controlPod {
	cf.mu.Lock()
	defer cf.mu.Unlock()
	live := make([]*controlPod, 0, len(cf.pods))
	for _, p := range cf.pods {
		if p.isRunning() {
			live = append(live, p)
		}
	}
	if len(live) == 0 {
		return nil
	}
	return live[cf.rng.Intn(len(live))]
}

func (cf *controlFleet) runChaos(ctx context.Context, tick time.Duration) {
	tk := time.NewTicker(tick)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			cf.oneAction()
		}
	}
}

func (cf *controlFleet) oneAction() {
	cf.mu.Lock()
	cf.actions++
	n := cf.actions
	cf.mu.Unlock()
	switch cf.rng.Intn(5) {
	case 0: // graceful drain + replace
		if cf.liveCount() > 1 {
			if p := cf.pickLive(); p != nil {
				cf.t.Logf("control-chaos[%d] drain %s (graceful stop + deregister)", n, p.id)
				p.stop()
				cf.addPod()
			}
		}
	case 1: // hard crash + replace (no deregister → liveness must re-tile it out)
		if cf.liveCount() > 1 {
			if p := cf.pickLive(); p != nil {
				cf.t.Logf("control-chaos[%d] CRASH %s (no deregister; liveness+GC must re-tile)", n, p.id)
				p.kill()
				cf.addPod()
			}
		}
	case 2: // join (scale-out)
		cf.t.Logf("control-chaos[%d] join a control pod (scale-out → reshard)", n)
		cf.addPod()
	case 3: // rolling restart: drain every pod one-by-one, keeping a ≥1 floor
		cf.t.Logf("control-chaos[%d] rolling-restart the control tier", n)
		cf.mu.Lock()
		pods := append([]*controlPod(nil), cf.pods...)
		cf.mu.Unlock()
		for _, p := range pods {
			if cf.liveCount() <= 1 || !p.isRunning() {
				continue
			}
			p.stop()
			cf.addPod()
			time.Sleep(400 * time.Millisecond)
		}
	case 4: // brief total-control outage: kill all, then bring a fresh set up
		cf.t.Logf("control-chaos[%d] TOTAL control outage (all pods down, then restart)", n)
		cf.mu.Lock()
		pods := append([]*controlPod(nil), cf.pods...)
		cf.mu.Unlock()
		for _, p := range pods {
			p.kill()
		}
		for i := 0; i < 3; i++ {
			cf.addPod()
		}
	}
}

func (cf *controlFleet) checkInvariants(ctx context.Context, expected int, viol *violations) {
	tk := time.NewTicker(2 * time.Second)
	defer tk.Stop()
	var lastReady int
	stalledSince := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
		}
		// no double-apply
		var over int
		_ = cf.pool.QueryRow(ctx, `SELECT count(*) FROM resources WHERE synced_gen > generation`).Scan(&over)
		if over > 0 {
			viol.add(fmt.Sprintf("%d resources synced_gen > generation (two control pods double-applied)", over))
		}
		// progress liveness — with NO control pod alive, no progress is EXPECTED
		// (the sweepers ARE the control tier), so skip the stall clock then.
		var ready int
		_ = cf.pool.QueryRow(ctx, `SELECT count(*) FROM resources WHERE kind='noop' AND synced_gen>=generation AND generation>0`).Scan(&ready)
		if ready >= expected {
			return
		}
		if ready > lastReady {
			lastReady = ready
			stalledSince = time.Now()
		} else if cf.liveCount() == 0 {
			stalledSince = time.Now() // total control outage: no progress expected
		} else if time.Since(stalledSince) > 90*time.Second {
			viol.add(fmt.Sprintf("progress stalled at %d/%d ready for >90s with live control pods (sweepers not progressing)", ready, expected))
			stalledSince = time.Now()
		}
	}
}

func (cf *controlFleet) shutdown() {
	cf.mu.Lock()
	pods := append([]*controlPod(nil), cf.pods...)
	cf.mu.Unlock()
	for _, p := range pods {
		p.stop()
	}
}
