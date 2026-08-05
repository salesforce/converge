package test

// TestBrokerChaos is a fault-injection soak over the REAL Connect broker↔worker layer.
// A fleet of brokers (each owning a shard tile, serving the broker Connect services on a real
// listener) and dumb workers (real converge.RunWorker clients over a fault-injectable
// transport) processes the classicbom workload while a chaos driver continuously:
//
//   - gracefully drains + restarts workers and brokers (SIGTERM rollout),
//   - hard-kills workers and brokers (crash / node loss — no clean release),
//   - joins new workers and brokers mid-run (scale-out / churn),
//   - partitions workers from the fleet: abrupt RST and silent half-open,
//   - and (via the probe) injects in-handler transient failures + latency.
//
// Throughout, invariant checkers assert the layer keeps its guarantees:
//   1. EVENTUAL COMPLETION — every composed child reaches ready despite all faults.
//   2. NO DOUBLE-APPLY — synced_gen advances at most once per generation (the fence
//      holds even when a task EXECUTES twice under reclaim/relay/crash).
//   3. PROGRESS LIVENESS — ready-count strictly climbs over sliding windows (no
//      total livelock during churn).
//   4. NO PERMANENT STRANDING — no work_queue row stays claimed-but-idle past a
//      bound (reaper / keepalive / release must recover it).
//
// This is the end-to-end proof that the layer's durability guarantees hold under
// real concurrent chaos, not just in isolation.
//
// With -chaos-reactors (requires -chaos-workload=bom and a non-db scenario), the reactor
// (lifecycle saga) spine rides the SAME churn: statussink is bound to the account child
// kind's 'synced' transition, every managed broker also runs a ReactorDispatcher (torn
// down + restarted with the broker on kill/drain), and the fault-wrapped reactor handler
// runs on the dumb workers over STAGE_REACT. The completion predicate then additionally
// requires lifecycle_outbox to fully drain — so a reactor delivery stranded by the churn
// fails the run just like a stranded reconcile task would. This puts the reactor path's
// OWN table/claim/ack/reap through broker crash/drain/partition + transient reaction
// failures, the coverage the reconcile path gets. (The `db` scenario is excluded with
// reactors: its near-continuous outages stall the reconcile+drain bar past any bounded
// budget — a severity limit; the reactor DB-fault path is covered by the deterministic
// TestReactorOutboxWritersNoDeadlock and the dispatcher's retry-through-partition.)
//
// Run:
//   go test -count=1 -timeout 90m ./test/ -run TestBrokerChaos -v -args -chaos
//   ( -chaos-workload=noop for a fast CI smoke; -chaos-seed to reproduce )
//   ( add -chaos-reactors to also churn the reactor spine — bom workload, non-db scenario )

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/examples/demos/classic/fault"
	"github.com/salesforce/converge/examples/demos/classic/networking"
	"github.com/salesforce/converge/examples/demos/classic/statussink"
	"github.com/salesforce/converge/internal/engine"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/sdk-go/converge"
	"github.com/salesforce/converge/test/internal/demoruntime"
	"github.com/salesforce/converge/test/internal/noop"
)

var (
	chaosEnabled  = flag.Bool("chaos", false, "Run the broker/worker chaos soak (default: skip)")
	chaosWorkload = flag.String("chaos-workload", "bom", "Workload: bom (classicbom ~5.4k) | noop")
	chaosSeed     = flag.Int64("chaos-seed", 1, "RNG seed for reproducible chaos")
	chaosNoop     = flag.Int("chaos-noop-count", 400, "noop root count when -chaos-workload=noop")
	chaosBudget   = flag.Duration("chaos-budget", 30*time.Minute, "max wall time before the run fails")
	chaosTick     = flag.Duration("chaos-tick", 2500*time.Millisecond, "chaos action cadence")
	chaosFailRate = flag.Float64("chaos-fail-rate", 0.10, "in-handler transient failure probability")
	// chaosScenario selects which FAULT CLASS the driver injects. Each action is
	// tagged with the scenarios it belongs to; the driver picks (seeded-random) only
	// among actions active for the selected scenario. "all" (default) mixes every
	// class in one soak.
	//   base    — worker/broker drain+kill+join + worker RST/half-open partition (the original set)
	//   db      — Postgres blip, broker↔DB partition, control-plane (drainer/reaper) restart
	//   rollout — rolling broker restart, simultaneous multi-broker loss, node loss, brief zero-broker outage
	//   network — broker↔broker relay partition, asymmetric (send/recv-only), latency, flaky link
	//   worker  — hung handler past ceiling, crash-replace, drain-restart, permanent scale-in
	//   all     — every action above
	chaosScenario = flag.String("chaos-scenario", "all", "fault class: base|db|rollout|network|worker|all")
	// chaosReactors turns the reactor (lifecycle saga) spine ON for the soak: statussink
	// is bound to the workload's 'synced' transition, every managed broker also runs a
	// ReactorDispatcher (torn down + restarted with the broker), and the reactor handler
	// runs on the dumb workers over Connect. This puts the reactor path's OWN table/claim/
	// ack/reap through the SAME churn (broker kill/drain/partition, DB blip) as reconcile —
	// and the completion predicate then requires lifecycle_outbox to fully drain. OFF by
	// default so a plain chaos run isn't slowed by the extra deliveries.
	chaosReactors = flag.Bool("chaos-reactors", false, "Also drive the reactor spine through the chaos churn (requires -chaos-workload=bom; not the db/all scenario)")
)

// numShards mirrors the 256-shard keyspace; the fleet splits it across brokers.
const chaosTotalShards = 256

func TestBrokerChaos(t *testing.T) {
	if !*chaosEnabled {
		t.Skip("chaos soak disabled; run with -args -chaos")
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))

	// The outer ctx must outlive the completion budget (below) plus its reactor headroom
	// plus the shutdown drain. Reactor mode raises the completion bar (reconcile-ready AND
	// lifecycle_outbox drained) so it gets extra grace; size the ctx to cover the widest case.
	ctx, cancel := context.WithTimeout(context.Background(), *chaosBudget+*chaosBudget/2+5*time.Minute)
	defer cancel()

	// A WIDE pool: the control engine + up to chaosMaxBrokers brokers each run
	// several concurrent DB loops (claim / heartbeat / relay ListClusterMembers /
	// drain / reap). The default pgx pool (~NumCPU) starves under that fan-out during
	// churn and wedges the run — so size it generously for the fleet. (Workers hold
	// NO DB connection; only the control plane + brokers touch Postgres.)
	//
	// The pool dials through a fault-gated DialFunc (dbGate) so the "db" scenario can
	// model a Postgres blip / broker↔DB partition by tripping the gate + pool.Reset()
	// (drops live conns) — deterministic and reversible, at the DB socket layer.
	basePool := setupPostgres(t, ctx)
	bigCfg, err := pgxpool.ParseConfig(basePool.Config().ConnString())
	require.NoError(t, err)
	bigCfg.MaxConns = 50
	dbGate := &faultGate{}
	baseDBDialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	bigCfg.ConnConfig.DialFunc = func(dctx context.Context, network, addr string) (net.Conn, error) {
		if dbGate.get() == faultCut {
			return nil, fmt.Errorf("chaos: db partition (simulated)")
		}
		return baseDBDialer.DialContext(dctx, network, addr)
	}
	basePool.Close()
	pool, err := pgxpool.NewWithConfig(ctx, bigCfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	rng := rand.New(rand.NewSource(*chaosSeed))

	// ── workload + registry ──────────────────────────────────────────────────
	// One SHARED probe: fleet-wide transient failures + double-apply accounting.
	probe := newChaosProbe(*chaosFailRate, 50*time.Millisecond, 400*time.Millisecond, *chaosSeed)
	reg := newTReg()
	var workerProviders []converge.Provider

	if *chaosWorkload == "noop" {
		nr := noop.New(80 * time.Millisecond)
		reg.Add(nr)
		workerProviders = probe.wrapRuntimes([]converge.Provider{nr})
	} else {
		// classicbom: composer (in the broker's manifest, but its compose reaction runs
		// on a worker too) + account + networking workers, all probe-wrapped.
		reg.Add(demoruntime.ClassicBOM(0))
		reg.Add(demoruntime.Account(0, 0, fault.Injector{}))
		for _, c := range networking.AllRuntimes(0, fault.Injector{}) {
			reg.Add(c)
		}
		workerProviders = probe.wrapRuntimes(reg.providerList())
	}

	// Reactor spine (opt-in via -chaos-reactors): register statussink so its manifest
	// seeds (the claim resolves its reaction) and add it to the workers so the reactor
	// handler runs remotely over Connect — the SAME STAGE_REACT path prod uses. The
	// managed brokers each run a ReactorDispatcher (wired in managedBroker.start below);
	// the binding is applied after the fleet is up. The reactor handler IS fault-wrapped
	// (a flakyWrap at the same transient-fail rate as the reconcile probe): a reactor
	// delivery fails transiently, stays claimed, and the reaper re-arms it (at-least-once)
	// — so a failing reactor exercises the SIDE of the fence the reconcile probe can't, and
	// the completion predicate (lifecycle_outbox must fully drain) proves it still reaches
	// exactly-once effect despite failures + broker churn. The statussink upload key embeds
	// the generation, so a re-fired delivery is an idempotent overwrite (see the dedicated
	// exactly-once proof in TestLifecycleReactorExactlyOnceEffectUnderRedelivery).
	//
	// Reactors REQUIRE the bom workload: statussink watches a CHILD kind (account) crossing
	// 'synced', the representative "root rolls up → a child's status fires a reactor" saga.
	// The noop workload is a root-only Worker kind with no children, so there's no
	// representative kind to bind — a self-binding on the root is a degenerate config, not
	// coverage. Fail fast rather than run something meaningless.
	runReactors := *chaosReactors
	if runReactors && *chaosWorkload == "noop" {
		t.Fatal("-chaos-reactors requires -chaos-workload=bom (statussink watches the account child kind; noop has no children to react to)")
	}
	// -chaos-reactors is incompatible with the `db` (and the `db`-inclusive `all`) scenario.
	// The completion bar with reactors is reconcile 100% AND lifecycle_outbox fully drained;
	// the db scenario keeps Postgres down for the majority of the run (a 3-8s outage every
	// ~2.5s tick — measured ~429/599 actions were outages), so neither reconcile nor the
	// reactor drain gets a clear window to finish within any bounded soak. That is a scenario
	// SEVERITY limit, not a reactor defect: the DB-fault resilience of the reactor spine is
	// covered elsewhere — TestReactorOutboxWritersNoDeadlock is the deterministic guard for the
	// DB-reconnect-storm deadlock a restart induces, and the ReactorDispatcher's claim/heartbeat
	// correctly log+retry through a partition (no crash, no strand). Reactors ride the `base`,
	// `rollout`, `network`, and `worker` scenarios (broker/worker churn + partitions) fine.
	if runReactors && (*chaosScenario == "db" || *chaosScenario == "all") {
		t.Fatalf("-chaos-reactors is incompatible with -chaos-scenario=%s: continuous DB outages stall the reconcile+lifecycle-drain completion bar past any bounded budget (a severity limit, not a reactor bug). Reactor DB-fault resilience is covered by TestReactorOutboxWritersNoDeadlock + the ReactorDispatcher's retry-through-partition; use -chaos-scenario=base|rollout|network|worker with reactors.", *chaosScenario)
	}
	reactorWatch := model.Kind(account.Kind) // most numerous kind → most reactor deliveries
	if runReactors {
		sinkKR, _ := demoruntime.StatusSink("mem://chaos-sink", `{"endpoint":"mem://chaos-sink","prefix":"rolled-up/"}`)
		reg.Add(sinkKR)
		// Fault-wrap the reactor handler so deliveries fail transiently like reconcile work.
		flakySink, _ := flakeRuntime(sinkKR, 20*time.Millisecond, 120*time.Millisecond, *chaosFailRate, *chaosSeed)
		workerProviders = append(workerProviders, flakySink)
	}

	// ── control plane (drainer + reaper), tuned for chaos-relevant recovery ────
	migrateForSeed(t, ctx, pool)
	require.NoError(t, reg.seed(ctx, pool))
	// The control plane is RESTARTABLE (the "db" scenario bounces the drainer/reaper):
	// a mutex-guarded handle + a rebuild closure the fault action calls to stop the
	// current engine and start a fresh one on the same pool.
	control := &restartableControl{pool: pool, duties: func() []engine.Duty {
		d, _ := engine.DutiesFromConfig(engine.EngineConfig{
			RunControl:        true,
			SweeperStaleAfter: 15 * time.Second,
			SweeperInterval:   1 * time.Second,
			RetryAfter:        3 * time.Second,
		})
		return d
	}}
	require.NoError(t, control.start(ctx))
	t.Cleanup(func() { control.stop() })

	// shared manifest cache for all brokers.
	mc := runtime.NewKindManifestCache(pool)
	require.NoError(t, mc.Load(ctx))
	mc.Start(ctx)
	t.Cleanup(mc.Stop)

	// ── the supervised fleet ───────────────────────────────────────────────────
	fleet := newChaosFleet(t, ctx, pool, mc, workerProviders, rng)
	fleet.dbGate = dbGate
	fleet.control = control
	fleet.scenario = *chaosScenario
	fleet.reactors = runReactors
	fleet.bootstrap(3, 5) // 3 brokers (split the keyspace), 5 workers
	t.Cleanup(fleet.shutdown)

	// With -chaos-reactors, bind statussink to the watched kind's 'synced' transition now
	// that the fleet's brokers (each running a ReactorDispatcher) are up — bindings are
	// read live on the next claim. Every synced transition of a watched resource then emits
	// a lifecycle_outbox delivery the reactor dispatchers drain through the churn.
	if runReactors {
		require.NoError(t, store.New(pool).UpsertReactorBinding(ctx, store.ReactorBinding{
			Name:       "chaos-synced-to-statussink",
			WatchKind:  reactorWatch,
			Transition: string(model.TransitionSynced),
			Reactor:    model.Kind(statussink.Kind),
			Enabled:    true,
		}))
		t.Logf("[reactor] statussink bound to %s/synced; reactor spine rides the chaos churn", reactorWatch)
	}

	// ── submit the workload ────────────────────────────────────────────────────
	te := &testEngine{Pool: pool}
	expected := submitChaosWorkload(t, ctx, te, *chaosWorkload, *chaosNoop)
	t.Logf("=== chaos: workload=%s expected=%d brokers=3 workers=5 seed=%d budget=%v ===",
		*chaosWorkload, expected, *chaosSeed, *chaosBudget)

	// ── run the chaos driver + invariant checkers until complete or budget ─────
	start := time.Now()
	var wg sync.WaitGroup
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	// completion watcher: signals done when the workload is fully reconciled. The
	// "done" predicate differs by workload — noop is a root-only Worker kind (no
	// children/owner), so it's measured by synced_gen catching up; classicbom
	// composes owned children measured by is_ready. With -chaos-reactors it ALSO
	// requires lifecycle_outbox to drain (every reactor delivery claimed + acked
	// through the churn), so the run isn't "done" until the reactor spine caught up too.
	readyCount := readyCounter(*chaosWorkload)
	doneCh := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		if !waitReadyOrCtx(runCtx, pool, expected, readyCount) {
			return
		}
		if runReactors && !waitLifecycleDrainedOrCtx(runCtx, pool) {
			return
		}
		close(doneCh)
	}()

	// chaos driver.
	wg.Add(1)
	go func() { defer wg.Done(); fleet.runChaos(runCtx, *chaosTick) }()

	// invariant checkers (double-apply, liveness, stranding) — fail the test on breach.
	viol := newViolations()
	wg.Add(1)
	go func() { defer wg.Done(); fleet.checkInvariants(runCtx, pool, expected, readyCount, viol) }()

	// Reactor mode gets +50% budget headroom: its completion bar is strictly harder
	// (reconcile-ready AND every reactor delivery drained), and the fault-wrapped reactor
	// handler adds a retry tail AFTER the last resource is ready — a bounded drain the
	// reconcile-only run doesn't pay. This is proportional grace, not an open-ended wait:
	// a genuinely STRANDED delivery (claimed forever, or unclaimed with no worker) still
	// trips the timeout, and dumpChaosState now prints lifecycle depth so the failure shows
	// whether the spine was draining (in-flight, benign-slow) or stuck (a real strand).
	completionBudget := *chaosBudget
	if runReactors {
		completionBudget += *chaosBudget / 2
	}
	select {
	case <-doneCh:
		t.Logf("=== all %d resources ready in %v ===", expected, time.Since(start).Round(time.Millisecond))
	case <-time.After(completionBudget):
		runCancel()
		wg.Wait()
		dumpChaosState(t, ctx, pool)
		t.Fatalf("chaos budget %v exceeded before all %d resources became ready", completionBudget, expected)
	}
	runCancel()
	wg.Wait()

	// ── final assertions ───────────────────────────────────────────────────────
	// The broker-MESH fast-recovery edges (onPeerGone / dropForeign / relay overflow)
	// are asserted DETERMINISTICALLY in test/broker_mesh_recovery_test.go, not here —
	// the soak's churn can't reliably build the parked-push precondition (see the note
	// in chaos_faults.go). This suite still exercises the mesh under churn via
	// relay-slow-peer + the workers<brokers relay paths, guarded by the invariants below.
	viol.assert(t)

	// No double-apply: every resource's synced_gen == generation and is_ready — and
	// the DB never regressed synced_gen (checked live by the invariant loop). Report
	// the execution multiplicity the chaos induced (allowed; informational).
	multi := probe.multiApplied()
	calls, fails, aborts := probe.stats()
	t.Logf("=== probe: calls=%d transient_fails=%d ctx_aborts=%d re_executed_units=%d ===",
		calls, fails, aborts, len(multi))
	if len(multi) > 0 {
		shown := 0
		for k, n := range multi {
			t.Logf("    re-executed %s/%s gen=%d ×%d (at-least-once; fence must dedup the APPLY)", k.kind, k.res, k.gen, n)
			if shown++; shown >= 5 {
				break
			}
		}
	}
	require.Greater(t, fails, int64(0), "expected some transient failures at rate %.2f", *chaosFailRate)

	// Fence proof at rest: no resource left failed/unsynced, and synced_gen never
	// exceeded generation (a double-apply would over-advance it).
	assertNoDoubleApply(t, ctx, pool)
	t.Logf("=== chaos PASSED: fleet self-healed through %d chaos actions ===", fleet.actionCount())
}

// ── chaos fleet supervisor ─────────────────────────────────────────────────────

type chaosFleet struct {
	t         *testing.T
	ctx       context.Context
	pool      *pgxpool.Pool
	mc        *runtime.KindManifestCache
	wruntimes []converge.Provider
	rng       *rand.Rand

	// fault handles for the richer scenarios (nil on the base scenario).
	dbGate   *faultGate          // trips the pool's DialFunc → DB blip / broker↔DB partition
	dbBusy   atomic.Bool         // true while a db cut is in progress OR in its recovery gap
	control  *restartableControl // drainer/reaper, bounced by the db scenario
	scenario string              // active fault class (base|db|rollout|network|worker|all)

	// reactors turns on a per-broker ReactorDispatcher (torn down + restarted with each
	// managed broker) so the reactor spine rides the SAME churn as reconcile. Off → the
	// brokers run only the work dispatcher (the default chaos shape).
	reactors bool

	mu      sync.Mutex
	brokers []*managedBroker
	workers []*managedWorker
	nextB   int
	nextW   int
	actions int
}

// gatedRelayClient is the HTTP transport a broker uses to dial PEER brokers for
// relay, dialing through a fault gate so the network scenario can sever this
// broker's peer reachability (Route/RelayComplete) — the peer-RPC timeout paths —
// independently of its worker-facing listener. The chaos brokers serve cleartext
// on their listeners (no TLS in-test).
func gatedRelayClient(gate *faultGate) connect.HTTPClient {
	d := newFaultDialer(gate)
	return &http.Client{Transport: &http.Transport{
		DialContext:         d.dial,
		MaxIdleConns:        32,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     30 * time.Second,
	}}
}

func newChaosFleet(t *testing.T, ctx context.Context, pool *pgxpool.Pool, mc *runtime.KindManifestCache,
	wruntimes []converge.Provider, rng *rand.Rand) *chaosFleet {
	return &chaosFleet{t: t, ctx: ctx, pool: pool, mc: mc, wruntimes: wruntimes, rng: rng}
}

// bootstrap stands up the initial fleet: nb brokers splitting the keyspace, nw
// workers each dialing a broker. Relay is ON so a worker on any broker can drain
// any tile (and so partitioning a worker's broker still lets peers cover it).
func (f *chaosFleet) bootstrap(nb, nw int) {
	for i := 0; i < nb; i++ {
		f.addBroker()
	}
	// give brokers a moment to register so workers dial a live one.
	time.Sleep(300 * time.Millisecond)
	for i := 0; i < nw; i++ {
		f.addWorker()
	}
}

// fleet size caps: churn must happen WITHIN a bound, not grow unboundedly. Each
// live broker/worker runs goroutines that hold pgx pool connections (dispatcher,
// heartbeat, relay), so an unbounded fleet exhausts the pool and wedges the run
// (this was a real harness bug: scale-out + kill-and-replace grew the fleet until
// the pool starved). These caps keep the churn realistic and the pool healthy.
const (
	chaosMaxBrokers = 6
	chaosMaxWorkers = 8
)

// addBroker starts a new broker owning a re-balanced tile — UNLESS the fleet is
// already at the broker cap (then it's a no-op, so scale-out churns within bounds).
// It first prunes stopped brokers so the cap counts only live ones. On membership
// change we re-tile ALL live brokers so together they still cover the keyspace.
func (f *chaosFleet) addBroker() {
	f.mu.Lock()
	f.pruneDeadLocked()
	if len(f.brokers) >= chaosMaxBrokers {
		f.mu.Unlock()
		return
	}
	id := fmt.Sprintf("broker-%d", f.nextB)
	f.nextB++
	// Per-broker relay transport with its OWN fault gate so the network scenario can
	// sever THIS broker's peer-relay reachability (Route/RelayComplete) without
	// touching its worker-facing listener.
	relayGate := &faultGate{}
	b := &managedBroker{
		id: id, pool: f.pool, mc: f.mc,
		shards:    runtime.NewShardSet(nil), // set by retile
		relayGate: relayGate,
		relayHTTP: gatedRelayClient(relayGate),
		lister:    store.New(f.pool),
		reactors:  f.reactors, // run a ReactorDispatcher alongside the work dispatcher
	}
	f.brokers = append(f.brokers, b)
	f.mu.Unlock()

	b.start(f.t, f.ctx)
	f.retile()
}

// pruneDeadLocked drops stopped/killed brokers and workers from the tracking slices
// so the size caps reflect only LIVE processes. Caller holds f.mu.
func (f *chaosFleet) pruneDeadLocked() {
	bkept := f.brokers[:0]
	for _, b := range f.brokers {
		if b.isRunning() {
			bkept = append(bkept, b)
		}
	}
	f.brokers = bkept
	wkept := f.workers[:0]
	for _, w := range f.workers {
		if w.isRunning() {
			wkept = append(wkept, w)
		}
	}
	f.workers = wkept
}

// retile recomputes each LIVE broker's contiguous shard tile so the union covers
// the whole keyspace (mirrors what the Resharder does on membership change).
func (f *chaosFleet) retile() {
	f.mu.Lock()
	defer f.mu.Unlock()
	live := make([]*managedBroker, 0, len(f.brokers))
	for _, b := range f.brokers {
		if b.isRunning() {
			live = append(live, b)
		}
	}
	if len(live) == 0 {
		return
	}
	per := chaosTotalShards / len(live)
	for i, b := range live {
		lo := i * per
		hi := lo + per - 1
		if i == len(live)-1 {
			hi = chaosTotalShards - 1
		}
		b.shards.Store(shardRangeSlice(int16(lo), int16(hi)))
	}
}

func (f *chaosFleet) addWorker() {
	f.mu.Lock()
	f.pruneDeadLocked()
	if len(f.workers) >= chaosMaxWorkers {
		f.mu.Unlock()
		return // at cap — churn within bounds
	}
	id := fmt.Sprintf("worker-%d", f.nextW)
	f.nextW++
	// dial a random LIVE broker.
	target := f.randomLiveBrokerAddr()
	w := &managedWorker{
		id: id, providers: f.wruntimes, dialAddr: target,
		gate: &faultGate{}, drainGrace: 10 * time.Second,
	}
	f.workers = append(f.workers, w)
	f.mu.Unlock()
	if target == "" {
		return // no live broker to dial yet; a later chaos tick will add one
	}
	w.start(f.t, f.ctx)
}

func (f *chaosFleet) randomLiveBrokerAddr() string {
	live := make([]string, 0, len(f.brokers))
	for _, b := range f.brokers {
		if b.isRunning() {
			b.mu.Lock()
			addr := b.addr
			b.mu.Unlock()
			if addr != "" {
				live = append(live, addr)
			}
		}
	}
	if len(live) == 0 {
		return ""
	}
	return live[f.rng.Intn(len(live))]
}

// runChaos fires one random fault every tick until ctx ends.
func (f *chaosFleet) runChaos(ctx context.Context, tick time.Duration) {
	tk := time.NewTicker(tick)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			f.oneChaosAction()
		}
	}
}

// oneChaosAction picks and applies a single fault from the actions ACTIVE for the
// selected scenario (seeded-random), keeping the fleet within safe bounds (never
// zero brokers or zero workers — that's operator error, not a fault the system is
// expected to survive). See chaos_faults.go for the action registry.
func (f *chaosFleet) oneChaosAction() {
	acts := f.activeActions()
	if len(acts) == 0 {
		return
	}
	f.mu.Lock()
	f.actions++
	n := f.actions
	pick := acts[f.rng.Intn(len(acts))]
	f.mu.Unlock()
	// Narrate the fault: what it injects + what the system must do to survive it, so
	// a -v run reads as a fault story and a failure is anchored to the last action.
	f.t.Logf("chaos[%d] %s: %s\n           expect: %s", n, pick.name, pick.desc, pick.expect)
	pick.apply(f)
}

// startNewBrokerForGap keeps broker count from decaying: after a stop/kill, spin a
// replacement so the fleet churns rather than shrinks to nothing.
func (f *chaosFleet) startNewBrokerForGap() { f.addBroker() }

func (f *chaosFleet) replaceWorker(old *managedWorker) {
	// prune the dead worker and add a fresh one (models a pod replacement).
	f.mu.Lock()
	kept := f.workers[:0]
	for _, w := range f.workers {
		if w != old {
			kept = append(kept, w)
		}
	}
	f.workers = kept
	f.mu.Unlock()
	f.addWorker()
}

func (f *chaosFleet) countLive() (brokers, workers int) {
	for _, b := range f.brokers {
		if b.isRunning() {
			brokers++
		}
	}
	for _, w := range f.workers {
		if w.isRunning() {
			workers++
		}
	}
	return
}

func (f *chaosFleet) pickLiveBroker() *managedBroker {
	f.mu.Lock()
	defer f.mu.Unlock()
	live := make([]*managedBroker, 0, len(f.brokers))
	for _, b := range f.brokers {
		if b.isRunning() {
			live = append(live, b)
		}
	}
	if len(live) == 0 {
		return nil
	}
	return live[f.rng.Intn(len(live))]
}

func (f *chaosFleet) pickLiveWorker() *managedWorker {
	f.mu.Lock()
	defer f.mu.Unlock()
	live := make([]*managedWorker, 0, len(f.workers))
	for _, w := range f.workers {
		if w.isRunning() {
			live = append(live, w)
		}
	}
	if len(live) == 0 {
		return nil
	}
	return live[f.rng.Intn(len(live))]
}

func (f *chaosFleet) actionCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.actions
}

func (f *chaosFleet) shutdown() {
	f.mu.Lock()
	bs := append([]*managedBroker(nil), f.brokers...)
	ws := append([]*managedWorker(nil), f.workers...)
	f.mu.Unlock()
	for _, w := range ws {
		w.stop()
	}
	for _, b := range bs {
		b.stop(context.Background())
	}
}

// ── invariant checkers ─────────────────────────────────────────────────────────

type violations struct {
	mu   sync.Mutex
	msgs []string
}

func newViolations() *violations { return &violations{} }
func (v *violations) add(msg string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.msgs = append(v.msgs, msg)
}
func (v *violations) assert(t *testing.T) {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, m := range v.msgs {
		t.Errorf("INVARIANT VIOLATION: %s", m)
	}
}

// checkInvariants polls the DB while chaos runs, enforcing:
//   - no double-apply: synced_gen never exceeds generation (a second APPLY would).
//   - progress liveness: ready-count doesn't stall for too long (unless already done).
//   - no permanent stranding: a claimed row's heartbeat_at doesn't stay ancient
//     while the row is unprogressed past a generous bound.
func (f *chaosFleet) checkInvariants(ctx context.Context, pool *pgxpool.Pool, expected int, count func(context.Context, *pgxpool.Pool) int, viol *violations) {
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

		// (a) no double-apply — synced_gen must never exceed generation.
		var over int
		_ = pool.QueryRow(ctx,
			`SELECT count(*) FROM resources WHERE synced_gen > generation`).Scan(&over)
		if over > 0 {
			viol.add(fmt.Sprintf("%d resources have synced_gen > generation (double-apply / stale over-advance)", over))
		}

		// (b) progress liveness — ready count should climb; tolerate plateaus while a
		// big transient-failure wave retries or a fault window is active, but not
		// indefinitely. A DELIBERATE fault that halts the whole system (DB outage, brief
		// zero-broker window) is not a livelock: reset the stall clock while one is
		// active so we only flag a genuine unable-to-recover plateau. The threshold is
		// generous (stallLimit) to absorb the longest fault + recovery window.
		ready := count(ctx, pool)
		if ready >= expected {
			return // done — stop checking; the completion watcher will close doneCh
		}
		if ready > lastReady {
			lastReady = ready
			stalledSince = time.Now()
		} else if f.faultActive() {
			stalledSince = time.Now() // don't count a deliberate outage window as a stall
		} else if time.Since(stalledSince) > stallLimit {
			viol.add(fmt.Sprintf("progress stalled at %d/%d ready for >%v with NO active fault (possible livelock/stranding)", ready, expected, stallLimit))
			stalledSince = time.Now() // report once per window, keep watching
		}

		// (c) no permanent stranding — a row claimed but not heartbeated recently
		// while still unprogressed, well past the reaper window, means recovery failed.
		// StaleAfter is 15s here; flag anything unrecovered past 90s. SKIP while a DB
		// outage / control restart is active: the reaper CAN'T reap without the DB (or
		// while it's down), so a stale heartbeat_at then is expected, not a bug.
		if !f.faultActive() {
			var stranded int
			_ = pool.QueryRow(ctx, `
				SELECT count(*) FROM work_queue
				WHERE worker_id IS NOT NULL
				  AND heartbeat_at < now() - interval '90 seconds'`).Scan(&stranded)
			if stranded > 0 {
				viol.add(fmt.Sprintf("%d work_queue rows stranded (claimed, no heartbeat >90s, reaper didn't recover)", stranded))
			}
		}
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

// readyCounter returns a function that counts fully-reconciled resources for the
// workload. noop is a ROOT-ONLY Worker kind (no children/owner), measured by
// synced_gen catching generation; classicbom composes OWNED children measured by
// is_ready. Using the wrong predicate (e.g. is-ready-owned for noop) counts 0
// forever and the test never completes.
func readyCounter(workload string) func(context.Context, *pgxpool.Pool) int {
	if workload == "noop" {
		return func(ctx context.Context, pool *pgxpool.Pool) int {
			var n int
			_ = pool.QueryRow(ctx,
				`SELECT count(*) FROM resources WHERE kind = 'noop' AND synced_gen >= generation AND generation > 0`).Scan(&n)
			return n
		}
	}
	return func(ctx context.Context, pool *pgxpool.Pool) int {
		var n int
		_ = pool.QueryRow(ctx,
			`SELECT count(*) FROM resources WHERE owner_id IS NOT NULL AND is_ready`).Scan(&n)
		return n
	}
}

func waitReadyOrCtx(ctx context.Context, pool *pgxpool.Pool, expected int, count func(context.Context, *pgxpool.Pool) int) bool {
	tk := time.NewTicker(1 * time.Second)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-tk.C:
			if count(ctx, pool) >= expected {
				return true
			}
		}
	}
}

// waitLifecycleDrainedOrCtx returns true once lifecycle_outbox is empty after having
// held rows — every reactor delivery emitted by the active binding was claimed,
// dispatched to statussink over Connect, and fenced-acked, surviving whatever broker
// churn hit its dispatcher. It requires a non-zero peak first so it can't report "done"
// before any delivery was emitted (the workload's roots roll up slightly after their
// children, so bindings may not have fired yet when reconcile-ready first trips).
func waitLifecycleDrainedOrCtx(ctx context.Context, pool *pgxpool.Pool) bool {
	tk := time.NewTicker(1 * time.Second)
	defer tk.Stop()
	sawRows := false
	for {
		select {
		case <-ctx.Done():
			return false
		case <-tk.C:
			var n int
			if pool.QueryRow(ctx, `SELECT count(*) FROM lifecycle_outbox`).Scan(&n) != nil {
				continue // a DB blip mid-scenario — retry next tick
			}
			if n > 0 {
				sawRows = true
			} else if sawRows {
				return true
			}
		}
	}
}

func submitChaosWorkload(t *testing.T, ctx context.Context, te *testEngine, workload string, noopCount int) int {
	t.Helper()
	if workload == "noop" {
		for i := 0; i < noopCount; i++ {
			_, err := te.CreateRoot(ctx, model.Kind(noop.Kind), fmt.Sprintf("chaos-noop-%d", i), json.RawMessage(`{}`), nil)
			require.NoError(t, err)
		}
		return noopCount
	}
	bom := loadRealBOM(t)
	require.NotNil(t, bom.DeploymentInstance)
	totalTeams, tgwFDs := 0, 0
	for _, fd := range bom.DeploymentInstance.FunctionalDomains {
		if fd == nil {
			continue
		}
		totalTeams += len(fd.ServiceTeams)
		if fd.AWSTransitGateway != nil && fd.AWSTransitGateway.Enabled && !fd.AccountsOnly {
			tgwFDs++
		}
	}
	expected := tgwFDs*2 + totalTeams*3
	specJSON, err := json.Marshal(bom)
	require.NoError(t, err)
	_, err = te.CreateRoot(ctx, model.Kind(classicbom.Kind), bom.DeploymentInstance.Name+"-chaos", specJSON, nil)
	require.NoError(t, err)
	return expected
}

func assertNoDoubleApply(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	var over int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM resources WHERE synced_gen > generation`).Scan(&over))
	require.Zero(t, over, "resources with synced_gen > generation — the fence let a stale/duplicate apply through")
}

func dumpChaosState(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	var total, ready, failed, claimed int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM resources WHERE owner_id IS NOT NULL`).Scan(&total)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM resources WHERE owner_id IS NOT NULL AND is_ready`).Scan(&ready)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM resources WHERE failure_terminal AND failure_gen = generation`).Scan(&failed)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM work_queue WHERE worker_id IS NOT NULL`).Scan(&claimed)
	t.Logf("STATE: children=%d ready=%d terminal_failed=%d work_queue_claimed=%d", total, ready, failed, claimed)
	// Lifecycle (reactor) depth too — so a reactor-mode timeout distinguishes "reconcile
	// stalled" from "reconcile done, reactor spine still draining": with -chaos-reactors the
	// completion predicate also requires lifecycle_outbox to empty, so a run can time out
	// with reconcile at 100% while deliveries are mid-flight (fault retries + churn). A
	// PLATEAU here across two dumps would signal a real reactor strand, not just slowness.
	var lcRemaining, lcClaimed int
	_ = pool.QueryRow(ctx,
		`SELECT count(*), count(*) FILTER (WHERE broker_id IS NOT NULL) FROM lifecycle_outbox`).
		Scan(&lcRemaining, &lcClaimed)
	t.Logf("STATE: lifecycle_outbox remaining=%d (claimed=%d)", lcRemaining, lcClaimed)
}
