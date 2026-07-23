package test

// 1M-resource stress test using the real BOM (testfixtures/resource-classicbom.json)
// extended to ~1M total resources, on the split topology:
//
//   - 3 control-plane pods (composer + outbox drainer + work-reaper),
//     each owning a disjoint subset of outbox + reaper shards.
//   - 50 worker-node pods per kind × 4 kinds (account/vpc/tgw/route)
//     = 200 workers, each owning a disjoint subset of work_queue shards.
//   - One supersized Postgres (max_connections=2500, tmpfs data dir,
//     synchronous_commit=off).
//
// What we're stressing:
//   - schedule_eligible scaling at 1M rows
//   - cascade-trigger fan-out under high concurrency
//   - outbox drainer keeping up with 200-worker throughput
//   - reaper bootstrap-pickup not stalling at 1M scale
//   - lock-manager fast-path NOT spilling under disjoint shard ownership
//
// Gated behind -stress so a vanilla `go test ./test/` skips it.
//
// Run via:
//   go test -count=1 -timeout 90m ./test/ -run TestRealBOM1M -v -args -stress
//
// Pass -stress-ui=true to also expose a UI (HTTP API) on :18080,
// :18081, :18082 (one per control pod) so you can browse Resources /
// Summary while it runs.
//
// Pass -stress-reactors=true to ALSO drive the reactor (lifecycle saga) spine at
// scale: it binds the statussink reactor to the account kind's 'synced' transition
// (~1/3 of resources), runs a ReactorDispatcher on every control pod, and asserts
// lifecycle_outbox drains from its fan-out peak to zero — exercising the reactor
// path's OWN table/claim/ack/reap under real load, the twin of the reconcile path.
// OFF by default so the canonical reconcile-throughput number is measured with the
// reactor spine dormant (its documented zero-cost 1M-healthy state).

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/examples/demos/classic/fault"
	"github.com/salesforce/converge/examples/demos/classic/networking"
	"github.com/salesforce/converge/examples/demos/classic/statussink"
	"github.com/salesforce/converge/internal/api"
	"github.com/salesforce/converge/internal/engine"
	"github.com/salesforce/converge/internal/metrics"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/shardutil"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/test/internal/demoruntime"
	"github.com/salesforce/converge/test/internal/inproc"
)

var (
	stressEnabled = flag.Bool("stress", false, "Run the long real-BOM 1M stress test (default: skip)")
	stressUI      = flag.Bool("stress-ui", false, "Expose UI HTTP on :18080-:18082 during the stress run")
	// stressReactors turns the reactor (lifecycle saga) spine ON for the run: it
	// registers the statussink reactor, binds it to the `account` kind's 'synced'
	// transition (the most numerous kind → ~1/3 of all resources cross it), and runs
	// a ReactorDispatcher on every control pod. This drives the reactor path at scale
	// — its own table (lifecycle_outbox), claim, ack, and reap under real fan-out —
	// the twin of the reconcile path the base run exercises. OFF by default so the
	// canonical reconcile throughput number is measured with the reactor spine
	// dormant (zero lifecycle_outbox rows, its documented 1M-healthy cost).
	stressReactors = flag.Bool("stress-reactors", false, "Drive the reactor spine at scale (bind statussink to account/synced) during the stress run")
)

// TestRealBOM1M extends testfixtures/resource-classicbom.json to ~1M resources
// and runs the split topology.
func TestRealBOM1M(t *testing.T) {
	if !*stressEnabled {
		t.Skip("stress test disabled; run with -args -stress")
	}
	runRealBOMStress(t, 1_000_000)
}

// TestRealBOM250k is the fast inner-loop variant: identical topology
// and code path, scaled to ~250k resources so a benchmark iteration
// finishes in roughly a quarter of the 1M wall-clock. Use it to tune
// batch sizes / loop cadences / SQL gates, then confirm the win on
// the full 1M run.
//
//	go test -count=1 -timeout 30m ./test/ -run TestRealBOM250k -v -args -stress
func TestRealBOM250k(t *testing.T) {
	if !*stressEnabled {
		t.Skip("stress test disabled; run with -args -stress")
	}
	runRealBOMStress(t, 250_000)
}

// runRealBOMStress drives the full split-topology stress workload at
// the requested resource count. Shared by TestRealBOM1M and
// TestRealBOM250k.
func runRealBOMStress(t *testing.T, targetResources int) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Minute)
	defer cancel()

	// Production-style topology: a primary postgres and a streaming
	// replica wired via pg_basebackup + standby.signal. Engine traffic
	// (composer, drainer, reaper, workers) uses the primary; the API
	// server's UI read pool points at the replica, mirroring how the
	// app is deployed in production.
	dbs := setupReplicatedPostgres(t, ctx)
	connStr := dbs.PrimaryDSN
	replicaConnStr := dbs.ReplicaDSN

	const (
		numControlPods    = 3
		numWorkerPods     = 50
		workerMaxParallel = 10
		controlPoolMax    = 50
		// Pool size is driven by CONCURRENT DB connections, NOT by
		// workerMaxParallel × #kinds. The store acquires a pooled conn per
		// query and releases it immediately (pool.Query/Exec); a worker task
		// does NOT hold a conn for its duration. A leaf worker task is just
		// RunWork + one brief AppendOutbox at completion, so the simultaneous
		// conn users on a pod are: the 1 LISTEN listener (held for life), the
		// heartbeat loop, the dispatcher's claims, and the burst of
		// AppendOutbox writes from tasks finishing in the same instant. 24
		// comfortably absorbs that completion burst (even with 40 tasks in
		// flight, only a fraction write at once) with headroom — it does not
		// scale with maxParallel. The budget check keeps the fleet total under
		// max_connections.
		workerPoolMax = 24
	)

	// The four worker kinds. classicbom is composed by the ClassicBOM
	// provider and runs through whichever node owns the classicbom
	// kind — we attach it to control pod 0 below since there's only
	// one classicbom resource and it doesn't need fan-out.
	kinds := []model.Kind{model.Kind(account.Kind), model.Kind(networking.KindTGW), model.Kind(networking.KindVPC), model.Kind(networking.KindRoute)}

	// Guard the connection budget so a worker/pool change can't blow
	// past max_connections=2500 silently.
	connBudget := numWorkerPods*workerPoolMax + numControlPods*controlPoolMax
	require.LessOrEqual(t, connBudget, 2400,
		"connection budget %d exceeds safe ceiling; lower workers/pool", connBudget)
	t.Logf("[budget] %d worker pods × %d pool + %d control × %d = %d conns (max_connections=2500)",
		numWorkerPods, workerPoolMax, numControlPods, controlPoolMax, connBudget)

	type stoppable interface {
		Stop(context.Context) error
	}
	var allPods []stoppable
	t.Cleanup(func() {
		stopCtx, c := context.WithTimeout(context.Background(), 60*time.Second)
		defer c()
		for _, p := range allPods {
			_ = p.Stop(stopCtx)
		}
	})

	makePool := func(maxConns int32) *pgxpool.Pool {
		cfg, err := pgxpool.ParseConfig(connStr)
		require.NoError(t, err)
		cfg.MaxConns = maxConns
		cfg.MinConns = 1
		// Same connection-health knobs as the production binary:
		// prevents broken-but-cached connections from hanging
		// goroutines after a postgres OOM/restart event.
		cfg.HealthCheckPeriod = 30 * time.Second
		cfg.MaxConnLifetime = 5 * time.Minute
		cfg.MaxConnIdleTime = 1 * time.Minute
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		require.NoError(t, err)
		t.Cleanup(func() { pool.Close() })
		return pool
	}

	// Collect every worker pool so we can dump aggregate pgxpool stats
	// (empty-acquire count etc.) at the end — the client-side signal for
	// hypothesis (A) pool starvation. See stress_instrument_test.go.
	var workerPools []*pgxpool.Pool

	// ── Control pods: composer + outbox drainer + reaper ──
	// Each pod owns a range-partitioned slice of outbox + reaper shards
	// so disjoint pods touch disjoint b-tree leaf pages of
	// idx_work_outbox_drain and idx_resources_shard_lagging.
	type controlPod struct {
		pool *pgxpool.Pool
		eng  *engine.ControlPlane
		reg  *tReg // declared schemas for this pod's API server
	}
	var controlPods []controlPod

	// Reactor spine (opt-in via -stress-reactors). statussink is the reference reactor
	// kind; each pod runs its OWN in-process statussink whose "upload" writes to a local
	// in-memory store. The scale assertion is drain-from-peak-to-zero on lifecycle_outbox
	// (the ack — a DELETE — fires only on a clean handler completion), so it needs no
	// cross-pod object readback. endpoint/prefix ride statussink's default providerconfig.
	const sinkEndpoint = "mem://stress-sink"
	const sinkPrefix = "rolled-up/"

	// OTel metrics for the stress run. This test hand-builds pods (it does NOT run
	// cmd/converge's run(), so the production metrics wiring is inactive) — so we
	// build one Provider here, gated on OTEL_METRICS_ENABLED, wire the control
	// sweeper counter into each pod's config, and (below, with -stress-ui) mount
	// /metrics on the :1808x HTTP servers alongside the API. The DB-pool + process
	// gauges register in the -stress-ui block. metricsProvider is a no-op when the
	// env is unset, so nothing changes for a normal run.
	metricsProvider, err := metrics.New(metrics.Config{
		Enabled:        os.Getenv("OTEL_METRICS_ENABLED") == "true",
		ServiceName:    "converge",
		ServiceVersion: "stress",
		Role:           "all",
	})
	require.NoError(t, err)
	controlSweep := metricsProvider.RegisterControl()

	// ClassicBOM Composer needs to live on a control pod. ControlPlane
	// itself only spawns reaper + drainer goroutines — it does NOT
	// drain work_queue. We need an in-process worker per control pod
	// configured for kind=classicbom so the Composer dispatcher runs there.
	//
	// Each control pod gets an in-process worker bound to its outbox/reaper
	// shard slice — the classicbom resource hashes to one specific shard;
	// only the pod that owns that shard claims it. The others' dispatchers
	// poll empty work_queue rows in their shard ranges and idle.
	for i := 0; i < numControlPods; i++ {
		pool := makePool(controlPoolMax)
		shards := shardutil.ShardsForPod(i, numControlPods, shardutil.NumShards)

		// Composer pods declare every kind's manifest (not just classicbom) so the
		// API server can serve schemas for vpc/account/tgw/route. The companion
		// in-process worker below scopes its dispatcher to classicbom only, so this pod
		// doesn't actually run account/networking work.
		reg := newTReg()
		reg.Add(demoruntime.ClassicBOM(0))
		reg.Add(demoruntime.Account(0, 0, fault.Injector{}))
		reg.Add(networking.AllRuntimes(0, fault.Injector{})...)

		// With -stress-reactors, register the statussink reactor kind on every control
		// pod so (a) its manifest seeds → the claim can resolve its reaction, and (b) its
		// handler is available to this pod's in-process reactor executor. All pods share
		// ONE object store (same endpoint) so the end-of-run assertion sees every upload.
		if *stressReactors {
			sinkKR, _ := demoruntime.StatusSink(sinkEndpoint,
				fmt.Sprintf(`{"endpoint":%q,"prefix":%q}`, sinkEndpoint, sinkPrefix))
			reg.Add(sinkKR)
		}

		// Migrate + seed the manifests (deriving kind_config via the DB trigger)
		// BEFORE the control plane reads kind_config at construction. Idempotent
		// across pods sharing this primary (content-hash upsert); startInProcWorker
		// re-seeds when it starts the companion worker.
		migrateForSeed(t, ctx, pool)
		require.NoError(t, reg.seed(ctx, pool))
		cp, err := engine.NewControlPlane(ctx, &engine.ControlPlaneConfig{
			Pool:   pool,
			Shards: runtime.NewShardSet(shards), // explicit static pin (no resharder in this test)
			Swept:  controlSweep.Swept,          // OTel sweeper-rows counter (nil no-op when metrics off)
		})
		require.NoError(t, err)
		require.NoError(t, cp.Start(ctx))
		allPods = append(allPods, cp)
		controlPods = append(controlPods, controlPod{pool: pool, eng: cp, reg: reg})

		// Companion in-process worker for the classicbom Composer dispatcher.
		// Same pool, scoped to this pod's shards. classicbom is a single root,
		// so cap parallel low (it shares the control pool with the sweepers +
		// this dispatcher's heartbeat + work_ready listener; the uncapped
		// default of 100/pair could try to claim more than the pool holds).
		fwn := startInProcWorker(t, ctx, pool, reg, []model.Kind{model.Kind(classicbom.Kind)}, 4, runtime.NewShardSet(shards))
		allPods = append(allPods, fwn)

		// With -stress-reactors, run a ReactorDispatcher on this pod over its shard slice
		// — the SAME ReactorDispatcher → StageDispatcher seam production uses, here over an
		// in-process executor of this pod's registry (statussink's handler runs in-process).
		// It drains lifecycle_outbox rows in this pod's shards: claim → STAGE_REACT →
		// statussink upload → fenced ack. ReadyToDispatch is nil (in-process always has the
		// handler). The pods partition lifecycle_outbox by shard exactly as they do
		// work_queue, so the reactor fan-out scales with the fleet.
		if *stressReactors {
			rx := runtime.NewReactorDispatcher(store.New(pool), runtime.NewPgxListener(pool), inproc.New(reg.providerList()), "")
			rx.Shards = runtime.NewShardSet(shards)
			rxCtx, rxCancel := context.WithCancel(ctx)
			go func() { _ = rx.Run(rxCtx) }()
			t.Cleanup(rxCancel)
		}

		t.Logf("[control] pod %d started with shards %v..%v (classicbom dispatcher attached)",
			i, firstOrZero(shards), lastOrZero(shards))
	}

	// With -stress-reactors, bind statussink to the account kind's 'synced' transition.
	// account is the most numerous kind (one per team → ~1/3 of all resources), so ~1/3 of
	// the resources crossing 'synced' each emit a lifecycle_outbox delivery — genuine
	// reactor load at scale, not the single-root delivery a classicbom binding would give.
	// One binding for the whole fleet (the rule table is global); done after the pods exist
	// so their reactor dispatchers pick it up on their first claim (bindings are read live).
	if *stressReactors {
		require.NoError(t, store.New(controlPods[0].pool).UpsertReactorBinding(ctx, store.ReactorBinding{
			Name:       "account-synced-to-statussink",
			WatchKind:  model.Kind(account.Kind),
			Transition: string(model.TransitionSynced),
			Reactor:    model.Kind(statussink.Kind),
			Enabled:    true,
		}))
		t.Logf("[reactor] statussink bound to %s/synced across %d control pods (reactor spine ACTIVE)", account.Kind, numControlPods)
	}

	// ── Worker pods: all-kinds-per-pod, range-partitioned across shards ──
	// numWorkerPods pods, each running ONE dispatcher that claims ALL four
	// kinds (one runtime pair per kind). This is the production-shape
	// topology the unified dispatcher was built for: a homogeneous worker
	// pool where any pod drains any kind, instead of one pod pinned per kind.
	// Each pod owns a disjoint contiguous shard slice (ShardsForPod), so the
	// pods partition the work_queue with no overlap.
	for j := 0; j < numWorkerPods; j++ {
		pool := makePool(workerPoolMax)
		workerPools = append(workerPools, pool)
		shards := shardutil.ShardsForPod(j, numWorkerPods, shardutil.NumShards)

		// Register every worker kind's provider so this pod's dispatcher
		// spawns one pair per kind and can claim all of them.
		reg := newTReg()
		reg.Add(demoruntime.Account(0, 0, fault.Injector{}))
		reg.Add(networking.VPCRuntime(0, 0, fault.Injector{}))
		reg.Add(networking.TGWRuntime(0, 0, fault.Injector{}))
		reg.Add(networking.RouteRuntime(0, 0, fault.Injector{}))

		wn := startInProcWorker(t, ctx, pool, reg, kinds, workerMaxParallel, runtime.NewShardSet(shards))
		allPods = append(allPods, wn)
	}
	t.Logf("[worker] %d all-kinds worker pods started (max_parallel=%d/kind × %d kinds, sharded across %d shards)",
		numWorkerPods, workerMaxParallel, len(kinds), shardutil.NumShards)

	// Optional: HTTP servers on :18080-:18082, one per control pod.
	// Useful for poking at /api/objects + the embedded UI while the
	// stress run is in progress.
	//
	// UI reads go to the streaming replica via a dedicated pool so
	// /api/* GETs never compete with composer/drainer/reaper for
	// primary connections. Writes (POST/PUT/DELETE) still flow to
	// the primary through cp.pool, just like in production.
	httpServers := []*http.Server{}
	if *stressUI {
		makeReadPool := func() *pgxpool.Pool {
			cfg, err := pgxpool.ParseConfig(replicaConnStr)
			require.NoError(t, err)
			cfg.MaxConns = controlPoolMax
			cfg.MinConns = 1
			cfg.HealthCheckPeriod = 30 * time.Second
			cfg.MaxConnLifetime = 5 * time.Minute
			cfg.MaxConnIdleTime = 1 * time.Minute
			pool, err := pgxpool.NewWithConfig(ctx, cfg)
			require.NoError(t, err)
			t.Cleanup(func() { pool.Close() })
			return pool
		}
		// Metrics (when OTEL_METRICS_ENABLED): register the process + DB-pool gauges
		// once against the first control pod's pool, then mount /metrics on every
		// :1808x server next to the API. (The API mux serves /api + /healthz; we wrap
		// it so the SAME port also answers /metrics — the test's single plaintext port
		// stands in for prod's separate :8081 health/metrics listener.)
		if metricsProvider.Enabled() && len(controlPods) > 0 {
			metricsProvider.RegisterProcess("stress", "all")
			p0 := controlPods[0].pool
			metricsProvider.RegisterDBPool(func() metrics.PoolStat { return p0.Stat() })
			t.Logf("[metrics] OTel /metrics enabled on :18080-:%d", 18080+len(controlPods)-1)
		}
		const httpBase = 18080
		for i, cp := range controlPods {
			srv := api.NewServer(api.DepsFromPools(cp.pool, makeReadPool()))
			srv.SetDeclaredSchemas(declaredManifests(cp.reg))
			// Combine the API handler with /metrics on one mux so the stress-UI port
			// answers both /api|/healthz (API) and /metrics (OTel Prometheus).
			mux := http.NewServeMux()
			mux.Handle("/metrics", metricsProvider.Handler())
			mux.Handle("/", srv.Handler())
			hs := &http.Server{Addr: fmt.Sprintf(":%d", httpBase+i), Handler: mux}
			go func(hs *http.Server, idx int) {
				if err := hs.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					slog.Error("http server", "pod", idx, "addr", hs.Addr, "error", err)
				}
			}(hs, i)
			httpServers = append(httpServers, hs)
			t.Logf("[control] pod %d HTTP at %s (/metrics: %v)", i, hs.Addr, metricsProvider.Enabled())
		}
		t.Cleanup(func() {
			for _, hs := range httpServers {
				_ = hs.Shutdown(context.Background())
			}
			_ = metricsProvider.Shutdown(context.Background())
		})
	}

	// ── Build the BOM: real data shape extended to ~1M ──
	bom := loadRealBOM(t)
	require.NotNil(t, bom.DeploymentInstance)

	numFDs := len(bom.DeploymentInstance.FunctionalDomains)
	// Per-FD resources from the ClassicBOM Composer:
	//   - 1 fd-tgw account + 1 TGW per FD that has a TGW (all 58 do here)
	//   - 1 account + 1 vpc + 1 route per team
	// total = 2*tgwFDs + 3*teams. Solve for teams.
	targetTeams := (targetResources - 2*numFDs) / 3
	extendBOMEvenly(&bom, targetTeams)

	totalTeams := 0
	tgwFDs := 0
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

	totalPods := numControlPods + numWorkerPods
	t.Logf("=== %d pods (%d control + %d worker), %d FDs, %d teams (%d resources), spec=%d MB ===",
		totalPods, numControlPods, numWorkerPods,
		numFDs, totalTeams, expected, len(specJSON)/(1024*1024))

	// Reset pg_stat_statements right before the test phase so the
	// snapshot we take at the end attributes exactly the work this
	// run did (excludes startup migrations, schema introspection, etc.).
	// CREATE EXTENSION is idempotent.
	_, err = controlPods[0].pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS pg_stat_statements`)
	require.NoError(t, err)
	_, err = controlPods[0].pool.Exec(ctx, `SELECT pg_stat_statements_reset()`)
	require.NoError(t, err)

	// Start the server-side wait-event sampler for the duration of the
	// run. Samples pg_stat_activity every 2s; the histogram tells us
	// whether the run is CPU-bound or stalled on Lock/LWLock/BufferPin
	// contention — the decisive signal for hypothesis (B). Uses control
	// pod 0's pool (50 conns, not the worker pools) so sampling never
	// competes with workers for connections.
	sampleCtx, sampleCancel := context.WithCancel(ctx)
	defer sampleCancel()
	sampler := newWaitEventSampler()
	sampler.start(sampleCtx, controlPods[0].pool, 2*time.Second)

	// With -stress-reactors, reactor deliveries fire CONCURRENTLY with reconcile — an
	// account emits its 'synced' lifecycle_outbox row as soon as IT rolls up, long before
	// the whole BOM is ready — and the reactor dispatchers drain them continuously. So the
	// lifecycle_outbox has already emptied by the time reconcile-readiness trips; sampling
	// it only AFTER would see a vacuous zero. Instead, sample it in the BACKGROUND across
	// the whole run to capture the fan-out PEAK (proof the spine fired at scale) and the
	// final drain (proof no delivery stranded). Started before submit, stopped after.
	var reactorSampler *lifecycleSampler
	if *stressReactors {
		reactorSampler = newLifecycleSampler()
		reactorSampler.start(sampleCtx, controlPods[0].pool, 2*time.Second)
	}

	// Submit through control pod 0 (in-process; not the HTTP API).
	start := time.Now()
	applied, err := store.New(controlPods[0].pool).ApplySpec(
		ctx,
		model.Kind(classicbom.Kind),
		fmt.Sprintf("%s-%dk-demo", bom.DeploymentInstance.Name, expected/1000),
		specJSON,
		nil,
	)
	require.NoError(t, err)
	rootID := applied.ID
	t.Logf("root id: %s — submitted at t=%s", rootID, start.Format("15:04:05.000"))

	waitForTotalReady(t, ctx, controlPods[0].pool, expected, 60*time.Minute)
	elapsed := time.Since(start)

	// With -stress-reactors, confirm the reactor spine both FIRED at scale and fully
	// drained. Reactor deliveries run alongside reconcile (the account/synced binding
	// emits one per account crossing synced, ~1/3 of resources) and the dispatchers drain
	// them continuously — so the queue never accumulates to the full emitted total at
	// once; its instantaneous PEAK DEPTH is emit-rate-minus-drain-rate over the run, a
	// large backlog that proves the spine handled real fan-out without wedging (a wedged
	// spine would pile up far higher or never drain). A few deliveries may still be
	// in-flight the instant reconcile-readiness trips, so give lifecycle_outbox a short
	// settle to reach zero, then assert:
	//   - PEAK depth was a substantial backlog → the fan-out genuinely happened at scale
	//     (not a vacuously-empty queue). The floor is intentionally well under 1/3 because
	//     a healthy fast-draining spine keeps the standing depth a fraction of the total.
	//   - final count is 0 → every delivery was claimed → dispatched → fenced-acked, with
	//     no stranded claim (the ack DELETE fires only on a clean handler completion). This
	//     is the load-bearing correctness signal.
	if *stressReactors {
		waitForLifecycleSettled(t, ctx, controlPods[0].pool, 5*time.Minute)
		peak, final := reactorSampler.result()
		require.GreaterOrEqual(t, peak, expected/50,
			"reactor spine barely fired: peak lifecycle_outbox depth %d is below the fan-out floor (expected a substantial standing backlog for a ~%d-delivery run)", peak, expected/3)
		require.Zero(t, final, "lifecycle_outbox must fully drain — %d deliveries stranded (claimed-but-unacked)", final)
		t.Logf("[reactor] spine fired at scale and drained: peak standing depth=%d, final=0 stranded", peak)
	}
	sampleCancel() // stop sampling before we run the heavy report queries

	// Dump top-30 queries by total exec time. This is the
	// "where did the 3 minutes go" attribution.
	dumpStatStatements(t, ctx, controlPods[0].pool)

	// Contention attribution: server-side wait-event histogram +
	// client-side pool stats. Label each report with the topology so
	// two runs (e.g. 50×10 vs 5×100) are trivially diffable in the log.
	cfgLabel := fmt.Sprintf("%dpods-allkinds x par%d", numWorkerPods, workerMaxParallel)
	sampler.report(t, cfgLabel)
	logPoolStats(t, cfgLabel, workerPools)

	t.Logf("=== DONE [%s]: %d resources in %v (%.0f res/sec) ===",
		cfgLabel, expected, elapsed.Round(time.Millisecond), float64(expected)/elapsed.Seconds())
}

// waitForTotalReady polls until at least `expected` non-root resources
// are in phase=ready, or the deadline expires. Logs a progress line
// every 5s so a long run isn't silent.
func waitForTotalReady(t *testing.T, ctx context.Context, pool *pgxpool.Pool, expected int, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	pollInterval := 1 * time.Second
	logInterval := 5 * time.Second
	lastLog := time.Now()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("context cancelled while waiting for %d ready", expected)
		default:
		}
		var got int64
		err := pool.QueryRow(ctx,
			`SELECT count(*) FROM resources WHERE owner_id IS NOT NULL AND is_ready`,
		).Scan(&got)
		if err != nil {
			t.Logf("waitForTotalReady: count query failed: %v", err)
		} else if int(got) >= expected {
			return
		}

		if time.Now().After(deadline) {
			rows, qErr := pool.Query(ctx, `
				SELECT
				    is_ready::text,
				    deletion_requested_at IS NOT NULL AS deleting,
				    count(*)
				FROM resources WHERE owner_id IS NOT NULL
				GROUP BY 1, 2 ORDER BY 1, 2`,
			)
			if qErr == nil {
				out := map[string]int64{}
				for rows.Next() {
					var ready, deleting string
					var c int64
					if scanErr := rows.Scan(&ready, &deleting, &c); scanErr == nil {
						out[ready+"/del="+deleting] = c
					}
				}
				rows.Close()
				t.Logf("readiness histogram at timeout: %+v", out)
			}
			t.Fatalf("timed out waiting for %d ready (got %d)", expected, got)
		}
		if time.Since(lastLog) >= logInterval {
			pct := 100 * float64(got) / float64(expected)
			t.Logf("progress: %d / %d ready (%.1f%%)", got, expected, pct)
			lastLog = time.Now()
		}
		time.Sleep(pollInterval)
	}
}

// lifecycleSampler watches lifecycle_outbox in the background across a stress run,
// recording the high-water row count (the reactor fan-out PEAK) and the latest count.
// The reactor spine drains CONCURRENTLY with reconcile, so the peak occurs mid-run and
// is gone by the time reconcile-readiness trips — sampling it live is the only way to
// prove reactors fired at scale. Reads are lock-free-ish via a mutex around two ints;
// the sampler is a single goroutine, the reader waits until it's stopped.
type lifecycleSampler struct {
	mu   sync.Mutex
	peak int
	last int
	done chan struct{}
}

func newLifecycleSampler() *lifecycleSampler { return &lifecycleSampler{done: make(chan struct{})} }

// start samples lifecycle_outbox every interval until ctx ends, tracking peak + last.
func (s *lifecycleSampler) start(ctx context.Context, pool *pgxpool.Pool, interval time.Duration) {
	go func() {
		defer close(s.done)
		tk := time.NewTicker(interval)
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tk.C:
				var n int
				if pool.QueryRow(ctx, `SELECT count(*) FROM lifecycle_outbox`).Scan(&n) != nil {
					continue // a transient scan error mid-run — try the next tick
				}
				s.mu.Lock()
				s.last = n
				if n > s.peak {
					s.peak = n
				}
				s.mu.Unlock()
			}
		}
	}()
}

// result returns the observed peak and last counts. Call after the sampler's ctx ended.
func (s *lifecycleSampler) result() (peak, last int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak, s.last
}

// waitForLifecycleSettled waits until lifecycle_outbox reaches zero — the last few
// reactor deliveries in flight when reconcile-readiness tripped get their moment to be
// claimed → dispatched → fenced-acked. A non-zero plateau here means a delivery is
// STRANDED (claimed but never acked — the failure this coverage exists to catch); on
// timeout it dumps the stuck lifecycle state so the strand is visible.
func waitForLifecycleSettled(t *testing.T, ctx context.Context, pool *pgxpool.Pool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var remaining, claimed int64
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*), count(*) FILTER (WHERE broker_id IS NOT NULL) FROM lifecycle_outbox`).
			Scan(&remaining, &claimed))
		if remaining == 0 {
			return
		}
		if time.Now().After(deadline) {
			dumpLifecycleState(t, ctx, pool)
			t.Fatalf("lifecycle_outbox did not settle to zero: %d remaining (%d still claimed) — reactor delivery stranded", remaining, claimed)
		}
		time.Sleep(2 * time.Second)
	}
}

// dumpStatStatements prints the top-30 queries by total exec time
// from pg_stat_statements, plus a bare "totals" line. Logged via
// t.Log so they appear in -v output.
//
// Columns:
//
//	total_ms     = sum of execution time across all calls
//	calls        = number of times this query was executed
//	mean_ms      = total / calls
//	rows         = total rows touched (RETURNING / SELECT result count)
//	io_ms        = sum of block-read + block-write IO time (track_io_timing)
//	query        = first 200 chars of the query text
//
// Reading guide for diagnosing throughput:
//   - High total + high calls + low mean → death-by-1000-cuts; look at
//     whether you can batch.
//   - High total + low calls + high mean → one big slow query; look at
//     plan / index.
//   - High io_ms relative to total → IO-bound; check buffer hit rates,
//     row sizes, TOAST.
func dumpStatStatements(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()

	const topN = 30

	rows, err := pool.Query(ctx, `
		SELECT
		    round(total_exec_time::numeric, 1)                AS total_ms,
		    calls,
		    round((total_exec_time / NULLIF(calls,0))::numeric, 3) AS mean_ms,
		    rows,
		    round((COALESCE(blk_read_time,0) + COALESCE(blk_write_time,0))::numeric, 1) AS io_ms,
		    left(regexp_replace(query, E'\\s+', ' ', 'g'), 200) AS query
		FROM pg_stat_statements
		WHERE query NOT LIKE '%pg_stat_statements%'
		  AND query NOT LIKE '%pg_stat_activity%'
		ORDER BY total_exec_time DESC
		LIMIT $1
	`, topN)
	if err != nil {
		t.Logf("pg_stat_statements query failed: %v", err)
		return
	}
	defer rows.Close()

	t.Logf("=== pg_stat_statements top %d by total_exec_time ===", topN)
	t.Logf("%-12s %-9s %-10s %-10s %-9s  query", "total_ms", "calls", "mean_ms", "rows", "io_ms")
	for rows.Next() {
		var totalMs, meanMs, ioMs float64
		var calls, nrows int64
		var query string
		if err := rows.Scan(&totalMs, &calls, &meanMs, &nrows, &ioMs, &query); err != nil {
			t.Logf("scan: %v", err)
			continue
		}
		t.Logf("%-12.1f %-9d %-10.3f %-10d %-9.1f  %s", totalMs, calls, meanMs, nrows, ioMs, query)
	}

	// Aggregate footer.
	var totalMs float64
	var totalCalls int64
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(sum(total_exec_time),0), COALESCE(sum(calls),0)
		FROM pg_stat_statements
		WHERE query NOT LIKE '%pg_stat_statements%'
		  AND query NOT LIKE '%pg_stat_activity%'
	`).Scan(&totalMs, &totalCalls); err == nil {
		t.Logf("=== TOTALS: %.1f ms exec across %d calls ===", totalMs, totalCalls)
	}
}

func firstOrZero(s []int16) int16 {
	if len(s) == 0 {
		return 0
	}
	return s[0]
}

func lastOrZero(s []int16) int16 {
	if len(s) == 0 {
		return 0
	}
	return s[len(s)-1]
}

// silence unused-import warnings if a knob gets toggled off above
var _ = runtime.NumShards
