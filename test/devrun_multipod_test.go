package test

// TestDevRunMultiPod is the "just dev-inproc-multipod" entrypoint: it spins
// up the same primary+replica + multi-control-pod + multi-worker
// topology that TestRealBOM1M uses, but at a dev-friendly worker
// count and with no required BOM. Useful for exercising pod
// sharding, replica-routed reads, and the cascade trigger paths
// against a real running app rather than a unit-style test.
//
// Topology mirrors TestRealBOM1M:
//   - 1 primary postgres + 1 streaming replica (async; sized for dev)
//   - 3 control pods (each runs reaper + drainer + a classicbom dispatcher +
//     an HTTP API on its own port)
//   - 10 workers per kind × 4 kinds = 40 worker pods total
//   - Each pod owns a disjoint slice of the 256 shards
//
// Three HTTP ports because one-per-pod helps validate "any pod can
// serve any read" — the same shape stress-test users debug with.
//
// Gated behind -dev-multipod so a vanilla `go test ./test/` skips
// it. Run via `just dev-inproc-multipod`.
//
// Flags:
//   -dev-multipod          enable this test (default: skip)
//   -dev-multipod-bom      path to a ClassicBOM JSON to auto-submit at boot
//   -dev-multipod-fake-delay  simulated worker latency per resource
//
// On exit (Ctrl+C), both containers are torn down via t.Cleanup.

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/examples/demos/classic/fault"
	"github.com/salesforce/converge/examples/demos/classic/networking"
	"github.com/salesforce/converge/internal/api"
	"github.com/salesforce/converge/internal/engine"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/shardutil"
	"github.com/salesforce/converge/test/internal/demoruntime"
)

var (
	devMultiPodEnabled   = flag.Bool("dev-multipod", false, "Run the multi-pod dev Converge (used by `just dev-inproc-multipod`)")
	devMultiPodBOM       = flag.String("dev-multipod-bom", "", "Path to a ClassicBOM JSON file to auto-submit at boot")
	devMultiPodFakeDelay = flag.Duration("dev-multipod-fake-delay", 0*time.Millisecond, "Simulated worker latency")
)

func TestDevRunMultiPod(t *testing.T) {
	if !*devMultiPodEnabled {
		t.Skip("dev multi-pod runner disabled; run via `just dev-inproc-multipod` (sets -dev-multipod)")
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	// SIGINT/SIGTERM cancels everything. testing.T.Cleanup teardown
	// happens after this returns, taking both containers with it.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Primary + streaming replica. Same helper TestRealBOM1M uses.
	dbs := setupReplicatedPostgres(t, ctx)
	primaryDSN := dbs.PrimaryDSN
	replicaDSN := dbs.ReplicaDSN

	const (
		numControlPods    = 3
		numWorkerPods     = 50
		workerMaxParallel = 0
		controlPoolMax    = 12
		// Pool size is driven by CONCURRENT DB connections, NOT by
		// workerMaxParallel. The store acquires a pooled conn per query and
		// releases it immediately (pool.Query/Exec) — a worker task does NOT
		// hold a conn for its (possibly slow) duration. A leaf worker task is
		// just the work React (no DB) + one brief AppendOutbox at the end, so even
		// with 50×5 tasks in flight the only simultaneous conn users are: the
		// 1 LISTEN listener (held for the pod's life), the heartbeat loop, the
		// dispatcher's claims, and the burst of AppendOutbox writes from tasks
		// finishing at the same instant. A small fixed pool absorbs that; it
		// does not scale with maxParallel.
		workerPoolMax = 16
		readPoolMax   = 2
	)

	kinds := []model.Kind{model.Kind(account.Kind), model.Kind(networking.KindTGW), model.Kind(networking.KindVPC), model.Kind(networking.KindRoute)}

	type stoppable interface {
		Stop(context.Context) error
	}
	var allPods []stoppable

	makePool := func(dsn string, maxConns int32) *pgxpool.Pool {
		cfg, err := pgxpool.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.MaxConns = maxConns
		cfg.MinConns = 1
		// Health-check + recycle knobs: prevents connections from
		// surviving across a Postgres OOM kill or restart in a broken
		// state. Without these, a postmaster crash leaves stale
		// connections in the pool that get reused and hang the
		// goroutines that own them.
		cfg.HealthCheckPeriod = 30 * time.Second
		cfg.MaxConnLifetime = 5 * time.Minute
		cfg.MaxConnIdleTime = 1 * time.Minute
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		require.NoError(t, err)
		t.Cleanup(func() { pool.Close() })
		return pool
	}

	// ── Control pods ──
	type controlPod struct {
		pool *pgxpool.Pool
		eng  *engine.ControlPlane
		reg  *tReg // declared schemas for this pod's API server
	}
	var controlPods []controlPod
	for i := 0; i < numControlPods; i++ {
		pool := makePool(primaryDSN, controlPoolMax)
		shards := shardutil.ShardsForPod(i, numControlPods, shardutil.NumShards)

		// Every control pod registers every kind so its registry can
		// validate value flows on compose. WorkerKinds below scopes the
		// companion in-process worker to classicbom only, so this pod doesn't run
		// account/networking work. Caps are declared on each kind's manifest
		// (MaxInflight; 0 = uncapped here), derived into kind_config by the
		// manifest seed and read directly by the claim; tune live via
		// POST /api/kinds/{kind}/config.
		reg := newTReg()
		reg.Add(demoruntime.ClassicBOM(0))
		reg.Add(demoruntime.Account(*devMultiPodFakeDelay, 0, fault.Injector{}))
		reg.Add(networking.VPCRuntime(*devMultiPodFakeDelay, 0, fault.Injector{}))
		reg.Add(networking.TGWRuntime(*devMultiPodFakeDelay, 0, fault.Injector{}))
		reg.Add(networking.RouteRuntime(*devMultiPodFakeDelay, 0, fault.Injector{}))

		// Migrate + seed the manifests (deriving kind_config) BEFORE the control
		// plane reads kind_config at construction. Idempotent across pods sharing
		// the primary. startInProcWorker re-seeds when it starts the worker.
		migrateForSeed(t, ctx, pool)
		require.NoError(t, reg.seed(ctx, pool))
		cp, err := engine.NewControlPlane(ctx, &engine.ControlPlaneConfig{
			Pool:   pool,
			Shards: runtime.NewShardSet(shards), // explicit static pin per simulated pod
		})
		require.NoError(t, err)
		require.NoError(t, cp.Start(ctx))
		allPods = append(allPods, cp)
		controlPods = append(controlPods, controlPod{pool: pool, eng: cp, reg: reg})

		// Companion in-process worker for the classicbom composer pair on this control
		// pod, scoped to its shards and sharing its pool. These are composite
		// ROOTS (a handful, not the 1M leaves), so cap parallel low: it shares the
		// small controlPoolMax pool with the ControlPlane sweepers + this
		// dispatcher's heartbeat + work_ready listener, and an uncapped default
		// (100/pair) could try to claim more than the pool holds. A few roots
		// never need more than a couple of slots.
		fwn := startInProcWorker(t, ctx, pool, reg, []model.Kind{model.Kind(classicbom.Kind)}, 4, runtime.NewShardSet(shards))
		allPods = append(allPods, fwn)

		slog.Info("control pod started",
			"pod", i,
			"shards_first", firstOrZero(shards),
			"shards_last", lastOrZero(shards),
		)
	}

	// ── Worker pods: all-kinds-per-pod, range-partitioned across shards ──
	// Each of numWorkerPods pods runs ONE dispatcher claiming every worker
	// kind (one runtime pair per kind), owning a disjoint contiguous shard
	// slice — the production-shape homogeneous worker pool the unified
	// dispatcher was built for, rather than a pod pinned per kind.
	for j := 0; j < numWorkerPods; j++ {
		pool := makePool(primaryDSN, workerPoolMax)
		shards := shardutil.ShardsForPod(j, numWorkerPods, shardutil.NumShards)

		reg := newTReg()
		reg.Add(demoruntime.Account(*devMultiPodFakeDelay, 0, fault.Injector{}))
		reg.Add(networking.VPCRuntime(*devMultiPodFakeDelay, 0, fault.Injector{}))
		reg.Add(networking.TGWRuntime(*devMultiPodFakeDelay, 0, fault.Injector{}))
		reg.Add(networking.RouteRuntime(*devMultiPodFakeDelay, 0, fault.Injector{}))

		wn := startInProcWorker(t, ctx, pool, reg, kinds, workerMaxParallel, runtime.NewShardSet(shards))
		allPods = append(allPods, wn)
	}
	slog.Info("worker pods started",
		"pods", numWorkerPods,
		"kinds_per_pod", len(kinds),
		"max_parallel_per_kind", workerMaxParallel,
	)

	// ── HTTP servers, one per control pod ──
	// Each pod's API server gets its own write pool (the pod's primary
	// pool) and a dedicated replica pool. UI GETs go to the replica;
	// writes + read-after-write hit the primary. Ports 18080..18082.
	const httpBase = 18080
	httpServers := make([]*http.Server, 0, numControlPods)
	for i, cp := range controlPods {
		readPool := makePool(replicaDSN, readPoolMax)
		srv := api.NewServer(api.DepsFromPools(cp.pool, readPool))
		srv.SetDeclaredSchemas(declaredManifests(cp.reg))
		hs := &http.Server{
			Addr:    fmt.Sprintf(":%d", httpBase+i),
			Handler: srv.Handler(),
		}
		go func(hs *http.Server, idx int) {
			slog.Info("api+ui listening", "pod", idx, "url", "http://localhost"+hs.Addr)
			if err := hs.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("http server error", "pod", idx, "err", err)
			}
		}(hs, i)
		httpServers = append(httpServers, hs)
	}

	// Optionally auto-submit a ClassicBOM via control pod 0. The classicbom
	// is a single resource that hashes to one shard, so submitting via
	// pod 0 is fine — whichever pod owns that shard will run the
	// composer.
	if *devMultiPodBOM != "" {
		go func() {
			time.Sleep(500 * time.Millisecond)
			if err := submitManifestFromFile(ctx, controlPods[0].pool, *devMultiPodBOM); err != nil {
				slog.Error("auto-submit failed", "path", *devMultiPodBOM, "err", err)
				return
			}
			slog.Info("auto-submitted manifest", "path", *devMultiPodBOM)
		}()
	}

	slog.Info("converge running — Ctrl+C to stop",
		"control_pods", numControlPods,
		"worker_pods", numWorkerPods,
		"kinds_per_pod", len(kinds),
		"primary_url", "http://localhost:18080",
	)
	<-ctx.Done()

	slog.Info("shutting down")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	for _, hs := range httpServers {
		_ = hs.Shutdown(shutdownCtx)
	}
	for _, p := range allPods {
		_ = p.Stop(shutdownCtx)
	}
}
