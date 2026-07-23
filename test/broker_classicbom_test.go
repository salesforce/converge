package test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/examples/demos/classic/fault"
	"github.com/salesforce/converge/examples/demos/classic/networking"
	"github.com/salesforce/converge/internal/broker"
	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/engine"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/sdk-go/converge"
	"github.com/salesforce/converge/sdk-go/workerpb/workerpbconnect"
	"github.com/salesforce/converge/test/internal/demoruntime"
)

// TestBrokerClassicBOMAllStagesRemote proves that EVERY provider stage — Composer,
// Worker, AND StatusRollup — runs in a DUMB worker, not in-process. A
// classicbom root (Composer + StatusRollup, no Worker) composes into account / TGW /
// VPC / route children (Worker kinds); reaching root-ready requires the compose
// stage, every child's work stage, and the rollup stage to all execute. With NO
// in-process dispatch duty for any of these kinds, the only way the tree settles
// is worker→broker→DB for all three stage types.
func TestBrokerClassicBOMAllStagesRemote(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	// Registry: classicbom (Composer+Rollup) + its child kinds (Workers). Pure
	// Runtimes (delay 0), no Setup — hermetic.
	reg := newTReg()
	reg.Add(demoruntime.ClassicBOM(0))
	reg.Add(demoruntime.Account(0, 0, fault.Injector{}))
	reg.Add(networking.AllRuntimes(0, fault.Injector{})...)

	// Control duty only (drainer applies outbox → synced_gen; reaper backstops).
	// NO dispatch duty: nothing runs any stage in-process. Seed the kinds'
	// manifests first (the claim reads its pairs from kind_config, derived from
	// kind_manifest).
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

	// The broker: owns all shards, claims the reconcile pair for every kind.
	const brokerID = "broker-classicbom"
	cl := broker.New(pool, mc, brokerID, 32)
	for _, k := range reg.kinds() {
		cl.Dispatcher().AddPair(k, 1, store.TaskReconcile)
	}
	mux := http.NewServeMux()
	mux.Handle(cl.Handler())
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	claimCtx, claimCancel := context.WithCancel(ctx)
	t.Cleanup(claimCancel)
	go func() { _ = cl.Dispatcher().Run(claimCtx) }()

	// A dumb remote worker running EVERY kind's stages over Connect — the same
	// runtimes the registry seeded.
	workerProviders := []converge.Provider{demoruntime.ClassicBOM(0), demoruntime.Account(0, 0, fault.Injector{})}
	workerProviders = append(workerProviders, networking.AllRuntimes(0, fault.Injector{})...)
	client := workerpbconnect.NewWorkerServiceClient(srv.Client(), srv.URL)
	workerCtx, workerCancel := context.WithCancel(ctx)
	t.Cleanup(workerCancel)
	go func() {
		_ = converge.RunWorker(workerCtx, client, workerProviders, converge.RunOptions{MaxInflight: 16})
	}()

	// Create a classicbom root: 1 FD with 3 teams → 11 children (account/TGW/VPC/route).
	bom := smallBOM("project-remote", map[string][]string{
		"alpha-fd": {"team-alpha", "team-beta", "team-gamma"},
	})
	specBytes, err := json.Marshal(bom)
	require.NoError(t, err)
	rootID, err := (&testEngine{Pool: pool}).CreateRoot(ctx, model.Kind(classicbom.Kind), "project-remote", specBytes, nil)
	require.NoError(t, err)

	const expectedChildren = 11
	q := dbq.New(pool)

	// Wait for the whole tree to settle: root ready + all children ready. This
	// can ONLY happen if compose (remote), every child's work (remote), and
	// rollup (remote) all ran.
	deadline := time.Now().Add(90 * time.Second)
	settled := false
	for time.Now().Before(deadline) {
		root, err := q.GetResourceInfo(ctx, rootID)
		if err == nil && root.IsReady {
			children, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: pgtype.UUID{Bytes: rootID, Valid: true}, Limit: 1000000})
			if err == nil && len(children) == expectedChildren {
				allReady := true
				for _, c := range children {
					if !c.IsReady {
						allReady = false
						break
					}
				}
				if allReady {
					settled = true
					break
				}
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	if !settled {
		dumpResourceState(t, ctx, pool, rootID)
		t.Fatal("classicbom tree never fully settled via the all-remote broker tier")
	}

	// Compose AND rollup events prove those two stages ran (remotely).
	evs, err := store.New(pool).ListEvents(ctx, rootID, time.Time{}, 0, 100)
	require.NoError(t, err)
	var rollups, composes int
	for _, e := range evs {
		switch e.Type {
		case "rollup-succeeded":
			rollups++
		case "compose-succeeded":
			composes++
		}
	}
	require.GreaterOrEqual(t, composes, 1, "compose stage must have run (remotely)")
	require.GreaterOrEqual(t, rollups, 1, "rollup stage must have run (remotely)")

	// The lease was held by the broker for the compose/rollup writes.
	root, err := q.GetResourceInfo(ctx, rootID)
	require.NoError(t, err)
	require.True(t, root.IsReady)
	require.GreaterOrEqual(t, root.SyncedGen, int64(1))

	t.Logf("SUCCESS: classicbom root + %d children settled via ALL-REMOTE broker tier (compose=%d rollup=%d, all over Connect)",
		expectedChildren, composes, rollups)
}
