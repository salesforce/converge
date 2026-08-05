package test

// Two-axis status (Synced vs Ready/health) + drift-detection coverage.
// These exercise the K8s/Crossplane conditions model: a resource can be
// Synced=True (spec reconciled) yet Ready=False (observed unhealthy),
// and the periodic resync sweeper can flip Ready with no spec change.

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/engine"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// TestResyncDriftDemotesReady is the headline two-axis test: a resource
// that reconciled its spec successfully (Synced) and was Ready can later
// be flipped to Ready=False by the periodic resync probe — WITH NO SPEC
// OR GENERATION CHANGE — and recover the same way. This is the exact
// scenario a single "did we reconcile the declared generation" flag
// could not express.
//
// Flow:
//  1. Create a leaf with a health-probe worker (ResyncInterval short).
//  2. It reconciles → is_ready=true, health_ok=true, Ready=True stored.
//  3. Flip the probe to unhealthy. No spec change. The resync sweeper
//     re-pends it at the current generation; the worker re-observes and
//     reports Ready=False → health_ok=false → is_ready=false, while
//     synced_gen still equals generation (Synced stays True).
//  4. Flip the probe healthy again → resync re-promotes is_ready=true.
func TestResyncDriftDemotesReady(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	// 1s resync interval so drift is observable within the test deadline.
	probe := newHealthProbeWorker(model.Kind("widget"), 1*time.Second)
	reg := newTReg()
	reg.AddKind(probe, probe.Manifest())

	// Seed the widget's MANIFEST (after migrating so the table exists) so the
	// control plane — which reads kind_config, derived from kind_manifest by the
	// DB trigger, not a registry — picks up the widget's 1s ResyncInterval. Then
	// build it with a fast resync sweep.
	migrateForSeed(t, ctx, pool)
	require.NoError(t, reg.seed(ctx, pool))
	cp, err := engine.NewControlPlane(ctx, &engine.ControlPlaneConfig{
		Pool:                pool,
		ResyncSweepInterval: 500 * time.Millisecond,
		RetryAfter:          1 * time.Second,
	})
	require.NoError(t, err)
	require.NoError(t, cp.Start(ctx))
	wn := startInProcWorker(t, ctx, pool, reg, nil, 0, nil)
	defer func() { _ = wn.Stop(ctx); _ = cp.Stop(ctx) }()

	applied, err := store.New(pool).ApplySpec(ctx, model.Kind("widget"), "w1",
		mustJSON(map[string]string{"name": "w1", "k": "v"}), nil)
	require.NoError(t, err)
	rootID := applied.ID

	q := dbq.New(pool)

	// 2: reaches ready, healthy.
	require.Eventually(t, func() bool {
		r, err := q.GetResourceInfo(ctx, rootID)
		return err == nil && r.IsReady && r.HealthOk
	}, 15*time.Second, 100*time.Millisecond, "should reconcile to ready+healthy")

	genBefore := func() int64 {
		r, err := q.GetResourceInfo(ctx, rootID)
		require.NoError(t, err)
		return r.Generation
	}()

	// 3: flip unhealthy. No spec change → generation must NOT move.
	probe.SetHealthy("w1", false)

	require.Eventually(t, func() bool {
		r, err := q.GetResourceInfo(ctx, rootID)
		if err != nil {
			return false
		}
		// Demoted: not ready, health false, but STILL synced (gen unchanged).
		return !r.IsReady && !r.HealthOk && r.SyncedGen >= r.Generation
	}, 15*time.Second, 200*time.Millisecond,
		"resync probe should demote Ready=False with no spec change")

	rAfter, err := q.GetResourceInfo(ctx, rootID)
	require.NoError(t, err)
	require.Equal(t, genBefore, rAfter.Generation,
		"generation must NOT change on a health-only demotion")

	// The Ready=False condition is queryable as current truth.
	conds, err := q.ListResourceConditions(ctx, rootID)
	require.NoError(t, err)
	var foundReadyFalse bool
	for _, c := range conds {
		if c.Type == "Ready" && c.Status == "False" {
			foundReadyFalse = true
		}
	}
	require.True(t, foundReadyFalse, "a Ready=False condition row should exist after demotion")

	// 4: heal → resync re-promotes.
	probe.SetHealthy("w1", true)
	require.Eventually(t, func() bool {
		r, err := q.GetResourceInfo(ctx, rootID)
		return err == nil && r.IsReady && r.HealthOk
	}, 15*time.Second, 200*time.Millisecond, "resync should re-promote once healthy again")
}

// TestReconcileFailureSurfacesSyncedFalse validates that a failing
// reconcile records a Synced=False condition (queryable current truth).
func TestReconcileFailureSurfacesSyncedFalse(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	composer := newControllableComposer()
	worker := newFlakyWorker(model.Kind(account.Kind))
	worker.SetFailsBefore("doomed", 1000) // never succeeds within the test
	reg := newTReg()
	reg.AddKind(composer, composer.Manifest())
	reg.AddKind(worker, worker.Manifest())
	eng := startEngineWithRegistry(t, ctx, pool, reg)
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "fail-root",
		buildSpec(controllableChildSpec{Kind: string(account.Kind), Name: "doomed"}), nil)
	require.NoError(t, err)

	q := dbq.New(pool)

	// The doomed child should acquire a Synced=False condition.
	var childID pgtype.UUID
	require.Eventually(t, func() bool {
		children, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: pgtype.UUID{Bytes: rootID, Valid: true}, Limit: 1000000})
		if err != nil || len(children) != 1 {
			return false
		}
		childID = pgtype.UUID{Bytes: children[0].ID, Valid: true}
		conds, err := q.ListResourceConditions(ctx, children[0].ID)
		if err != nil {
			return false
		}
		for _, c := range conds {
			if c.Type == "Synced" && c.Status == "False" {
				return true
			}
		}
		return false
	}, 20*time.Second, 200*time.Millisecond, "failing reconcile should record Synced=False")
	_ = childID
}
