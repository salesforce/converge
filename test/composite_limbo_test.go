package test

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
	"github.com/salesforce/converge/internal/model"
)

// TestCompositeFailedChildRollsUpToDegraded validates the composite-limbo
// fix: a composite root with a permanently-FAILING descendant must surface
// as phase='Degraded' (Ready=False/ChildrenNotReady), NOT sit forever in
// invisible 'Reconciling' limbo with zero conditions.
//
// Before the fix: the descendant gate (synced_gen < generation) blocked the
// root's rollup forever on the doomed child, so the root never ran its
// rollup, never emitted a condition, and was indistinguishable from a
// still-progressing root. After the fix: a FAILED child (failure_gen =
// generation) no longer blocks the gate, the rollup runs, counts the
// unready child, and demotes the root to Degraded.
func TestCompositeFailedChildRollsUpToDegraded(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	composer := newControllableComposer()
	composer.withRollup = true // composite-demotion behavior
	worker := newFlakyWorker(model.Kind(account.Kind))
	worker.SetFailsBefore("doomed", 1000) // never succeeds within the test
	reg := newTReg()
	reg.AddKind(composer, composer.Manifest())
	reg.AddKind(worker, worker.Manifest())
	eng := startEngineWithRegistry(t, ctx, pool, reg)
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "limbo-root",
		buildSpec(
			controllableChildSpec{Kind: string(account.Kind), Name: "healthy"},
			controllableChildSpec{Kind: string(account.Kind), Name: "doomed"},
		), nil)
	require.NoError(t, err)

	q := dbq.New(pool)
	ownerArg := pgtype.UUID{Bytes: rootID, Valid: true}

	// The doomed child must reach phase='Failed', and the ROOT — which
	// before the fix would stall at 'Reconciling' — must reach 'Degraded'.
	require.Eventually(t, func() bool {
		children, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: ownerArg, Limit: 1000000})
		if err != nil || len(children) != 2 {
			return false
		}
		var doomedFailed, healthyReady bool
		for _, c := range children {
			switch c.Name {
			case "doomed":
				doomedFailed = c.Phase == "Failed"
			case "healthy":
				healthyReady = c.Phase == "Ready"
			}
		}
		root, err := q.GetResourceInfo(ctx, rootID)
		if err != nil {
			return false
		}
		return doomedFailed && healthyReady && root.Phase == "Degraded"
	}, 30*time.Second, 250*time.Millisecond,
		"doomed child phase=Failed, healthy child phase=Ready, root phase=Degraded (not limbo)")

	// The root must carry a Ready=False/ChildrenNotReady condition (the
	// rollup ran and demoted it) — proving it left limbo with a real
	// surfaced reason rather than zero conditions.
	conds, err := q.ListResourceConditions(ctx, rootID)
	require.NoError(t, err)
	var sawChildrenNotReady bool
	for _, c := range conds {
		if c.Type == "Ready" && c.Status == "False" && c.Reason == "ChildrenNotReady" {
			sawChildrenNotReady = true
		}
	}
	require.True(t, sawChildrenNotReady,
		"root must carry Ready=False/ChildrenNotReady (rollup ran over the failed child)")

	t.Logf("composite root correctly rolled a failed child up to Degraded (no limbo)")
}
