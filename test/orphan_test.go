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

// TestComposeShrinkRemovesChildren: when the Composer's emitted child
// set shrinks on re-reconcile, the dropped rows are hard-deleted (with
// ON DELETE CASCADE clearing any descendants), and re-adding a name
// inserts a fresh row that runs through Worker normally.
//
//  1. Initial spec: 4 children. All reach is_ready=true.
//  2. Shrunk spec: 2 children (drop 2). After re-reconcile:
//     - 2 surviving children stay ready.
//     - 2 dropped children are gone from `resources`.
//  3. Re-grow spec: re-add one of the dropped names. After
//     re-reconcile:
//     - The re-added name reappears as a brand-new row, gets worked,
//     becomes ready.
//     - The still-removed child stays gone.
//
// Validates ApplyComposeResult's stale-children deletion path.
func TestComposeShrinkRemovesChildren(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	composer := newControllableComposer()
	worker := newFlakyWorker(model.Kind(account.Kind))
	reg := newTReg()
	reg.AddKind(composer, composer.Manifest())
	reg.AddKind(worker, worker.Manifest())
	eng := startEngineWithRegistry(t, ctx, pool, reg)
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	// Step 1: 4 children.
	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "shrink-root",
		buildSpec(
			controllableChildSpec{Kind: string(account.Kind), Name: "a"},
			controllableChildSpec{Kind: string(account.Kind), Name: "b"},
			controllableChildSpec{Kind: string(account.Kind), Name: "c"},
			controllableChildSpec{Kind: string(account.Kind), Name: "d"},
		),
		nil,
	)
	require.NoError(t, err)
	q := dbq.New(pool)

	require.Eventually(t, func() bool {
		children, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: pgtype.UUID{Bytes: rootID, Valid: true}, Limit: 1000000})
		if err != nil || len(children) != 4 {
			return false
		}
		for _, c := range children {
			if !c.IsReady {
				return false
			}
		}
		return true
	}, 15*time.Second, 100*time.Millisecond, "all 4 children ready")

	// Step 2: shrink to {a, b}. Dropped names get hard-deleted. Re-applying
	// the same root (kind,name) is the upsert's update path.
	_, err = eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "shrink-root",
		buildSpec(
			controllableChildSpec{Kind: string(account.Kind), Name: "a"},
			controllableChildSpec{Kind: string(account.Kind), Name: "b"},
		),
		nil,
	)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		children, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: pgtype.UUID{Bytes: rootID, Valid: true}, Limit: 1000000})
		if err != nil || len(children) != 2 {
			return false
		}
		byName := map[string]dbq.GetChildrenByOwnerRow{}
		for _, c := range children {
			byName[c.Name] = c
		}
		return byName["a"].IsReady && byName["b"].IsReady
	}, 15*time.Second, 100*time.Millisecond, "c and d should be removed; a and b stay ready")

	// Worker should NOT have re-run on a/b (no spec change).
	require.Equal(t, 1, worker.Calls("a"), "a unchanged, only ran once")
	require.Equal(t, 1, worker.Calls("b"), "b unchanged, only ran once")
	t.Log("after shrink: c and d hard-deleted, a and b unchanged")

	// Step 3: re-add c. Spec is now {a, b, c}. Re-apply the same root.
	_, err = eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "shrink-root",
		buildSpec(
			controllableChildSpec{Kind: string(account.Kind), Name: "a"},
			controllableChildSpec{Kind: string(account.Kind), Name: "b"},
			controllableChildSpec{Kind: string(account.Kind), Name: "c"},
		),
		nil,
	)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		children, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: pgtype.UUID{Bytes: rootID, Valid: true}, Limit: 1000000})
		if err != nil || len(children) != 3 {
			return false
		}
		byName := map[string]dbq.GetChildrenByOwnerRow{}
		for _, c := range children {
			byName[c.Name] = c
		}
		return byName["a"].IsReady && byName["b"].IsReady && byName["c"].IsReady
	}, 15*time.Second, 100*time.Millisecond, "c should reappear as a fresh row and become ready")

	// c is a brand-new row, so its worker call count starts from 0 and
	// must hit 2 across the full lifecycle (initial create + re-add).
	require.GreaterOrEqual(t, worker.Calls("c"), 2,
		"c should run again on re-add")
	t.Log("after re-add: c reappears and reaches ready")
}
