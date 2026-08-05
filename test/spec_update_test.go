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

// TestSpecUpdateRereconciles validates the k8s-style "update spec →
// generation bump → re-pend → re-reconcile" loop:
//
//  1. Create a root resource. Composer runs once, producing N children.
//  2. All children reach is_ready=true.
//  3. Update the root's spec to declare a different child set.
//  4. The bump_generation trigger advances generation; the
//     cascade_on_spec_change trigger re-pends the root and schedules.
//  5. Composer re-runs; ApplyComposeResult diffs and hard-deletes the
//     dropped child while inserting the new one.
//  6. New child reaches ready; dropped child is gone.
//
// This is the load-bearing user-facing flow: kubectl-apply equivalent.
func TestSpecUpdateRereconciles(t *testing.T) {
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

	// Step 1: create root with two children.
	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "root-1",
		buildSpec(
			controllableChildSpec{Kind: string(account.Kind), Name: "alpha"},
			controllableChildSpec{Kind: string(account.Kind), Name: "beta"},
		),
		nil,
	)
	require.NoError(t, err)

	q := dbq.New(pool)

	// Step 2: wait for both children + root to reach ready.
	deadline := time.Now().Add(15 * time.Second)
	settled := false
	for time.Now().Before(deadline) {
		root, err := q.GetResourceInfo(ctx, rootID)
		if err == nil && root.IsReady {
			children, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: pgtype.UUID{Bytes: rootID, Valid: true}, Limit: 1000000})
			if err == nil && len(children) == 2 {
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
		time.Sleep(100 * time.Millisecond)
	}
	if !settled {
		dumpResourceState(t, ctx, pool, rootID)
		t.Fatalf("initial reconcile should succeed (composer calls=%d, worker calls alpha=%d beta=%d)",
			composer.Calls("root-1"), worker.Calls("alpha"), worker.Calls("beta"))
	}

	// Snapshot the initial root generation.
	rootBefore, err := q.GetResourceInfo(ctx, rootID)
	require.NoError(t, err)
	t.Logf("after initial compose: root.generation=%d, observed_generation=%d, composer calls=%d",
		rootBefore.Generation, rootBefore.SyncedGen, composer.Calls("root-1"))
	require.Equal(t, int64(1), rootBefore.Generation, "fresh root has generation 1")
	require.Equal(t, int64(1), rootBefore.SyncedGen, "Composer should have written observed_generation = 1")
	require.Equal(t, 1, composer.Calls("root-1"), "Composer should have run exactly once")

	// Step 3: update spec — drop "beta", add "gamma". Re-applying the
	// same root (kind,name) is the upsert's update path.
	_, err = eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "root-1",
		buildSpec(
			controllableChildSpec{Kind: string(account.Kind), Name: "alpha"},
			controllableChildSpec{Kind: string(account.Kind), Name: "gamma"},
		),
		nil,
	)
	require.NoError(t, err)

	// Step 4 + 5 + 6: wait for re-reconcile to settle. Final state:
	//   - root.generation = 2, observed_generation = 2, is_ready=true
	//   - alpha still ready (spec unchanged for it)
	//   - beta is gone (hard-deleted by ApplyComposeResult diff)
	//   - gamma reaches ready (newly composed)
	require.Eventually(t, func() bool {
		root, err := q.GetResourceInfo(ctx, rootID)
		if err != nil || !root.IsReady {
			return false
		}
		if root.Generation != 2 || root.SyncedGen != 2 {
			return false
		}
		children, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: pgtype.UUID{Bytes: rootID, Valid: true}, Limit: 1000000})
		if err != nil || len(children) != 2 {
			return false
		}
		byName := map[string]dbq.GetChildrenByOwnerRow{}
		for _, c := range children {
			byName[c.Name] = c
		}
		alpha, ok := byName["alpha"]
		if !ok || !alpha.IsReady {
			return false
		}
		if _, beta := byName["beta"]; beta {
			return false
		}
		gamma, ok := byName["gamma"]
		if !ok || !gamma.IsReady {
			return false
		}
		return true
	}, 15*time.Second, 100*time.Millisecond, "spec update should re-reconcile correctly")

	// Final assertions outside Eventually for nicer error messages.
	rootAfter, err := q.GetResourceInfo(ctx, rootID)
	require.NoError(t, err)
	require.Equal(t, int64(2), rootAfter.Generation)
	require.Equal(t, int64(2), rootAfter.SyncedGen)

	require.GreaterOrEqual(t, composer.Calls("root-1"), 2, "Composer should have run a second time after spec update")
	t.Logf("after spec update: root.generation=%d, observed_generation=%d, composer calls=%d",
		rootAfter.Generation, rootAfter.SyncedGen, composer.Calls("root-1"))

	// Worker should have run on alpha once (no spec change for alpha
	// so it stayed ready) and on gamma once. beta's worker may or may
	// not have run before its row was deleted; we don't assert on it.
	require.Equal(t, 1, worker.Calls("alpha"), "alpha worker should have run exactly once (spec unchanged)")
	require.Equal(t, 1, worker.Calls("gamma"), "gamma worker should have run exactly once")
}
