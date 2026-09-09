package test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/store"
)

// TestComposePrunesViaSoftDelete validates that when the composer stops
// producing a child, the prune honors the soft-delete / finalizer
// protocol instead of hard-deleting the row:
//   - a child of a kind WITH a Deleter (finalizer) → deletion_requested_at
//     set, finalizer seeded, a 'delete' task enqueued, row STILL present
//     (its Deleter must run before removal); and a recompose doesn't
//     re-request it.
//   - a child of a kind WITHOUT a Deleter → hard-deleted (row gone).
func TestComposePrunesViaSoftDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	eng := startAllRolesEngine(t, ctx, pool) // for schema migration only
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	repo := store.New(pool)

	// Registry: a kind WITH a teardown/finalizer, and a kind WITHOUT one.
	const delKind model.Kind = "deletable"
	const leafKind model.Kind = "leaf-nodeleter"
	dw := newDeletableWorker(delKind)
	leaf := newFlakyWorker(leafKind)
	reg := newTReg()
	reg.AddKind(dw, dw.Manifest())
	reg.AddKind(leaf, leaf.Manifest())

	// Seed the kinds' manifests (which carry delKind's FinalizerName) and build a
	// KindManifestCache to serve ApplyComposeResult its per-child prune policy
	// (OrphanGraceForKind/FinalizerForKind): delKind soft-deletes, leafKind hard-deletes.
	require.NoError(t, reg.seed(ctx, pool))
	pol := runtime.NewKindManifestCache(pool)
	require.NoError(t, pol.Load(ctx))
	pol.Start(ctx)
	defer pol.Stop()

	rootID, err := eng.CreateRoot(ctx, "soft-del-root-kind", "sd-root",
		json.RawMessage(`{}`), nil)
	require.NoError(t, err)

	child := func(k model.Kind, name string) model.ChildSpec {
		return model.ChildSpec{Kind: k, KindVersion: 1, Name: name, Spec: map[string]string{"name": name}}
	}
	const composeWorker = "compose-softdel-test-worker"
	apply := func(gen int64, children []model.ChildSpec) store.ComposeCounts {
		wq := fenceClaimRoot(t, ctx, pool, rootID, composeWorker, gen)
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		counts, err := repo.WithTx(tx).ApplyComposeResult(ctx, rootID, gen, wq, 1, 0, children, nil, nil, pol)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		return counts
	}

	childByName := func(name string) *dbq.GetChildrenByOwnerRow {
		kids, err := repo.GetChildrenByOwner(ctx, rootID, 1000000)
		require.NoError(t, err)
		for i := range kids {
			if kids[i].Name == name {
				return &kids[i]
			}
		}
		return nil
	}
	hasDeleteTask := func(id uuid.UUID) bool {
		var n int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM work_queue WHERE resource_id=$1 AND task_type='delete'`, id).Scan(&n))
		return n > 0
	}

	// ── gen 1: compose both children ──
	apply(1, []model.ChildSpec{
		child(delKind, "with-deleter"),
		child(leafKind, "no-deleter"),
	})
	dchild := childByName("with-deleter")
	lchild := childByName("no-deleter")
	require.NotNil(t, dchild)
	require.NotNil(t, lchild)
	require.False(t, dchild.DeletionRequestedAt.Valid, "not deleting yet")

	delID := dchild.ID

	// ── gen 2: drop BOTH children → prune ──
	c2 := apply(2, nil)
	require.Equal(t, 2, c2.Deleted, "both children counted as deleted")

	// The deletable child must SOFT-delete: row still present, marked
	// deleting, finalizer seeded, delete task enqueued — NOT hard-gone.
	soft := childByName("with-deleter")
	require.NotNil(t, soft, "child with a Deleter must NOT be hard-deleted")
	require.True(t, soft.DeletionRequestedAt.Valid, "must be in the soft-delete drain")
	require.True(t, hasDeleteTask(delID), "a delete task must be enqueued so the Deleter runs")

	// The no-Deleter child must be hard-deleted: row gone.
	require.Nil(t, childByName("no-deleter"), "child without a Deleter is hard-deleted")

	// ── gen 3: recompose again (still not producing it) → idempotent,
	// must NOT re-request deletion (no extra churn). ──
	c3 := apply(3, nil)
	require.Equal(t, 0, c3.Deleted, "already-draining child must not be re-deleted")
	require.NotNil(t, childByName("with-deleter"), "still draining (Deleter not run in this store-only test)")

	t.Logf("composer prune soft-deletes finalizer children, hard-deletes the rest, idempotently")
}
