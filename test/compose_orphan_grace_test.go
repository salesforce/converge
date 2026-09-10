package test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/store"
)

// TestComposeOrphanGrace validates orphan-grace pruning: when a composer stops
// producing a child of a kind with orphan_grace_secs > 0, the child is NOT torn
// down — it enters the Orphaned grace window (finite frozen_until stamped,
// row in the DAG, phase=Orphaned). A re-emit within the window re-adopts it
// (marks cleared, no teardown); only if the window elapses without a re-emit
// does the reaper's sweep escalate it into the real delete. This guards against
// a buggy composer that drops a child by mistake and re-emits it next cycle.
//
// Covered (subtests, sharing one Postgres container):
//   - leaf kind (grace>0): drop → Orphaned, not deleted; re-emit (changed AND
//     unchanged spec) re-adopts; reaper sweep after expiry → hard delete.
//   - finalizer kind (grace>0): sweep after expiry → soft-delete (the kind's
//     Deleter is enqueued), NOT hard delete.
//   - rollup ignores an orphaned child (it doesn't flip the composite unready).
//   - grace=0 backward-compat: drop → immediate prune (today's behavior).
func TestComposeOrphanGrace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	eng := startAllRolesEngine(t, ctx, pool) // for schema migration only
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	repo := store.New(pool)

	const (
		graceLeafKind model.Kind = "orphan-grace-leaf" // no finalizer, grace > 0
		graceFinKind  model.Kind = "orphan-grace-fin"  // finalizer, grace > 0
		immediateKind model.Kind = "orphan-immediate"  // no finalizer, grace == 0
		rootKind      model.Kind = "orphan-grace-root" // composer-less root holder
		graceWindow              = 3600 * time.Second  // long: never expires on its own; we age delete_after by hand
	)

	finWorker := newDeletableWorker(graceFinKind)
	// Per-kind prune policy (the store.ComposePolicy ApplyComposeResult consults to
	// decide soft-vs-hard prune + the grace stamp). The KindManifestCache satisfies
	// the interface, but orphan_grace_secs does NOT round-trip through kind_manifest
	// (no such column), so a DB-loaded cache always reports grace=0. We supply the
	// policy DIRECTLY so the composer stamps the real grace window:
	//   - graceLeafKind: grace > 0, no finalizer (hard-delete on expiry).
	//   - graceFinKind:  grace > 0 + finalizer (soft-delete on expiry).
	//   - immediateKind: grace == 0, no finalizer (immediate prune).
	pol := gracePolicy{
		grace: map[model.Kind]time.Duration{
			graceLeafKind: graceWindow,
			graceFinKind:  graceWindow,
		},
		finalizer: map[model.Kind]string{
			graceFinKind: finWorker.finalizerName(),
		},
	}

	// Seed kind_config so the reaper sweep can read each kind's finalizer_name +
	// grace in-DB (the composer stamps grace via the policy above; the sweep reads
	// the baked frozen_until + the finalizer column). startAllRolesEngine already
	// migrated; seed the three kinds' rows explicitly for a precise grace value.
	require.NoError(t, repo.SeedKindConfig(ctx, store.KindConfig{
		Kind: graceLeafKind, KindVersion: 1, OrphanGracePeriod: graceWindow,
	}))
	require.NoError(t, repo.SeedKindConfig(ctx, store.KindConfig{
		Kind: graceFinKind, KindVersion: 1, OrphanGracePeriod: graceWindow, FinalizerName: finWorker.finalizerName(),
	}))
	require.NoError(t, repo.SeedKindConfig(ctx, store.KindConfig{
		Kind: immediateKind, KindVersion: 1,
	}))

	rootID, err := eng.CreateRoot(ctx, rootKind, "orphan-grace-root-1", json.RawMessage(`{}`), nil)
	require.NoError(t, err)

	child := func(k model.Kind, name string) model.ChildSpec {
		return model.ChildSpec{Kind: k, KindVersion: 1, Name: name, Spec: map[string]string{"name": name}}
	}
	const composeWorker = "orphan-grace-test-worker"
	apply := func(gen int64, children []model.ChildSpec) store.ComposeCounts {
		t.Helper()
		wq := fenceClaimRoot(t, ctx, pool, rootID, composeWorker, gen)
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		counts, err := repo.WithTx(tx).ApplyComposeResult(ctx, rootID, gen, wq, 1, 0, children, nil, nil, pol)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		return counts
	}
	childByName := func(name string) *dbq.GetChildrenByOwnerRow {
		t.Helper()
		kids, err := repo.GetChildrenByOwner(ctx, rootID, 1000000)
		require.NoError(t, err)
		for i := range kids {
			if kids[i].Name == name {
				return &kids[i]
			}
		}
		return nil
	}
	// orphanState reads the (phase, orphaned, frozen set) of a child. The orphan
	// signal is a FINITE frozen_until (quarantine uses 'infinity'); this test
	// never quarantines, so a non-NULL finite frozen_until == orphaned. Kept as
	// two returns so the existing assertions read naturally.
	orphanState := func(id uuid.UUID) (phase string, orphaned bool, deleteAfter bool) {
		t.Helper()
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT phase, (frozen_until IS NOT NULL AND frozen_until <> 'infinity') FROM resources WHERE id=$1`, id).
			Scan(&phase, &deleteAfter))
		orphaned = deleteAfter
		return
	}
	hasDeleteTask := func(id uuid.UUID) bool {
		t.Helper()
		var n int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM work_queue WHERE resource_id=$1 AND task_type='delete'`, id).Scan(&n))
		return n > 0
	}
	// expireOrphan ages a row's frozen_until into the past so the next sweep picks it.
	expireOrphan := func(id uuid.UUID) {
		t.Helper()
		ct, err := pool.Exec(ctx,
			`UPDATE resources SET frozen_until = now() - interval '1 second' WHERE id=$1 AND frozen_until IS NOT NULL AND frozen_until <> 'infinity'`, id)
		require.NoError(t, err)
		require.Equal(t, int64(1), ct.RowsAffected(), "row must be orphaned to expire it")
	}

	// ── gen 1: compose all three children ──
	apply(1, []model.ChildSpec{
		child(graceLeafKind, "leaf-x"),
		child(graceFinKind, "fin-x"),
		child(immediateKind, "imm-x"),
	})
	leafID := childByName("leaf-x").ID
	finID := childByName("fin-x").ID
	immID := childByName("imm-x").ID
	for _, id := range []uuid.UUID{leafID, finID, immID} {
		_, orphaned, da := orphanState(id)
		require.False(t, orphaned, "fresh child not orphaned")
		require.False(t, da, "fresh child has no delete_after")
	}

	t.Run("drop_grace_kinds_orphans_not_deletes", func(t *testing.T) {
		// gen 2: drop ALL three children (produce nothing).
		c2 := apply(2, nil)

		// Grace kinds: stamped into the window, rows STILL present, phase=Orphaned.
		for _, id := range []uuid.UUID{leafID, finID} {
			require.NotNil(t, childByName(map[uuid.UUID]string{leafID: "leaf-x", finID: "fin-x"}[id]),
				"grace child must NOT be deleted")
			phase, orphaned, da := orphanState(id)
			require.True(t, orphaned, "grace child must be stamped orphaned_at")
			require.True(t, da, "grace child must have delete_after")
			require.Equal(t, "Orphaned", phase, "grace child phase must be Orphaned")
			require.False(t, hasDeleteTask(id), "no delete task yet — still in grace, Deleter must NOT have run")
		}
		require.GreaterOrEqual(t, c2.Orphaned, 2, "both grace children counted as orphaned")

		// Immediate kind (grace=0): hard-deleted now (today's behavior).
		require.Nil(t, childByName("imm-x"), "grace=0 leaf is pruned immediately")
	})

	t.Run("reemit_unchanged_readopts", func(t *testing.T) {
		// gen 3: re-emit leaf-x with the SAME spec (the buggy-drop-then-reappear
		// case). Must clear the grace marks WITHOUT a generation bump.
		var genBefore int64
		require.NoError(t, pool.QueryRow(ctx, `SELECT generation FROM resources WHERE id=$1`, leafID).Scan(&genBefore))

		c3 := apply(3, []model.ChildSpec{child(graceLeafKind, "leaf-x")})
		require.GreaterOrEqual(t, c3.Readopted, 1, "unchanged re-emit must re-adopt (clear marks)")

		phase, orphaned, da := orphanState(leafID)
		require.False(t, orphaned, "re-adopt must clear orphaned_at")
		require.False(t, da, "re-adopt must clear delete_after")
		require.NotEqual(t, "Orphaned", phase, "re-adopted child is back to a live phase")

		var genAfter int64
		require.NoError(t, pool.QueryRow(ctx, `SELECT generation FROM resources WHERE id=$1`, leafID).Scan(&genAfter))
		require.Equal(t, genBefore, genAfter, "unchanged re-adopt must NOT bump generation")
	})

	t.Run("reemit_changed_readopts", func(t *testing.T) {
		// Drop leaf-x again (re-orphan), then re-emit with a CHANGED spec — the
		// UpdateChildrenHotSQL path must also clear the marks.
		apply(4, nil)
		_, orphaned, _ := orphanState(leafID)
		require.True(t, orphaned, "re-dropped leaf is orphaned again")

		apply(5, []model.ChildSpec{{Kind: graceLeafKind, KindVersion: 1, Name: "leaf-x", Spec: map[string]string{"name": "leaf-x", "v": "2"}}})
		_, orphaned, da := orphanState(leafID)
		require.False(t, orphaned, "changed-spec re-emit must clear orphaned_at")
		require.False(t, da, "changed-spec re-emit must clear delete_after")
	})

	t.Run("reemit_labels_only_readopts", func(t *testing.T) {
		// Re-orphan leaf-x, then re-emit with the SAME spec but a CHANGED label.
		// This goes through the upsert path (labels differ → not skipped) but NOT
		// the spec UPDATE's mark-clear (spec is unchanged) — re-adoption must be
		// path-independent and still clear the marks. (Regression guard for the
		// labels-only gap the adversarial review surfaced.)
		applyL := func(gen int64, children []model.ChildSpec) {
			wq := fenceClaimRoot(t, ctx, pool, rootID, composeWorker, gen)
			tx, err := pool.Begin(ctx)
			require.NoError(t, err)
			_, err = repo.WithTx(tx).ApplyComposeResult(ctx, rootID, gen, wq, 1, 0, children, nil, nil, pol)
			require.NoError(t, err)
			require.NoError(t, tx.Commit(ctx))
		}
		applyL(6, nil) // drop → orphan
		_, orphaned, _ := orphanState(leafID)
		require.True(t, orphaned, "re-dropped leaf is orphaned again")

		// Same spec as gen 5 ("v":"2"), but add a label → labels-only change.
		applyL(7, []model.ChildSpec{{
			Kind:        graceLeafKind,
			KindVersion: 1, Name: "leaf-x",
			Spec:   map[string]string{"name": "leaf-x", "v": "2"},
			Labels: map[string]string{"team": "relabeled"},
		}})
		_, orphaned, da := orphanState(leafID)
		require.False(t, orphaned, "labels-only re-emit must clear orphaned_at (path-independent re-adoption)")
		require.False(t, da, "labels-only re-emit must clear delete_after")
	})

	t.Run("rollup_ignores_orphan", func(t *testing.T) {
		// With leaf-x orphaned, a rollup over the subtree must NOT count it as
		// unready (it has left the composition). Drive the test controller's
		// rollup directly with a descendant list including the orphaned child.
		apply(8, nil) // drop everything → leaf-x + fin-x orphaned
		_, orphaned, _ := orphanState(leafID)
		require.True(t, orphaned, "leaf-x orphaned for the rollup check")

		descs, err := repo.ListDescendants(ctx, rootID)
		require.NoError(t, err)
		// The orphaned children must surface with frozen_until set (finite, not
		// 'infinity') in the rollup view.
		var sawOrphan bool
		for _, d := range descs {
			if d.ID == leafID {
				require.True(t, d.FrozenUntil.Valid && d.FrozenUntil.InfinityModifier != pgtype.Infinity,
					"ListDescendants must surface the finite frozen_until (orphan grace) for the rollup")
				sawOrphan = true
			}
		}
		require.True(t, sawOrphan, "orphaned child must still appear in ListDescendants")
	})

	t.Run("sweep_leaf_hard_deletes", func(t *testing.T) {
		// leaf-x is orphaned (from gen 8). Expire its window and sweep → hard delete.
		require.NotNil(t, childByName("leaf-x"), "leaf still present pre-sweep")
		expireOrphan(leafID)
		swept, err := repo.SweepExpiredOrphans(ctx, 500, runtime.AllShards())
		require.NoError(t, err)
		require.GreaterOrEqual(t, swept, 1, "sweep tears down the expired leaf orphan")
		require.Nil(t, childByName("leaf-x"), "leaf orphan hard-deleted on grace expiry")
	})

	t.Run("sweep_finalizer_soft_deletes", func(t *testing.T) {
		// fin-x is orphaned (from gen 8). Expire + sweep → SOFT delete: row stays,
		// deletion_requested_at set, a delete task enqueued (the Deleter will run),
		// orphan marks cleared (now Deleting, not Orphaned).
		require.NotNil(t, childByName("fin-x"), "finalizer child present pre-sweep")
		expireOrphan(finID)
		_, err := repo.SweepExpiredOrphans(ctx, 500, runtime.AllShards())
		require.NoError(t, err)

		fin := childByName("fin-x")
		require.NotNil(t, fin, "finalizer child must NOT be hard-deleted by the sweep")
		require.True(t, fin.DeletionRequestedAt.Valid, "finalizer child soft-deleted (deletion_requested_at set)")
		require.True(t, hasDeleteTask(finID), "a delete task enqueued so the Deleter runs")
		_, orphaned, da := orphanState(finID)
		require.False(t, orphaned, "orphan marks cleared on escalation to soft-delete")
		require.False(t, da, "delete_after cleared on escalation to soft-delete")
		var phase string
		require.NoError(t, pool.QueryRow(ctx, `SELECT phase FROM resources WHERE id=$1`, finID).Scan(&phase))
		require.Equal(t, "Deleting", phase, "escalated child is Deleting, not Orphaned")
	})

	t.Logf("orphan-grace: drop→Orphaned, re-emit→re-adopt (changed+unchanged), sweep→hard(leaf)/soft(finalizer), grace=0→immediate")
}

// gracePolicy is a test-only store.ComposePolicy: a per-kind lookup of the
// orphan-grace window + finalizer that ApplyComposeResult consults to stamp the
// grace window (frozen_until) and pick soft-vs-hard prune. It stands in for the
// KindManifestCache, which cannot carry orphan_grace_secs (kind_manifest has no
// such column → a DB-loaded cache always reports grace=0). Kinds absent from a
// map default to 0/"" (no grace / no finalizer).
type gracePolicy struct {
	grace     map[model.Kind]time.Duration
	finalizer map[model.Kind]string
}

func (p gracePolicy) OrphanGraceForKind(kind model.Kind, _ int) time.Duration { return p.grace[kind] }
func (p gracePolicy) FinalizerForKind(kind model.Kind, _ int) string          { return p.finalizer[kind] }
