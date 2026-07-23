package test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/store"
)

// TestQuarantine validates the operator "set a failed resource aside" feature:
// quarantining freezes a resource from EVERY scheduler (no retry, no resync, no
// resurrection by a dependency's value-flow status change) and excludes it from
// its root's rollup + the descendant gate — so one bad child can't block the
// BOM — WITHOUT deleting it (the reaper never touches it). Un-quarantine (or a
// spec edit) re-arms it.
//
// Driven directly against the store (no engine) so state is deterministic and
// nothing races to claim/drain mid-assert. Mirrors compose_orphan_grace_test.go.
func TestQuarantine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)
	st := store.New(pool)

	const (
		rootKind  model.Kind = "quar-root"
		leafKind             = model.Kind(account.Kind) // a real, registered worker kind
		childName            = "quar-child"
	)

	// A root with one child. ApplySpec enqueues the root's reconcile row; we
	// drive the child directly below.
	rootID, err := st.ApplySpec(ctx, rootKind, "quar-root-1", json.RawMessage(`{}`), nil)
	require.NoError(t, err)

	// Create the child via the composer path so it's a real owned child with
	// an edge we can flow status across. Fence-claim + ApplyComposeResult; the
	// freshly-enqueued row is at the default claim_epoch 1, which the compose
	// fence matches (manifest 0 = inert).
	wq := fenceClaimRoot(t, ctx, pool, rootID.ID, "quar-test-worker", 1)
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	// Last arg is store.ComposePolicy; account has no orphan-grace/finalizer, so
	// nil (no-grace, immediate-hard-delete default) is the right policy here.
	_, err = st.WithTx(tx).ApplyComposeResult(ctx, rootID.ID, 1, wq, 1, 0,
		[]model.ChildSpec{{Kind: leafKind, KindVersion: 1, Name: childName, Spec: map[string]string{"name": childName}}},
		nil, nil, nil)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	childID := func() uuid.UUID {
		kids, err := st.GetChildrenByOwner(ctx, rootID.ID, 1000000)
		require.NoError(t, err)
		require.Len(t, kids, 1)
		return kids[0].ID
	}()

	phaseOf := func(id uuid.UUID) string {
		var p string
		require.NoError(t, pool.QueryRow(ctx, `SELECT phase FROM resources WHERE id=$1`, id).Scan(&p))
		return p
	}
	quarantinedAtSet := func(id uuid.UUID) bool {
		var set bool
		// (frozen_until = 'infinity') IS TRUE so a NULL frozen_until (released /
		// never-quarantined) scans as false, not a NULL → Scan error.
		require.NoError(t, pool.QueryRow(ctx, `SELECT (frozen_until = 'infinity') IS TRUE FROM resources WHERE id=$1`, id).Scan(&set))
		return set
	}
	hasReconcileTask := func(id uuid.UUID) bool {
		var n int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM work_queue WHERE resource_id=$1 AND task_type='reconcile'`, id).Scan(&n))
		return n > 0
	}
	// Force the child into a FAILED, lagging state (the thing an operator would
	// quarantine): bump generation so it's lagging, stamp failure_gen=generation.
	makeFailedLagging := func(id uuid.UUID) {
		_, err := pool.Exec(ctx,
			`UPDATE resources SET generation = generation + 1, failure_gen = generation + 1 WHERE id=$1`, id)
		require.NoError(t, err)
	}

	makeFailedLagging(childID)
	require.Equal(t, "Failed", phaseOf(childID), "child set up as Failed")

	t.Run("quarantine_sets_aside_and_drops_pending_task", func(t *testing.T) {
		// Pre-seed a pending reconcile row to prove quarantine drops it.
		require.NoError(t, pool.QueryRow(ctx, `SELECT 1`).Scan(new(int)))
		_, _ = st.ScheduleEligible(ctx, []uuid.UUID{childID}) // may or may not enqueue; we assert post-quarantine

		ok, err := st.QuarantineResource(ctx, childID, "test")
		require.NoError(t, err)
		require.True(t, ok, "quarantine succeeds on a live failed resource")
		require.True(t, quarantinedAtSet(childID), "quarantined_at stamped")
		require.Equal(t, "Quarantined", phaseOf(childID), "phase = Quarantined (above Failed)")
		require.False(t, hasReconcileTask(childID), "quarantine drops the pending reconcile task")

		// Idempotent: re-quarantine is a no-op.
		ok, err = st.QuarantineResource(ctx, childID, "test")
		require.NoError(t, err)
		require.False(t, ok, "re-quarantine is a no-op")
	})

	t.Run("frozen_from_schedule_eligible", func(t *testing.T) {
		n, err := st.ScheduleEligible(ctx, []uuid.UUID{childID})
		require.NoError(t, err)
		require.Equal(t, 0, n, "quarantined resource is NOT scheduled even though it's lagging")
		require.False(t, hasReconcileTask(childID), "no reconcile task enqueued for a quarantined resource")
	})

	t.Run("frozen_from_requeue_failed_and_pending", func(t *testing.T) {
		// Age it so the retry window has elapsed, then run the reaper backstop.
		_, err := pool.Exec(ctx, `UPDATE resources SET updated_at = now() - interval '1 hour' WHERE id=$1`, childID)
		require.NoError(t, err)
		_, err = st.RequeueFailedAndPending(ctx, time.Second, 500, runtime.AllShards())
		require.NoError(t, err)
		require.False(t, hasReconcileTask(childID), "requeue_failed_and_pending skips a quarantined resource")
	})

	t.Run("frozen_from_operate", func(t *testing.T) {
		// Invoking a subresource verb must NOT enqueue an operate task on a
		// quarantined resource (the EnqueueOperationWork freeze guard). The
		// operation row may be created, but no work_queue 'operate' row lands, so
		// the verb never runs against the live provider. (account has the
		// enable_flow_logs verb on networking; use a kind with a verb — here we
		// drive CreateOperationRow directly and assert the work_queue side.)
		hasOperateTask := func(id uuid.UUID) bool {
			var n int
			require.NoError(t, pool.QueryRow(ctx,
				`SELECT count(*) FROM work_queue WHERE resource_id=$1 AND task_type='operate'`, id).Scan(&n))
			return n > 0
		}
		_, err := st.CreateOperationRow(ctx, childID, "noop-verb", []byte(`{}`), "test")
		require.NoError(t, err)
		require.False(t, hasOperateTask(childID), "no operate task enqueued for a quarantined resource (freeze guard)")
	})

	t.Run("reaper_never_deletes_quarantined", func(t *testing.T) {
		// The orphan sweep keys on delete_after (NULL here), so it must not touch
		// a quarantined row. Confirm the row survives a sweep.
		_, err := st.SweepExpiredOrphans(ctx, 500, runtime.AllShards())
		require.NoError(t, err)
		var n int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM resources WHERE id=$1`, childID).Scan(&n))
		require.Equal(t, 1, n, "quarantined resource is NEVER deleted by the reaper")
	})

	t.Run("rollup_ignores_quarantined", func(t *testing.T) {
		// The root's descendant gate must not be blocked by the quarantined child:
		// schedule the root and confirm it's eligible (the child no longer pins it).
		// (The child is lagging+quarantined; without the gate exclusion the root
		// would be suppressed.) First make the root itself need a reconcile.
		_, err := pool.Exec(ctx, `UPDATE resources SET generation = generation + 1 WHERE id=$1`, rootID.ID)
		require.NoError(t, err)
		n, err := st.ScheduleEligible(ctx, []uuid.UUID{rootID.ID})
		require.NoError(t, err)
		require.Equal(t, 1, n, "root IS schedulable — quarantined child excluded from the descendant gate (rollup unblocked)")
	})

	t.Run("unquarantine_reactivates", func(t *testing.T) {
		ok, err := st.UnquarantineResource(ctx, childID, "test")
		require.NoError(t, err)
		require.True(t, ok, "unquarantine succeeds")
		require.False(t, quarantinedAtSet(childID), "quarantined_at cleared")
		require.NotEqual(t, "Quarantined", phaseOf(childID), "phase back to a live phase")
		// unquarantine_resource calls schedule_eligible; the child is lagging
		// (generation > synced_gen) and not terminal, so it re-pends.
		require.True(t, hasReconcileTask(childID), "un-quarantine re-arms: reconcile task enqueued")
	})

	t.Run("spec_edit_and_resync_do_NOT_unquarantine", func(t *testing.T) {
		// Quarantine is a HARD freeze: only an explicit unquarantine lifts it.
		// A spec edit (Apply) or a Resync persists the new spec / bumps generation
		// but must NOT clear quarantined_at and must NOT reschedule. The row
		// reconciles to its latest spec only once released.
		ok, err := st.QuarantineResource(ctx, rootID.ID, "test")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "Quarantined", phaseOf(rootID.ID))

		var genBefore int64
		require.NoError(t, pool.QueryRow(ctx, `SELECT generation FROM resources WHERE id=$1`, rootID.ID).Scan(&genBefore))

		// Spec edit: new spec persisted (generation bumps) but freeze HELD.
		_, err = st.ApplySpec(ctx, rootKind, "quar-root-1", json.RawMessage(`{"edited":true}`), nil)
		require.NoError(t, err)
		require.True(t, quarantinedAtSet(rootID.ID), "a spec edit must NOT clear quarantine (hard freeze)")
		require.Equal(t, "Quarantined", phaseOf(rootID.ID), "still Quarantined after a spec edit")
		require.False(t, hasReconcileTask(rootID.ID), "a spec edit on a quarantined resource does NOT reschedule it")
		var genAfter int64
		require.NoError(t, pool.QueryRow(ctx, `SELECT generation FROM resources WHERE id=$1`, rootID.ID).Scan(&genAfter))
		require.Greater(t, genAfter, genBefore, "the new spec IS persisted (generation bumped) — it just doesn't run until released")

		// Resync: bumps generation, freeze still blocks the enqueue → no-op run.
		require.NoError(t, st.Reconcile(ctx, rootID.ID))
		require.True(t, quarantinedAtSet(rootID.ID), "a resync must NOT clear quarantine")
		require.False(t, hasReconcileTask(rootID.ID), "a resync on a quarantined resource does NOT reschedule it")

		// Only an explicit release lifts it (and then it re-pends to the latest spec).
		ok, err = st.UnquarantineResource(ctx, rootID.ID, "test")
		require.NoError(t, err)
		require.True(t, ok)
		require.False(t, quarantinedAtSet(rootID.ID), "explicit release clears the quarantine")
		require.True(t, hasReconcileTask(rootID.ID), "released → re-pends to reconcile the latest spec")
	})
}

// TestQuarantineSpoolsOnlyPendingTask guards a bug found by the frozen_until
// adversarial sweep: quarantine_resource must drop ONLY the PENDING reconcile
// task (work_queue.worker_id IS NULL), never a CLAIMED one (worker_id set). A
// claimed reconcile is being executed by a worker right now; deleting its row
// out from under it makes the worker's fenced AppendOutbox result get silently
// discarded (the EXISTS fence no longer finds the row). Quarantine must let the
// in-flight reconcile finish and land normally — the freeze stops the NEXT
// schedule, not the running one.
func TestQuarantineSpoolsOnlyPendingTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)
	st := store.New(pool)

	id, err := st.ApplySpec(ctx, "quar-spool-root", "qs-1", json.RawMessage(`{}`), nil)
	require.NoError(t, err)

	reconcileTasks := func() (pending, claimed int) {
		// Claimed = a broker holds the lease (broker_id). worker_id is display-only.
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT count(*) FILTER (WHERE broker_id IS NULL),
			       count(*) FILTER (WHERE broker_id IS NOT NULL)
			  FROM work_queue WHERE resource_id=$1 AND task_type='reconcile'`, id.ID).Scan(&pending, &claimed))
		return
	}

	// ApplySpec enqueued a pending reconcile row; mark it CLAIMED (a broker holds the
	// lease, mirroring a real in-flight reconcile).
	_, err = pool.Exec(ctx,
		`UPDATE work_queue SET broker_id='qs-broker', heartbeat_at=now() WHERE resource_id=$1 AND task_type='reconcile'`, id.ID)
	require.NoError(t, err)
	p, c := reconcileTasks()
	require.Equal(t, 0, p)
	require.Equal(t, 1, c, "one claimed reconcile in flight")

	// Quarantine must NOT delete the claimed row.
	ok, err := st.QuarantineResource(ctx, id.ID, "test")
	require.NoError(t, err)
	require.True(t, ok)
	p, c = reconcileTasks()
	require.Equal(t, 1, c, "quarantine MUST NOT delete a CLAIMED reconcile (in-flight work survives)")
	require.Equal(t, 0, p, "no pending reconcile to drop here")
}

// TestOrphanFreezeRegression guards the bug the quarantine work also fixed: an
// ORPHANED resource (finite frozen_until set, pending teardown) must be FROZEN — a
// dependency's status change must NOT re-activate or spec-bump it, and no
// scheduler may re-pend it. Before the fix, the value-flow substitute pass
// bumped an orphaned dependent's generation and schedule_eligible re-queued it,
// resurrecting a resource that was on its way out.
func TestOrphanFreezeRegression(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)
	st := store.New(pool)

	rootID, err := st.ApplySpec(ctx, "orphan-freeze-root", "ofr-1", json.RawMessage(`{}`), nil)
	require.NoError(t, err)

	hasReconcileTask := func(id uuid.UUID) bool {
		var n int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM work_queue WHERE resource_id=$1 AND task_type='reconcile'`, id).Scan(&n))
		return n > 0
	}

	// Make the root an orphan: lagging + a FINITE frozen_until set (as the
	// composer prune would stamp), and clear any pending task.
	_, err = pool.Exec(ctx, `
		UPDATE resources
		   SET generation = generation + 1,
		       frozen_until = now() + interval '1 hour'
		 WHERE id=$1`, rootID.ID)
	require.NoError(t, err)
	_, _ = pool.Exec(ctx, `DELETE FROM work_queue WHERE resource_id=$1`, rootID.ID)

	var phase string
	require.NoError(t, pool.QueryRow(ctx, `SELECT phase FROM resources WHERE id=$1`, rootID.ID).Scan(&phase))
	require.Equal(t, "Orphaned", phase)

	// Every scheduler must skip it.
	n, err := st.ScheduleEligible(ctx, []uuid.UUID{rootID.ID})
	require.NoError(t, err)
	require.Equal(t, 0, n, "schedule_eligible must NOT re-pend an orphaned (pending-teardown) resource")
	require.False(t, hasReconcileTask(rootID.ID), "no reconcile task for an orphaned resource")

	_, err = pool.Exec(ctx, `UPDATE resources SET updated_at = now() - interval '1 hour' WHERE id=$1`, rootID.ID)
	require.NoError(t, err)
	_, err = st.RequeueFailedAndPending(ctx, time.Second, 500, runtime.AllShards())
	require.NoError(t, err)
	require.False(t, hasReconcileTask(rootID.ID), "requeue_failed_and_pending must skip an orphaned resource")
}
