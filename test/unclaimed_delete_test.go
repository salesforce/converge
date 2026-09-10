package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/examples/demos/classic/fault"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/test/internal/demoruntime"
)

// TestDeleteUnclaimedWork validates the abandoned-task GC pass
// (store.DeleteUnclaimedWork → delete_unclaimed_work), the reaper's 24h
// janitor for work that NO worker ever claimed — its kind lost its worker
// (provider removed / decommissioned), so the row would otherwise sit in
// idx_work_queue_pending forever, re-scanned by every claim for that kind.
//
// It is the OPPOSITE case to the crash reaper (reap_stale_work, see
// crash_recovery_test.go): that one RECOVERS a stale CLAIMED row by nulling
// worker_id; this one DROPS a never-claimed (worker_id IS NULL) row. The
// invariants under test:
//
//   - an UNCLAIMED row aged past the window is deleted;
//   - a FRESH unclaimed row survives (not deleted merely for being pending —
//     normal queued work is unclaimed for seconds);
//   - a CLAIMED row is never touched here, even with an ancient created_at
//     (that's the stale-claim reaper's job, gated on heartbeat, not this);
//   - it is GC, not abandonment: the resource stays lagging, so a returning
//     worker's fleet gets a fresh row from requeue_failed_and_pending.
//
// Driven directly against the store (no engine) so created_at is controlled
// precisely and nothing races to claim/drain the rows mid-assert — mirrors
// claim_release_test.go.
func TestDeleteUnclaimedWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)

	// Seed kind_config so the kinds resolve (ApplySpec/schedule_eligible read it).
	reg := newTReg()
	reg.Add(demoruntime.ClassicBOM(0))
	reg.Add(demoruntime.Account(0, 0, fault.Injector{}))
	require.NoError(t, reg.seed(ctx, pool))

	st := store.New(pool)

	// Three roots. ApplySpec enqueues each one's reconcile work_queue row via
	// schedule_eligible, all initially unclaimed (worker_id NULL) with a fresh
	// created_at — the genuine "just queued, waiting for a worker" state.
	abandonedRoot, err := st.ApplySpec(ctx, model.Kind(classicbom.Kind), "abandoned-root",
		buildSpec(controllableChildSpec{Kind: string(account.Kind), Name: "abandoned-child"}), nil)
	require.NoError(t, err)
	freshRoot, err := st.ApplySpec(ctx, model.Kind(classicbom.Kind), "fresh-root",
		buildSpec(controllableChildSpec{Kind: string(account.Kind), Name: "fresh-child"}), nil)
	require.NoError(t, err)
	claimedRoot, err := st.ApplySpec(ctx, model.Kind(classicbom.Kind), "claimed-root",
		buildSpec(controllableChildSpec{Kind: string(account.Kind), Name: "claimed-child"}), nil)
	require.NoError(t, err)

	// Age the abandoned root's UNCLAIMED row far past the GC window — exactly
	// what Postgres sees after a kind's worker has been gone for a day. (We
	// poison created_at rather than wait; this is the only test-specific state.)
	_, err = pool.Exec(ctx, `
		UPDATE work_queue SET created_at = now() - INTERVAL '48 hours'
		WHERE resource_id = $1 AND task_type = 'reconcile'`, abandonedRoot.ID)
	require.NoError(t, err)

	// Claim the third root's row with an EQUALLY ancient created_at but a held
	// broker_id (the lease): a claimed row must be invisible to this pass no matter
	// how old, because deleting it would orphan a task a live broker is running. (Its
	// staleness, if the broker dies, is the crash reaper's concern, not this.)
	ct, err := pool.Exec(ctx, `
		UPDATE work_queue SET broker_id = 'busy-broker', heartbeat_at = now(),
		                      created_at = now() - INTERVAL '48 hours'
		WHERE resource_id = $1 AND task_type = 'reconcile'`, claimedRoot.ID)
	require.NoError(t, err)
	require.Equal(t, int64(1), ct.RowsAffected())

	// Run the GC pass over ALL shards with a 24h window: only the abandoned
	// row's created_at (48h) exceeds it.
	deleted, err := st.DeleteUnclaimedWork(ctx, 24*time.Hour, 500, runtime.AllShards())
	require.NoError(t, err)
	require.Equal(t, 1, deleted, "exactly the one aged-out unclaimed row is GC'd")

	rowExists := func(resourceID interface{}) bool {
		var n int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM work_queue WHERE resource_id = $1 AND task_type = 'reconcile'`,
			resourceID).Scan(&n))
		return n > 0
	}

	require.False(t, rowExists(abandonedRoot.ID), "aged-out unclaimed row must be deleted")
	require.True(t, rowExists(freshRoot.ID), "fresh unclaimed row must survive (it's just queued)")
	require.True(t, rowExists(claimedRoot.ID), "claimed row must survive regardless of age")

	// GC, NOT abandonment: the deleted row's resource is still lagging
	// (generation > synced_gen), so it remains eligible for a fresh enqueue the
	// moment a worker for its kind returns — the queue entry was reclaimed, the
	// intent to reconcile was not.
	r, err := st.GetResourceInfo(ctx, abandonedRoot.ID)
	require.NoError(t, err)
	require.Greater(t, r.Generation, r.SyncedGen,
		"resource stays lagging after its queue row is GC'd, so requeue can re-enqueue")

	// Re-running with a window the fresh row can't satisfy still deletes nothing
	// (idempotent; the only candidate is already gone).
	deleted, err = st.DeleteUnclaimedWork(ctx, 24*time.Hour, 500, runtime.AllShards())
	require.NoError(t, err)
	require.Equal(t, 0, deleted, "second pass finds nothing left to GC")
}
