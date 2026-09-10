package test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/shardutil"
	"github.com/salesforce/converge/internal/store"
)

// TestResyncRecomposeRespectsInterval is the regression for the resync
// gate bug: a freshly-settled composite (last_reconciled_at IS NULL) was
// treated as "due now" by requeue_for_resync, so the first resync sweep —
// with ResyncRecomposes — zeroed composed_gen and forced a full recompose
// ~one sweep (~30s) after settling, instead of waiting the configured
// interval. At 1M children that immediate full re-diff was a ~20s
// regression and produced a redundant 2nd compose-succeeded.
//
// The fix baselines a never-resynced row's due-clock on updated_at (when
// it last settled), so it is NOT due until a full interval has elapsed.
//
// Drives requeue_for_resync directly (no resyncer wiring) to assert:
//   - immediately after settle, long interval → 0 requeued, composed_gen
//     UNCHANGED (no recompose forced);
//   - after backdating updated_at past the interval → 1 requeued,
//     composed_gen dropped below generation (drift-correction preserved).
func TestResyncRecomposeRespectsInterval(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
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

	q := dbq.New(pool)
	st := store.New(pool)
	shards := shardutil.ShardsForPod(0, 1, shardutil.NumShards)

	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "resync-root",
		buildSpec(controllableChildSpec{Kind: string(account.Kind), Name: "a"}),
		nil,
	)
	require.NoError(t, err)

	// Settle: root synced + composer stamped composed_gen to generation, AND the
	// reconcile work_queue row is drained. The drain is load-bearing for step (2):
	// requeue_for_resync excludes any row that still has a pending reconcile task (a
	// row already being worked needn't be re-pended), so if we proceed while the
	// engine's in-flight reconcile is still queued, the backdated row is (correctly)
	// skipped and n=0 — a false failure on a slow box where the drainer hasn't caught
	// up. Waiting for quiescence here makes step (2) deterministic regardless of runner
	// speed. (This was the flake: on a fast box the row cleared before step 2; on a
	// slow CI runner it didn't.)
	require.Eventually(t, func() bool {
		r, err := q.GetResourceInfo(ctx, rootID)
		if err != nil || r.SyncedGen < r.Generation {
			return false
		}
		var pending int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM work_queue WHERE resource_id = $1 AND task_type = 'reconcile'::task_type`,
			rootID).Scan(&pending); err != nil {
			return false
		}
		return pending == 0
	}, 20*time.Second, 100*time.Millisecond, "root should settle and its reconcile work drain")
	composedBefore := composedGenOf(t, ctx, pool, rootID)
	require.Greater(t, composedBefore, int64(0), "composer should have stamped composed_gen")

	// (1) Immediate resync with a long (1h) interval — the fresh row's
	// updated_at is ~now, so it must NOT be due. With the fix: 0 requeued,
	// composed_gen untouched (no recompose forced).
	n, err := st.RequeueForResync(ctx, model.Kind(classicbom.Kind), time.Hour, 100, shards, true)
	require.NoError(t, err)
	require.Equal(t, 0, n, "a freshly-settled root must NOT be resync-recomposed before a full interval elapses")
	require.Equal(t, composedBefore, composedGenOf(t, ctx, pool, rootID),
		"composed_gen must be unchanged (no recompose forced on the first sweep)")

	// (2) Backdate the due-clock past the interval, then resync again — now
	// due. composed_gen drops below generation so the next reconcile
	// re-runs the composer (drift-correction preserved).
	//
	// The due-clock baseline is COALESCE(last_reconciled_at, updated_at)
	// (see requeue_for_resync): last_reconciled_at is written ONLY by the
	// resyncer, so for a never-resynced row it is NULL and updated_at is the
	// baseline — hence we backdate updated_at. We COALESCE the backdate over
	// BOTH columns defensively, so the test still exercises "a full interval
	// elapsed" even if a concurrent path ever stamps last_reconciled_at.
	//
	// requeue_for_resync ALSO skips a row that has a pending reconcile task
	// (NOT EXISTS on work_queue) — a row already being worked needn't be
	// re-pended. The engine is live, so a late child-driven rollup can briefly
	// re-pend the root's reconcile between the two steps; if that row is
	// present when we call, the resync (correctly) skips it and n=0 — a false
	// failure. So we re-establish quiescence (settled AND no pending reconcile)
	// AFTER backdating and immediately before the assert, re-applying the
	// backdate last so a settling reconcile that bumped updated_at can't push
	// the clock back inside the interval. This makes step (2) deterministic
	// regardless of runner speed.
	require.Eventually(t, func() bool {
		r, err := q.GetResourceInfo(ctx, rootID)
		if err != nil || r.SyncedGen < r.Generation {
			return false
		}
		var pending int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM work_queue WHERE resource_id = $1 AND task_type = 'reconcile'::task_type`,
			rootID).Scan(&pending); err != nil {
			return false
		}
		return pending == 0
	}, 20*time.Second, 100*time.Millisecond, "root should be settled with its reconcile work drained before backdating")
	_, err = pool.Exec(ctx,
		`UPDATE resources
		    SET updated_at = now() - interval '2 hours',
		        last_reconciled_at = CASE WHEN last_reconciled_at IS NULL
		                                  THEN NULL
		                                  ELSE now() - interval '2 hours' END
		  WHERE id = $1`, rootID)
	require.NoError(t, err)
	n, err = st.RequeueForResync(ctx, model.Kind(classicbom.Kind), time.Hour, 100, shards, true)
	require.NoError(t, err)
	require.Equal(t, 1, n, "after a full interval the settled root IS due for resync")
	require.Less(t, composedGenOf(t, ctx, pool, rootID), genOf(t, ctx, pool, rootID),
		"recompose: composed_gen must drop below generation so the composer re-runs")
}

func composedGenOf(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) int64 {
	t.Helper()
	var g int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT composed_gen FROM resources WHERE id = $1`, id).Scan(&g))
	return g
}
