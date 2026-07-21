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

	// Settle: root synced + composer stamped composed_gen to generation.
	require.Eventually(t, func() bool {
		r, err := q.GetResourceInfo(ctx, rootID)
		return err == nil && r.SyncedGen >= r.Generation
	}, 20*time.Second, 100*time.Millisecond, "root should settle")
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

	// (2) Backdate updated_at past the interval, then resync again — now
	// due. composed_gen drops below generation so the next reconcile
	// re-runs the composer (drift-correction preserved).
	_, err = pool.Exec(ctx, `UPDATE resources SET updated_at = now() - interval '2 hours' WHERE id = $1`, rootID)
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
