package test

// Tests the per-kind GLOBAL concurrency cap: at most N tasks of a kind in
// flight at once, summed across all worker pods, as ONE shared token pool
// (no per-shard slice → no shard starves). Enforced in WorkQueueTakeBatch
// (the claim) and self-healed by recount_inflight on the reaper tick.
//
// Store-level tests: they drive the claim/recount SQL directly with
// hand-inserted work_queue rows and several store.Store instances standing
// in for pods, so they're fast and assert the gate precisely.

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // database/sql "pgx" driver, for goose
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/db"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

const capKind = model.Kind("vpc")

func migrateOnly(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	pool := setupPostgres(t, ctx)
	sqlDB, err := sql.Open("pgx", pool.Config().ConnString())
	require.NoError(t, err)
	defer sqlDB.Close()
	require.NoError(t, db.Migrate(ctx, sqlDB))
	return pool
}

// seedPending inserts n pending (worker_id IS NULL) reconcile rows for kind
// into the given shard, each on its own resource.
func seedPending(t *testing.T, ctx context.Context, pool *pgxpool.Pool, kind model.Kind, shard int16, n int) {
	t.Helper()
	for range n {
		// kind_version is REQUIRED (NOT NULL, no DEFAULT) — seed an explicit v1.
		_, err := pool.Exec(ctx,
			`INSERT INTO work_queue (resource_id, task_type, kind, kind_version, generation, spec, shard_id)
			 VALUES ($1, 'reconcile', $2, 1, 1, '{}'::jsonb, $3)`,
			uuid.New(), string(kind), shard)
		require.NoError(t, err)
	}
}

// globalInflight: the cap's view of global in-flight (SUM of all partials).
func globalInflight(t *testing.T, ctx context.Context, pool *pgxpool.Pool, kind model.Kind) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(in_flight),0)::int FROM kind_inflight WHERE kind=$1`,
		string(kind)).Scan(&n))
	return n
}

// trueLeased: ground-truth leased rows for kind across all shards. A row is
// leased when a broker holds it (broker_id set); worker_id is display-only.
func trueLeased(t *testing.T, ctx context.Context, pool *pgxpool.Pool, kind model.Kind) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*)::int FROM work_queue WHERE kind=$1 AND broker_id IS NOT NULL`,
		string(kind)).Scan(&n))
	return n
}

// TestKindCapGlobalAcrossPods: cap N, several worker pods each owning a
// disjoint shard range claiming hard with no draining — TOTAL leased never
// exceeds N, and the pool saturates (no budget left on the table).
func TestKindCapGlobalAcrossPods(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pool := migrateOnly(t, ctx)
	repo := store.New(pool)

	const capN = 10
	require.NoError(t, repo.UpsertKindConfig(ctx, store.KindConfig{Kind: capKind, KindVersion: 1, MaxInflight: capN}))

	type pod struct {
		lo, hi int16
		shards []int16
		id     string
	}
	pods := []pod{
		{lo: 0, hi: 3, id: "pod-0"},
		{lo: 4, hi: 7, id: "pod-1"},
		{lo: 8, hi: 11, id: "pod-2"},
		{lo: 12, hi: 15, id: "pod-3"},
	}
	for i := range pods {
		p := &pods[i]
		for s := p.lo; s <= p.hi; s++ {
			p.shards = append(p.shards, s)
		}
		seedPending(t, ctx, pool, capKind, p.lo, 20) // plenty over the cap
	}

	for round := range 5 {
		for _, p := range pods {
			_, err := repo.WorkQueueTakeBatch(ctx, capKind, 1, store.TaskReconcile, p.id, 50, p.shards)
			require.NoError(t, err)
		}
		leased := trueLeased(t, ctx, pool, capKind)
		require.LessOrEqualf(t, leased, capN,
			"round %d: global leased %d exceeded cap %d", round, leased, capN)
	}
	require.Equal(t, capN, trueLeased(t, ctx, pool, capKind),
		"global pool should be fully utilised across pods")
}

// TestKindCapNoShardStarvation is the property you asked for: a burst of
// work confined to ONE shard must be able to consume the FULL global cap —
// the pool is shared, not sliced per shard. Other pods/shards are idle, so
// if the cap were per-shard the busy shard would be throttled to cap/4.
func TestKindCapNoShardStarvation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := migrateOnly(t, ctx)
	repo := store.New(pool)

	const capN = 12
	require.NoError(t, repo.UpsertKindConfig(ctx, store.KindConfig{Kind: capKind, KindVersion: 1, MaxInflight: capN}))

	// ALL the work is on shard 0 (pod-0's range); pods 1..3 have nothing.
	seedPending(t, ctx, pool, capKind, 0, 100)
	pod0 := []int16{0, 1, 2, 3}

	// Pod-0 alone claims — it must be able to take the whole global pool of
	// 12, not a 1/4 slice, because no other shard is using any tokens.
	got, err := repo.WorkQueueTakeBatch(ctx, capKind, 1, store.TaskReconcile, "pod-0", 50, pod0)
	require.NoError(t, err)
	require.Len(t, got, capN, "one busy shard must be able to use the FULL global cap")
	require.Equal(t, capN, trueLeased(t, ctx, pool, capKind))
}

// TestKindCapUncappedUnbounded: a kind with no kind_config row (or max_inflight=0)
// is unlimited and writes no kind_inflight rows.
func TestKindCapUncappedUnbounded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := migrateOnly(t, ctx)
	repo := store.New(pool)

	const uncapped = model.Kind("noop")
	shards := []int16{0, 1, 2, 3}
	seedPending(t, ctx, pool, uncapped, 0, 30)

	tasks, err := repo.WorkQueueTakeBatch(ctx, uncapped, 1, store.TaskReconcile, "pod-x", 25, shards)
	require.NoError(t, err)
	require.Len(t, tasks, 25, "uncapped kind claims the full requested limit")
	require.Equal(t, 0, globalInflight(t, ctx, pool, uncapped))
}

// TestKindCapRecountHeals: the recount reclaims drifted hints and restores
// truth. Claim to the cap, delete some leased rows out from under the
// counter (as drain/reap would, with NO counter write), then a recount over
// the range restores accuracy and frees budget.
func TestKindCapRecountHeals(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := migrateOnly(t, ctx)
	repo := store.New(pool)

	const capN = 10
	require.NoError(t, repo.UpsertKindConfig(ctx, store.KindConfig{Kind: capKind, KindVersion: 1, MaxInflight: capN}))
	reaperRange := []int16{0, 1, 2, 3}
	seedPending(t, ctx, pool, capKind, 0, 20)

	tasks, err := repo.WorkQueueTakeBatch(ctx, capKind, 1, store.TaskReconcile, "pod-0", 50, reaperRange)
	require.NoError(t, err)
	require.Len(t, tasks, capN)
	require.Equal(t, capN, globalInflight(t, ctx, pool, capKind))

	// Saturated → no more.
	more, err := repo.WorkQueueTakeBatch(ctx, capKind, 1, store.TaskReconcile, "pod-0", 50, reaperRange)
	require.NoError(t, err)
	require.Empty(t, more)

	// Drain deletes 6 leased rows without touching the counter.
	for i := range 6 {
		_, err := pool.Exec(ctx, `DELETE FROM work_queue WHERE id=$1 AND shard_id=$2`,
			tasks[i].ID, tasks[i].ShardID)
		require.NoError(t, err)
	}
	require.Equal(t, capN, globalInflight(t, ctx, pool, capKind), "hint drifts before recount")
	require.Equal(t, 4, trueLeased(t, ctx, pool, capKind))

	// Recount over the reaper's range heals to ground truth.
	require.NoError(t, repo.RecountInflight(ctx, reaperRange))
	require.Equal(t, 4, globalInflight(t, ctx, pool, capKind), "recount restores truth")

	healed, err := repo.WorkQueueTakeBatch(ctx, capKind, 1, store.TaskReconcile, "pod-0", 50, reaperRange)
	require.NoError(t, err)
	require.Len(t, healed, 6, "freed budget is claimable after recount")
	require.Equal(t, capN, trueLeased(t, ctx, pool, capKind))
}

// TestKindCapRecountReclaimsWorkerHints is the cross-slicing fix: many
// finer WORKER hint partials inside one coarser REAPER range must be
// reclaimed and collapsed into one authoritative count — even when the
// worker ranges don't line up with the reaper range. This is the bug the
// range-owning recount fixes.
func TestKindCapRecountReclaimsWorkerHints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := migrateOnly(t, ctx)
	repo := store.New(pool)

	const capN = 100
	require.NoError(t, repo.UpsertKindConfig(ctx, store.KindConfig{Kind: capKind, KindVersion: 1, MaxInflight: capN}))

	// Four narrow worker ranges (range_lo = 0,4,8,12), each claims 5 — so
	// four hint partials accumulate, summing to 20.
	for _, lo := range []int16{0, 4, 8, 12} {
		seedPending(t, ctx, pool, capKind, lo, 5)
		shards := []int16{lo, lo + 1, lo + 2, lo + 3}
		got, err := repo.WorkQueueTakeBatch(ctx, capKind, 1, store.TaskReconcile, "w", 5, shards)
		require.NoError(t, err)
		require.Len(t, got, 5)
	}
	var partialRows int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*)::int FROM kind_inflight WHERE kind=$1`, string(capKind)).Scan(&partialRows))
	require.Equal(t, 4, partialRows, "four worker hint partials")
	require.Equal(t, 20, globalInflight(t, ctx, pool, capKind))

	// One COARSE reaper range [0..15] covers all four. Its recount must
	// reclaim all four hints and leave ONE authoritative row = 20.
	require.NoError(t, repo.RecountInflight(ctx, []int16{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}))
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*)::int FROM kind_inflight WHERE kind=$1`, string(capKind)).Scan(&partialRows))
	require.Equal(t, 1, partialRows, "recount collapses worker hints into one authoritative partial")
	require.Equal(t, 20, globalInflight(t, ctx, pool, capKind), "global sum preserved exactly")
}

// TestKindCapRecountSweepsStalePartials: a partial from a reaper range that
// stopped recounting is aged out so it can't inflate the SUM forever.
func TestKindCapRecountSweepsStalePartials(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := migrateOnly(t, ctx)
	repo := store.New(pool)

	require.NoError(t, repo.UpsertKindConfig(ctx, store.KindConfig{Kind: capKind, KindVersion: 1, MaxInflight: 10}))

	// Stale authoritative partial for a long-dead reaper range (range_lo=99).
	// kind_version is REQUIRED (NOT NULL, no DEFAULT) — seed an explicit v1.
	_, err := pool.Exec(ctx,
		`INSERT INTO kind_inflight (kind, kind_version, range_lo, in_flight, refreshed_at)
		 VALUES ($1, 1, 99, 7, now() - interval '10 minutes')`, string(capKind))
	require.NoError(t, err)
	require.Equal(t, 7, globalInflight(t, ctx, pool, capKind))

	// A live reaper recounts a DIFFERENT range (0..3); the sweep drops the
	// stale partial regardless of range.
	require.NoError(t, repo.RecountInflight(ctx, []int16{0, 1, 2, 3}))
	require.Equal(t, 0, globalInflight(t, ctx, pool, capKind),
		"stale partial swept; live range has no leased rows")
}
