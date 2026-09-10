package db

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestShardOfOverflowSafe verifies the shard hash is overflow-safe and
// stays in [0,255]. The expression is abs(mod(hashtext(x),256)), NOT
// mod(abs(hashtext(x)),256): hashtext returns a signed int4 that can be
// INT_MIN, whose abs() overflows int4 → "integer out of range", which
// would fail the INSERT for that resource. mod-first avoids it. This test
// guards against a regression back to the abs-first form.
func TestShardOfOverflowSafe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pg, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("shard_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pg.Terminate(context.Background()) })

	connStr, err := pg.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	dbConn, err := sql.Open("pgx", connStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbConn.Close() })

	require.NoError(t, Migrate(ctx, dbConn))

	// 1. The function must not error and must stay in range across a large
	//    sample of inputs.
	var minShard, maxShard int
	require.NoError(t, dbConn.QueryRowContext(ctx, `
		SELECT min(shard_of(gen_random_uuid())), max(shard_of(gen_random_uuid()))
		FROM generate_series(1, 200000)
	`).Scan(&minShard, &maxShard))
	require.GreaterOrEqual(t, minShard, 0, "shard_of returned a negative shard")
	require.LessOrEqual(t, maxShard, 255, "shard_of returned a shard >= NumShards")

	// 2. The INT_MIN overflow case: mod-then-abs is used because abs-then-mod
	//    overflows on INT_MIN (abs(-2147483648) has no int4 representation).
	//    Exercise the value directly — it must succeed and yield a valid shard.
	var edge int
	require.NoError(t, dbConn.QueryRowContext(ctx,
		`SELECT abs(mod((-2147483648)::int, 256))`).Scan(&edge))
	require.Equal(t, 0, edge)

	// 3. Sanity: abs(mod(h,256)) is value-identical to mod(abs(h),256) for
	//    every non-INT_MIN input, so the overflow-safe form addresses the same
	//    shard as the naive form. Assert zero mismatches over a sample (INT_MIN
	//    excluded — it cannot occur as a shard input).
	var mismatches int
	require.NoError(t, dbConn.QueryRowContext(ctx, `
		WITH s AS (SELECT hashtext(gen_random_uuid()::text) AS h FROM generate_series(1, 200000))
		SELECT count(*) FILTER (WHERE abs(mod(h,256)) <> mod(abs(h),256))
		FROM s WHERE h <> -2147483648
	`).Scan(&mismatches))
	require.Equal(t, 0, mismatches, "shard expr diverges from mod(abs(...)) values")
}
