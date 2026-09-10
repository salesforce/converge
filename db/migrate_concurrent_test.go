package db

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestConcurrentMigrateIsSafe proves the multi-pod boot race is closed: N pods
// (control + broker) starting at once against a FRESH database all call
// MigrateAll simultaneously, and the Postgres session advisory lock
// (WithSessionLocker) serializes them so EVERY call returns nil — exactly one
// applies the schema, the rest block on the lock then observe it current and
// no-op. Before the session lock, the losers hit "relation already exists" and
// crashlooped. This is the regression guard for that fix.
//
// Each goroutine opens its OWN *sql.DB (its own connection pool), the way
// separate pods each have their own connection — a shared *sql.DB would hide the
// cross-process race the lock defends against.
func TestConcurrentMigrateIsSafe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pg, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("concurrent_migrate_test"),
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

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	const pods = 8
	// Barrier so all goroutines hit MigrateAll as simultaneously as the scheduler
	// allows — maximizing the race window the lock must cover.
	start := make(chan struct{})
	errs := make([]error, pods)
	var wg sync.WaitGroup
	for i := range pods {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			d, err := sql.Open("pgx", dsn) // each "pod" its own handle
			if err != nil {
				errs[i] = err
				return
			}
			defer d.Close()
			<-start
			errs[i] = MigrateAll(ctx, d)
		}(i)
	}
	close(start)
	wg.Wait()

	// EVERY pod must succeed — the whole point of the session lock is that a
	// concurrent migrator waits and no-ops rather than erroring.
	for i, e := range errs {
		require.NoErrorf(t, e, "pod %d: MigrateAll must not error under concurrency", i)
	}

	// The schema was applied EXACTLY once: the goose version table has exactly one
	// applied app migration (version 1 — converge uses a single 00001_schema.sql),
	// not N duplicate rows from re-application.
	verify, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	defer verify.Close()

	var applied int
	require.NoError(t, verify.QueryRowContext(ctx,
		`SELECT count(*) FROM goose_db_version WHERE version_id > 0 AND is_applied`).Scan(&applied))
	require.Equal(t, 1, applied, "schema must be applied exactly once, not re-applied per pod")

	// And the schema is actually present + usable (a core table exists).
	var reg string
	require.NoError(t, verify.QueryRowContext(ctx, `SELECT to_regclass('public.resources')::text`).Scan(&reg))
	require.Equal(t, "resources", reg, "migrations did not create the schema")

	// A second MigrateAll after everyone's done is a clean no-op (idempotent).
	require.NoError(t, MigrateAll(ctx, verify), "re-running MigrateAll on a current DB must be a no-op")
}
