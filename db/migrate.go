package db

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5/pgxpool"
	// Registers the "pgx" database/sql driver that MigrateAllPool's sql.Open opens
	// (goose runs through a *sql.DB; the rest of the app uses the pgxpool).
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

//go:embed migrations/*.sql
var Migrations embed.FS

func init() {
	goose.SetBaseFS(Migrations)
}

// migrationsFS is the embedded migrations rooted at the "migrations" dir, as the
// goose Provider expects (it walks the FS root for *.sql).
func migrationsFS() (fs.FS, error) {
	return fs.Sub(Migrations, "migrations")
}

// MigrateAll runs the app migrations UP, safe for CONCURRENT callers.
//
// Every DB-touching pod (control AND broker) runs this on startup before it
// reads any schema-dependent table, so no pod races ahead of the migration on a
// fresh database (there is no boot ordering between the control and broker
// Deployments). Concurrency is made safe by a Postgres SESSION advisory lock
// (goose WithSessionLocker): of N pods starting at once, exactly ONE acquires the
// lock and applies the pending migrations; the rest BLOCK on the lock and, when
// it releases, observe the schema already current and return nil — no
// "relation already exists" error, no boot crashloop. Idempotent: a no-op
// (returns nil) when the schema is already up to date.
func MigrateAll(ctx context.Context, sqlDB *sql.DB) error {
	return Migrate(ctx, sqlDB)
}

// MigrateAllPool is the pgxpool-shaped entrypoint the pod boot uses: it opens a
// throwaway database/sql handle over the SAME DSN as the running pgxpool (goose
// needs a *sql.DB; the rest of the app uses pgx), runs MigrateAll under the
// advisory lock, and closes the handle. cmd/converge calls this ONCE as the first
// DB operation in run() — before any schema-reading step (the config-plane cache,
// the spec-validator rebuild, the sweepers) — so on a fresh DB every such read
// sees a migrated schema.
func MigrateAllPool(ctx context.Context, pool *pgxpool.Pool) error {
	sqlDB, err := sql.Open("pgx", pool.Config().ConnString())
	if err != nil {
		return fmt.Errorf("open sql db for migrations: %w", err)
	}
	defer sqlDB.Close()
	return MigrateAll(ctx, sqlDB)
}

// Migrate applies all pending migrations UP through goose, serialized across
// concurrent callers by the session advisory lock. MigrateAll/MigrateAllPool are
// the callable entrypoints; this is the shared implementation.
func Migrate(ctx context.Context, sqlDB *sql.DB) error {
	fsys, err := migrationsFS()
	if err != nil {
		return fmt.Errorf("sub migrations fs: %w", err)
	}
	// PostgresSessionLocker: acquires pg_advisory_lock on a dedicated session for
	// the duration of the Up, releasing it after. Its default lock ID is fixed, so
	// every converge pod contends on the SAME key — the serialization point. The
	// default lock is BLOCKING (a loser waits, then finds nothing pending), which
	// is exactly the "wait, don't crash" behaviour we want at boot.
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("new postgres session locker: %w", err)
	}
	p, err := goose.NewProvider(goose.DialectPostgres, sqlDB, fsys, goose.WithSessionLocker(locker))
	if err != nil {
		return fmt.Errorf("new goose provider: %w", err)
	}
	// Up applies all pending migrations under the lock; returns (nil, nil) when
	// nothing is pending (the common case once one pod has migrated).
	if _, err := p.Up(ctx); err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}
	return nil
}

// The DOWN / RESET / STATUS paths stay on the legacy package-level API: they are
// single-operator, out-of-band commands (`converge migrate down|reset|status`),
// never run concurrently by a fleet, so they need no session lock.

func MigrateDown(ctx context.Context, db *sql.DB) error {
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	return goose.DownContext(ctx, db, "migrations")
}

func MigrateReset(ctx context.Context, db *sql.DB) error {
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	return goose.ResetContext(ctx, db, "migrations")
}

func MigrateStatus(ctx context.Context, db *sql.DB) error {
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	return goose.StatusContext(ctx, db, "migrations")
}
