package test

// TestDevDBSingle spins up a SINGLE throwaway Postgres testcontainer, applies the
// converge schema (so the tables exist — the same migrations a converge pod runs at
// boot), prints its DSN to stdout in a machine-parseable form, and then blocks
// until SIGINT/SIGTERM. It runs NO converge code itself — the point is a
// zero-setup local Postgres you can point conctl / a hand-run `bin/converge` /
// psql at while iterating, without installing or seeding Postgres yourself.
//
// This is the SINGLE-DB sibling of TestDevDBOnly (which brings up the primary +
// streaming-replica pair `just dev` uses). Use this one when you just want "a
// Postgres + its DSN" and don't need a read replica.
//
// On Ctrl+C the test returns and testing.T.Cleanup tears the container down.
//
// Output contract (the `just dev-db` recipe greps these lines):
//
//	DATABASE_URL=postgres://...
//	DEV_DB_READY
//
// Gated behind -dev-db-single so a vanilla `go test ./test/` skips it. Run via
// `just dev-db`.

import (
	"bufio"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib" // pgx database/sql driver for the migration run
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/db"
)

var devDBSingleEnabled = flag.Bool("dev-db-single", false,
	"Bring up ONE throwaway Postgres (migrated) and print its DSN, then block until Ctrl+C (used by `just dev-db`)")

func TestDevDBSingle(t *testing.T) {
	if !*devDBSingleEnabled {
		t.Skip("single dev db disabled; run via `just dev-db` (sets -dev-db-single)")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// One Postgres container (reuses the package's setupPostgres, which also
	// registers t.Cleanup to Terminate it). We want the DSN, not the pool.
	pool := setupPostgres(t, ctx)
	dsn := pool.Config().ConnString()

	// Apply the converge schema so the tables exist — the same migrations a converge
	// pod runs at boot. Skippable, but a bare DB is rarely what you want here.
	sqlDB, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	defer sqlDB.Close()
	require.NoError(t, db.MigrateAll(ctx, sqlDB), "apply converge schema")

	// Emit the DSN for the recipe to capture, then flush (Go test output is buffered).
	w := bufio.NewWriter(os.Stdout)
	fmt.Fprintf(w, "DATABASE_URL=%s\n", dsn)
	fmt.Fprintln(w, "DEV_DB_READY")
	_ = w.Flush()

	t.Log("dev db up (migrated) — Ctrl+C to tear it down")
	<-ctx.Done()
	t.Log("dev db shutting down")
}
