package db

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var update = flag.Bool("update", false, "overwrite schema.golden.sql with current migration output")

const goldenFile = "schema.golden.sql"

// TestMigrationSchema applies all migrations against a real Postgres instance,
// runs pg_dump inside the container, and compares the result to schema.golden.sql.
//
// Run with -update to regenerate the golden file:
//
//	go test ./db/ -run TestMigrationSchema -update
func TestMigrationSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("schema_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pgContainer.Terminate(context.Background()) })

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	sqlDB, err := sql.Open("pgx", connStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })

	require.NoError(t, Migrate(ctx, sqlDB))

	got := pgDump(t, pgContainer)

	if *update {
		require.NoError(t, os.WriteFile(goldenFile, []byte(got), 0o644))
		t.Logf("wrote %s", goldenFile)
		return
	}

	raw, err := os.ReadFile(goldenFile)
	if os.IsNotExist(err) {
		t.Fatalf("%s does not exist; run with -update to create it", goldenFile)
	}
	require.NoError(t, err)

	want := string(raw)
	if got != want {
		wantLines := strings.Split(want, "\n")
		gotLines := strings.Split(got, "\n")
		var sb strings.Builder
		max := len(wantLines)
		if len(gotLines) > max {
			max = len(gotLines)
		}
		for i := 0; i < max; i++ {
			wl, gl := "", ""
			if i < len(wantLines) {
				wl = wantLines[i]
			}
			if i < len(gotLines) {
				gl = gotLines[i]
			}
			if wl != gl {
				fmt.Fprintf(&sb, "line %d:\n  want: %s\n  got:  %s\n", i+1, wl, gl)
			}
		}
		t.Fatalf("schema drift detected (re-run with -update to accept):\n%s", sb.String())
	}
}

// pgDump runs pg_dump --schema-only inside the container and returns the output
// with volatile header lines stripped so the result is deterministic.
func pgDump(t *testing.T, c *postgres.PostgresContainer) string {
	t.Helper()

	cid := c.GetContainerID()
	cmd := exec.Command("docker", "exec", cid,
		"pg_dump", "-U", "test", "-d", "schema_test",
		"--schema-only", "--no-owner", "--no-acl",
		"--no-comments",
	)
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Run(), "pg_dump failed: %s", stderr.String())

	// Strip lines that vary between runs: SET lines, blank lines at top,
	// goose_db_version table and its sequence (not our schema).
	var kept []string
	skip := false
	for _, line := range strings.Split(out.String(), "\n") {
		// Drop goose internal table block (table + sequence + constraint).
		if strings.Contains(line, "goose_db_version") {
			skip = true
			continue
		}
		if skip {
			if line == "" {
				skip = false
			}
			continue
		}
		// Drop SET statements, pg_dump header comments, and SELECT pg_catalog lines.
		if strings.HasPrefix(line, "SET ") ||
			strings.HasPrefix(line, "SELECT pg_catalog") ||
			strings.HasPrefix(line, "-- Dumped") ||
			strings.HasPrefix(line, "-- PostgreSQL database dump") ||
			strings.HasPrefix(line, "\\restrict") ||
			strings.HasPrefix(line, "\\unrestrict") {
			continue
		}
		kept = append(kept, line)
	}

	// Collapse multiple consecutive blank lines into one.
	var result []string
	blank := false
	for _, line := range kept {
		if strings.TrimSpace(line) == "" {
			if blank {
				continue
			}
			blank = true
		} else {
			blank = false
		}
		result = append(result, line)
	}

	return strings.Join(result, "\n")
}
