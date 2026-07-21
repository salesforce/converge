package test

// TestDevDBOnly is the "just dev" database backend: it brings up the
// EXACT same primary + async-streaming-replica Postgres pair that
// TestDevRunMultiPod uses (setupReplicatedPostgres), prints the two
// DSNs to stdout in a machine-parseable form, and then blocks until
// SIGINT/SIGTERM. It runs NO Converge code itself — the point is to
// own only the database lifecycle so `just dev` can launch the real
// bin/converge binaries against this DB, the same database setup the
// in-process multipod harness uses.
//
// On Ctrl+C the test returns and testing.T.Cleanup tears down both
// containers and the docker network, exactly as in the multipod runner.
//
// Output contract (the Makefile greps these two lines):
//
//	PRIMARY_DSN=postgres://...
//	REPLICA_DSN=postgres://...
//
// Gated behind -dev-db so a vanilla `go test ./test/` skips it. Run via
// `just dev`.

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"testing"
)

var devDBEnabled = flag.Bool("dev-db", false, "Bring up the dev-style primary+replica Postgres and print DSNs (used by `just dev`)")

func TestDevDBOnly(t *testing.T) {
	if !*devDBEnabled {
		t.Skip("dev db backend disabled; run via `just dev` (sets -dev-db)")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Same primary + streaming-replica pair the multipod harness uses.
	dbs := setupReplicatedPostgres(t, ctx)

	// Emit the DSNs for the Makefile to capture, then flush so the parent
	// process sees them immediately (Go test output is otherwise buffered).
	w := bufio.NewWriter(os.Stdout)
	fmt.Fprintf(w, "PRIMARY_DSN=%s\n", dbs.PrimaryDSN)
	fmt.Fprintf(w, "REPLICA_DSN=%s\n", dbs.ReplicaDSN)
	fmt.Fprintln(w, "DEV_DB_READY")
	_ = w.Flush()

	t.Log("dev db up — Ctrl+C to tear down both containers")
	<-ctx.Done()
	t.Log("dev db shutting down")
}
