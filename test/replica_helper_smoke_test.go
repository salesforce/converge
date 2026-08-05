package test

// Smoke test for setupReplicatedPostgres. Brings up the primary +
// streaming replica pair, writes a row on the primary, and waits for
// it to appear on the replica. Validates that the helper's
// pg_basebackup + standby.signal wiring is actually producing a hot
// standby that streams from the primary.
//
// Gated behind -stress-replica so a vanilla `go test ./test/` skips
// the ~30s container boot.

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

var stressReplica = flag.Bool("stress-replica", false, "Run the streaming-replica smoke test (default: skip)")

func TestStreamingReplica(t *testing.T) {
	if !*stressReplica {
		t.Skip("replica smoke test disabled; run with -args -stress-replica")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dbs := setupReplicatedPostgres(t, ctx)

	primary, err := pgxpool.New(ctx, dbs.PrimaryDSN)
	require.NoError(t, err)
	defer primary.Close()
	replica, err := pgxpool.New(ctx, dbs.ReplicaDSN)
	require.NoError(t, err)
	defer replica.Close()

	// Write on primary.
	_, err = primary.Exec(ctx, `CREATE TABLE smoke (id int PRIMARY KEY, note text)`)
	require.NoError(t, err)
	_, err = primary.Exec(ctx, `INSERT INTO smoke (id, note) VALUES (1, 'hello')`)
	require.NoError(t, err)

	// Read on replica with a short retry loop. Streaming replication
	// is async; the row should land in well under a second on a
	// quiet primary.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var note string
		err := replica.QueryRow(ctx, `SELECT note FROM smoke WHERE id = 1`).Scan(&note)
		if err == nil {
			require.Equal(t, "hello", note)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replica did not stream insert within deadline: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Confirm replica refuses writes — proves it really is a hot
	// standby, not a second writable primary.
	_, err = replica.Exec(ctx, `INSERT INTO smoke (id, note) VALUES (2, 'nope')`)
	require.Error(t, err, "replica must refuse writes")
}
