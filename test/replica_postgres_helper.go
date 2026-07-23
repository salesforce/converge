package test

// Async-streaming replication helper for stress tests.
//
// Production target is AWS Aurora Postgres, which replicates at the
// storage layer (no WAL replay on readers, ~10ms typical reader lag).
// We can't model Aurora's storage mechanism in a container, so we
// approximate it with vanilla physical streaming replication in async
// mode — matching how the real cluster is configured:
//
//   - synchronous_commit=on + EMPTY synchronous_standby_names: this is
//     what the production Aurora cluster runs. A COMMIT waits only on
//     the LOCAL WAL flush, never on a reader ACK — the secondary (here,
//     the streaming replica; in prod, the cross-region member) is async
//     and the writer never blocks on it. So commit latency is the local
//     durability cost, independent of replica state, same as Aurora.
//   - Real pg_basebackup + standby.signal + hot_standby on the
//     replica: the read DSN really points at a separate server with
//     its own buffer pool, exercising the same connection-pool
//     split the production Aurora reader/writer endpoints exercise.
//
// Limitations vs Aurora the test cannot model:
//
//   - Vanilla streaming uses a single-threaded WAL apply on the
//     replica. Under heavy primary write load the replica falls
//     behind and reads stall on RecoveryConflict* wait events.
//     Aurora doesn't have this — its readers see invalidations from
//     the storage layer, not WAL replay. So under heavy writes the
//     test will look slower than production. That's the cost of
//     using vanilla streaming as a stand-in.
//
// Returns primary + replica DSNs. Caller wires the API server's
// read pool to the replica DSN and everything else to the primary.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moby/moby/api/types/container"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcnet "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// primaryReplica bundles the two DSNs the test needs.
type primaryReplica struct {
	PrimaryDSN string
	ReplicaDSN string
}

// setupReplicatedPostgres provisions a primary + async streaming
// replica pair, configured to mirror the production Aurora cluster:
// max_connections=5000, synchronous_commit=on, 4 GB shared_buffers
// (see commonTuning for the full mapping and the one deliberate RAM
// concession). Both nodes get the same knobs so the replica has the
// same query budget as the primary when serving UI traffic.
//
// Data dir is backed by docker's default storage driver (overlay2 on
// Mac, backed by Docker Desktop's VM disk image). Older versions of
// this helper used a 12 GB tmpfs mount for ~30× faster I/O at 1M-row
// scale, but the 24 GB combined RAM pressure (12 GB × 2 containers)
// caused Docker Desktop OOMs on dev machines. Disk-backed is the
// default for everyone now.
func setupReplicatedPostgres(t *testing.T, ctx context.Context) primaryReplica {
	t.Helper()

	// Shared docker network so the replica can dial the primary by
	// alias 'pgprimary' instead of by IP.
	net, err := tcnet.New(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = net.Remove(context.Background()) })
	netName := net.Name

	// Init script that runs after initdb. Two responsibilities:
	//   1. Create a dedicated 'replicator' role with REPLICATION
	//      privileges. Production setups use a non-superuser
	//      replication role; we mirror that.
	//   2. Append a host-replication line to pg_hba.conf. The
	//      postgres image's POSTGRES_HOST_AUTH_METHOD only emits a
	//      'host all all all' line; a replication client needs its
	//      own 'host replication ...' entry. md5 auth + a real
	//      password is the production pattern, so we use that here
	//      rather than trust on the wire.
	initDir := t.TempDir()
	initScript := `#!/bin/sh
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replicatorpw';
EOSQL
echo "host replication replicator all md5" >> "$PGDATA/pg_hba.conf"
`
	initPath := filepath.Join(initDir, "00-replication.sh")
	require.NoError(t, os.WriteFile(initPath, []byte(initScript), 0o755))

	// Postgres config — mirrors the production Aurora cluster the app runs
	// against (aurora-postgresql 17.4, db.r7g.4xlarge, cluster parameter group
	// hydration…0002), as verified live in that cluster. The intent is fidelity
	// to production, NOT a faster local box: the values below are what Aurora
	// actually reports, so the local stress run exercises the same commit and
	// memory behaviour production sees.
	//
	//   synchronous_commit=on — Aurora runs this and it is a hard durability
	//   requirement. It does NOT pull in the cross-region secondary: Aurora's
	//   synchronous_standby_names is empty and the global secondary replicates
	//   ASYNC at the storage layer, so a COMMIT waits only on the LOCAL 6-way
	//   storage quorum (a few ms), never on a remote-region ACK. We model that
	//   local durability cost by keeping synchronous_commit=on here too.
	//
	//   shared_buffers / effective_cache_size — Aurora auto-sizes these to
	//   ~20 GB on r7g.4xlarge (32 GB box). We can't realistically give a
	//   testcontainer 20 GB × 2 nodes, so we set 4 GB — the ONE deliberate
	//   divergence from prod (RAM-bound is not this workload's bottleneck;
	//   measured I/O was ~0% of exec time, all buffer-cache hits).
	//
	//   work_mem / random_page_cost left at Aurora's effective values — raising
	//   them was measured to give ZERO benefit (the workload is CPU + lock-
	//   bound in drain_outbox_batch, not sort/scan-bound).
	commonTuning := []string{
		// max_connections: Aurora's formula yields ~5000 on r7g.4xlarge. The
		// local `just dev` fleet is 53 processes (3 control + 50 workers), each
		// with a pgx pool of max(4, NumCPU) conns + a LISTEN conn — hundreds of
		// connections. The Postgres default (100) makes the drainer + worker
		// listeners hit "FATAL: too many clients already" (53300) and a BOM
		// stalls, so this is load-bearing; matching Aurora's 5000 is ample.
		"-c", "max_connections=5000",
		// Durability: ON, matching Aurora (and the app's hard requirement).
		"-c", "synchronous_commit=on",
		// Memory: Aurora runs ~20 GB; 4 GB is the deliberate testcontainer
		// concession (see comment above). effective_cache_size tracks it.
		"-c", "shared_buffers=4GB",
		"-c", "effective_cache_size=12GB",
		// Aurora-default-equivalent knobs (raising them measured no benefit).
		"-c", "work_mem=4MB",
		"-c", "maintenance_work_mem=512MB",
		"-c", "random_page_cost=1.1",
		// Observability: Aurora preloads pg_stat_statements + has track_io_timing
		// on by default; mirror that so local profiling matches prod.
		"-c", "shared_preload_libraries=pg_stat_statements",
		"-c", "pg_stat_statements.track=all",
		"-c", "pg_stat_statements.max=10000",
		"-c", "track_io_timing=on",
	}
	// Primary boots WITHOUT synchronous_standby_names. If we set it
	// here, every COMMIT on the primary blocks until a sync standby
	// ACKs — but the standby can't exist until the primary is up
	// enough to accept replication connections. We promote the
	// primary into synchronous mode after the standby has registered,
	// via ALTER SYSTEM + pg_reload_conf below. This mirrors how
	// production typically rolls out sync replication on an
	// already-running primary.
	primaryReplicationCfg := []string{
		// wal_level=replica is the minimum that lets pg_basebackup
		// produce a streamable copy. logical would also work but
		// adds overhead we don't need for physical replication.
		"-c", "wal_level=replica",
		"-c", "max_wal_senders=10",
		"-c", "max_replication_slots=10",
		"-c", "hot_standby=on",
		// wal_keep_size keeps a buffer of WAL on the primary so a
		// briefly-disconnected replica can catch up without falling
		// out of the streaming window.
		"-c", "wal_keep_size=512MB",
	}

	// ── Primary ──
	// Use the official postgres testcontainers module so we keep its
	// init-script / wait-strategy behaviour. trust auth + the network
	// alias 'pgprimary' lets the replica connect without password.
	primaryArgs := append([]string{"postgres"}, commonTuning...)
	primaryArgs = append(primaryArgs, primaryReplicationCfg...)

	pgPrimary, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("orchestrator_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		// Init script runs once after initdb; it creates the
		// replicator role and appends the host-replication entry
		// to pg_hba.conf. The entrypoint then restarts postgres
		// so the new pg_hba.conf is loaded.
		postgres.WithInitScripts(initPath),
		testcontainers.WithExposedPorts("5432/tcp"),
		tcnet.WithNetwork([]string{"pgprimary"}, net),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
		testcontainers.WithCmd(primaryArgs...),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.ShmSize = 4 * 1024 * 1024 * 1024
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pgPrimary.Terminate(context.Background()) })

	primaryDSN, err := pgPrimary.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	// ── Replica ──
	// Plain postgres:16-alpine. We override the entrypoint with a
	// shell that pg_basebackup's from the primary, then exec's
	// postgres. Same tuning knobs, no replication-sender knobs (a
	// hot standby doesn't need them — though they don't hurt and
	// may matter if we promote the replica later).
	replicaArgs := append([]string{"postgres"}, commonTuning...)

	// Entrypoint script:
	//   1. Wait for primary to accept SQL connections.
	//   2. Wipe the data dir (still empty on first start).
	//   3. pg_basebackup with -R so postgresql.auto.conf gets
	//      primary_conninfo + standby.signal is created. -Xs streams
	//      WAL during the backup so the replica is consistent at
	//      end of basebackup, no extra catch-up needed.
	//   4. Patch primary_conninfo to include application_name=replica1
	//      so the standby is identifiable in pg_stat_replication on
	//      the primary. Used by the verifyReplicaIsStreaming sanity
	//      check below.
	//   5. exec postgres with the same tuning args used by the
	//      primary so the replica can serve UI traffic at scale.
	entrypointScript := fmt.Sprintf(`set -eu
export PGPASSWORD=replicatorpw
echo "[replica-init] waiting for primary..."
until pg_isready -h pgprimary -p 5432 -U replicator >/dev/null 2>&1; do
  sleep 0.5
done
echo "[replica-init] primary up; running pg_basebackup"
rm -rf "$PGDATA"/* "$PGDATA"/.* 2>/dev/null || true
pg_basebackup \
  -h pgprimary -p 5432 -U replicator \
  -D "$PGDATA" \
  -Fp -Xs -R -P -v
echo "[replica-init] tagging standby as application_name=replica1"
# pg_basebackup -R writes primary_conninfo into postgresql.auto.conf
# without an application_name. Append one so synchronous_standby_names
# on the primary matches this standby and unblocks synchronous commits.
sed -i "s|^primary_conninfo = '\\(.*\\)'$|primary_conninfo = '\\1 application_name=replica1'|" \
  "$PGDATA/postgresql.auto.conf"
chown -R postgres:postgres "$PGDATA" 2>/dev/null || true
chmod 700 "$PGDATA"
echo "[replica-init] starting postgres as hot standby"
exec su-exec postgres %s
`, shellJoin(replicaArgs))

	pgReplica, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16-alpine",
			ExposedPorts: []string{"5432/tcp"},
			Networks:     []string{netName},
			NetworkAliases: map[string][]string{
				netName: {"pgreplica"},
			},
			Env: map[string]string{
				// PGDATA must match the path we wipe + basebackup into.
				"PGDATA": "/var/lib/postgresql/data",
				// We do NOT set POSTGRES_PASSWORD / POSTGRES_USER /
				// POSTGRES_DB — those would trigger the official
				// entrypoint's initdb path, which conflicts with us
				// owning startup. Our custom Cmd skips initdb entirely.
			},
			// Replace the image's entrypoint so the official
			// docker-entrypoint.sh doesn't try to initdb on top of
			// our basebackup.
			Entrypoint: []string{"/bin/sh", "-c", entrypointScript},
			Cmd:        nil,
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.ShmSize = 4 * 1024 * 1024 * 1024
			},
			WaitingFor: wait.ForLog("database system is ready to accept read-only connections").
				WithStartupTimeout(120 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pgReplica.Terminate(context.Background()) })

	host, err := pgReplica.Host(ctx)
	require.NoError(t, err)
	port, err := pgReplica.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)
	replicaDSN := fmt.Sprintf(
		"postgres://test:test@%s:%s/orchestrator_test?sslmode=disable",
		host, port.Port(),
	)

	// Sanity-check: the replica must be a real hot standby. If it's
	// not, reads will silently hit a writable second primary that
	// doesn't share state with the writer, which would be much worse
	// than a degraded UI.
	verifyReplicaIsHotStandby(t, ctx, replicaDSN)
	verifyReplicaIsStreaming(t, ctx, primaryDSN)

	return primaryReplica{PrimaryDSN: primaryDSN, ReplicaDSN: replicaDSN}
}

// verifyReplicaIsHotStandby connects to replicaDSN and asserts
// pg_is_in_recovery() = true. Catches misconfiguration that would
// otherwise present as silently-stale UI data.
func verifyReplicaIsHotStandby(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()
	var inRecovery bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery))
	require.True(t, inRecovery, "replica is not running as a hot standby")
}

// verifyReplicaIsStreaming waits up to 10s for the standby to show
// up in pg_stat_replication with state='streaming'. We don't require
// sync_state='sync' — this setup runs async streaming, the way
// Aurora-shaped read replicas behave from the writer's perspective
// (the writer doesn't block on reader ACKs at commit time).
func verifyReplicaIsStreaming(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	deadline := time.Now().Add(10 * time.Second)
	for {
		var appName, state string
		err := pool.QueryRow(ctx, `
			SELECT application_name, state
			FROM pg_stat_replication
			WHERE application_name = 'replica1'
			LIMIT 1
		`).Scan(&appName, &state)
		if err == nil && state == "streaming" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("standby did not start streaming within deadline (last err=%v, state=%q)", err, state)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// shellJoin renders a slice of args as a shell-safe space-separated
// string for embedding in the replica entrypoint. None of our args
// contain spaces or quotes, so a plain join would also work — this is
// belt-and-braces in case tuning values pick up shell metacharacters
// later.
func shellJoin(args []string) string {
	out := ""
	for i, a := range args {
		if i > 0 {
			out += " "
		}
		// crude but sufficient for postgres -c key=val tokens
		out += "'" + a + "'"
	}
	return out
}
