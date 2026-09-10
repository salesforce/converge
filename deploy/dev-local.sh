#!/usr/bin/env bash
#
# dev-local.sh
#
# Launches a local dev Converge fleet of REAL bin/converge
# binaries (separate OS processes — control + worker, exactly like the
# k8s Deployments) against the SAME database setup `just dev-inproc-multipod`
# uses: the primary + async-streaming-replica Postgres pair from the
# TestDevDBOnly testcontainer harness.
#
# Flow:
#   1. Start TestDevDBOnly (go test -dev-db) in the background. It brings
#      up the two Postgres containers and prints PRIMARY_DSN/REPLICA_DSN
#      + DEV_DB_READY, then blocks holding the containers open.
#   2. Parse the DSNs and wait for DEV_DB_READY.
#   3. `converge migrate up` against the primary (idempotent; the
#      control binaries also auto-migrate on boot, this just front-loads it).
#   4. Launch the three tiers: 3 control binaries (ROLE=control, API :8080..:8082),
#      3 broker binaries (ROLE=broker, Connect :9090..:9092, probes :19500..:19502),
#      and 50 dumb worker binaries (bin/worker, no ROLE — they dial a broker via
#      BROKER_ADDR, probes :19000..:19049). UI GETs are routed to the replica via
#      DATABASE_READ_URL; everything else hits the primary. No caps: the broker fans
#      out with its built-in dispatcher default, workers run WORKER_MAX_PARALLEL
#      tasks each, and every kind's kind_config.max_inflight is left at 0 (uncapped).
#   5. Wait. On Ctrl+C / SIGTERM, kill every binary, then stop the DB
#      harness — its t.Cleanup tears down both containers + the network.
#
# The 256 shards are assigned DYNAMICALLY from live cluster membership — no
# static pin. Each binary registers in cluster_members and its
# Resharder tiles [0,256) among the live binaries of its role (3 control, 50
# worker), re-tiling automatically as processes start/stop — the same as
# deploy/converge-deployment.yaml. Kill a worker process and watch the
# survivors pick up its shards; start more and watch them shrink.
#
# Requires: Docker (for the testcontainers), a built ./bin/converge.
# Invoked by `just dev`; set BOM=path to auto-submit a manifest at boot.
#
# ─── ARGUMENTS ────────────────────────────────────────────────────────────────
# None positional. Everything is driven by ENVIRONMENT VARIABLES (below). `just
# dev [bom] [crds]` maps its two positional args to the BOM and KIND_FIXTURES_DIR
# env vars; every other knob is set directly in the environment, e.g.:
#     NUM_WORKERS=10 WORKER_MAX_PARALLEL=16 bash deploy/dev-local.sh
#
# ─── ENVIRONMENT VARIABLES ────────────────────────────────────────────────────
# Fleet shape:
#   NUM_CONTROL          (3)      control pods (ROLE=control): sweepers + HTTP/API.
#   NUM_BROKERS          (3)      broker pods (ROLE=broker): own work_queue tiles,
#                                 serve the Connect BrokerService, run provider stages.
#   NUM_WORKERS          (50)     dumb worker processes (bin/worker): dial a broker,
#                                 pull+run stage tasks, hold NO DB handle.
#   WORKER_MAX_PARALLEL  (100)    per-worker concurrent-task ceiling (= credit it
#                                 advertises to its broker).
#   WORKER_BROKER_DIST   (unset)  comma-list of per-broker worker counts to IMBALANCE
#                                 the fleet and exercise broker↔broker mesh forwarding
#                                 (e.g. "35,8,7"); sum should equal NUM_WORKERS.
#                                 Unset = even round-robin (worker j → broker j%NUM_BROKERS).
#
# Ports (each tier bases at N and increments per pod):
#   CONTROL_API_BASE     (8080)   control APIs on 8080..8080+NUM_CONTROL-1.
#   BROKER_CONNECT_BASE  (9090)   broker Connect listeners on 9090..9090+NUM_BROKERS-1.
#   BROKER_HEALTH_BASE   (19500)  broker /livez /readyz probes on 19500..
#   WORKER_HEALTH_BASE   (19000)  worker probes on 19000.. (NOT the 9000 block —
#                                 macOS/Docker Desktop often hold 9000/9010).
#
# Binaries (override to point at custom builds; a demo passes its own WORKER_BIN):
#   BIN         ($ROOT/bin/converge)  the control+broker binary (no provider code).
#   WORKER_BIN  ($ROOT/bin/worker)    the worker binary (the ONLY one with providers).
#   CONCTL      ($ROOT/bin/conctl)    the CLI used to apply CRDs / BOM.
#
# Workload (both optional; `just dev [bom] [crds]` sets these):
#   BOM                (unset)  path to a resource manifest to auto-submit at boot.
#   KIND_FIXTURES_DIR  (unset)  colon-separated dirs of *.kind.json CRDs to apply
#                               via conctl once the API is up.
#
# TLS / mTLS (all optional; unset = plain HTTP / cleartext Connect):
#   CONTROL_TLS_CERT + CONTROL_TLS_KEY  (unset)  serve the control API over https://
#                               with this SERVER cert (no client-cert verification).
#                               Must be set together. Superseded by MTLS_DIR.
#   MTLS_DIR           (unset)  full-fleet MUTUAL TLS. A dir holding ca.crt /
#                               server.crt / server.key / client.crt / client.key
#                               (generate with the gen_mtls_certs.sh dev helper).
#                               When set, EVERY tier runs mTLS: control API AND
#                               broker listeners serve server.crt over https:// and
#                               RequireAndVerifyClientCert vs ca.crt; workers dial
#                               the broker over https:// presenting client.crt/.key
#                               and pin the broker CA; conctl (CRD/BOM apply)
#                               presents client.crt/.key + trusts ca.crt.
#
# Postgres DSNs are NOT env-configurable here — the TestDevDBOnly harness spins a
# throwaway primary+replica pair and prints their DSNs, which this script parses.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${BIN:-$ROOT/bin/converge}"             # control + broker (NO provider code)
# The worker binary is overridable so a DEMO can point this launcher at its own
# worker (examples/demos/*/cmd/worker → bin/<demo>-worker). Defaults to the
# built-in worker (bin/worker). The control/broker binary is the SAME generic
# core for all.
WORKER_BIN="${WORKER_BIN:-$ROOT/bin/worker}" # dumb worker (the ONLY binary with providers)
NUM_CONTROL="${NUM_CONTROL:-3}"
NUM_WORKERS="${NUM_WORKERS:-50}"
CONTROL_API_BASE="${CONTROL_API_BASE:-8080}"   # control APIs on 8080..8080+N-1

# OPTIONAL control-API TLS. Set CONTROL_TLS_CERT + CONTROL_TLS_KEY (PEM paths) to
# serve the control API over HTTPS (LISTEN_ADDR=https://) instead of plain HTTP —
# e.g. to exercise `conctl --tls-ca … / --insecure` end to end. Unset (default) =
# plain HTTP, unchanged. When set, the "== API ==" URLs below are https://.
CONTROL_TLS_CERT="${CONTROL_TLS_CERT:-}"
CONTROL_TLS_KEY="${CONTROL_TLS_KEY:-}"
if { [ -n "$CONTROL_TLS_CERT" ] && [ -z "$CONTROL_TLS_KEY" ]; } || { [ -z "$CONTROL_TLS_CERT" ] && [ -n "$CONTROL_TLS_KEY" ]; }; then
  echo "CONTROL_TLS_CERT and CONTROL_TLS_KEY must be set together" >&2; exit 1
fi
if [ -n "$CONTROL_TLS_CERT" ]; then CONTROL_SCHEME="https"; else CONTROL_SCHEME="http"; fi

# OPTIONAL full-fleet mTLS. Set MTLS_DIR=<dir> holding ca.crt / server.crt /
# server.key / client.crt / client.key (generate with the gen_mtls_certs.sh dev
# helper). When set, EVERY tier runs mutually-authenticated TLS:
#   - control API + broker listeners serve server.crt over https:// AND
#     RequireAndVerifyClientCert against ca.crt (TLS_CLIENT_CA_FILE) — so a caller
#     must present a client cert signed by the CA.
#   - workers dial the broker over https:// presenting client.crt/.key and pin the
#     broker's server CA (BROKER_CA_FILE=ca.crt).
#   - conctl (CRD/BOM apply) presents client.crt/.key + trusts ca.crt.
# The single CA signs server.crt (serverAuth+clientAuth, so a broker can also act
# as a client to peer brokers in the mesh) and client.crt (clientAuth). Unset =
# no mTLS, unchanged. MTLS_DIR takes precedence over CONTROL_TLS_CERT/KEY.
#
# If MTLS_DIR also holds spiffe-ids.env (gen_mtls_certs.sh writes it, with the certs'
# SPIFFE URI SANs), the fleet ALSO enforces the per-audience SPIFFE allowlists:
# API_/WORKER_AUTHZ_SPIFFE_IDS = the client id, MESH_AUTHZ_SPIFFE_IDS = the server id
# (the identity a broker presents dialing a peer). See docs/tls.md.
#   deploy/gen_mtls_certs.sh bin/dev-certs && MTLS_DIR=bin/dev-certs just dev
MTLS_DIR="${MTLS_DIR:-}"
if [ -n "$MTLS_DIR" ]; then
  for f in ca.crt server.crt server.key client.crt client.key; do
    [ -f "$MTLS_DIR/$f" ] || { echo "MTLS_DIR set but $MTLS_DIR/$f missing" >&2; exit 1; }
  done
  CONTROL_TLS_CERT="$MTLS_DIR/server.crt"; CONTROL_TLS_KEY="$MTLS_DIR/server.key"
  CONTROL_SCHEME="https"
  MTLS_CA="$MTLS_DIR/ca.crt"
  MTLS_SERVER_CRT="$MTLS_DIR/server.crt"; MTLS_SERVER_KEY="$MTLS_DIR/server.key"
  MTLS_CLIENT_CRT="$MTLS_DIR/client.crt"; MTLS_CLIENT_KEY="$MTLS_DIR/client.key"
  echo "--- mTLS ENABLED: control API + broker require+verify client certs vs $MTLS_CA ---"
  # OPTIONAL SPIFFE-ID authz on top of mTLS. If the certs carry SPIFFE URI SANs
  # (gen_mtls_certs.sh writes spiffe-ids.env with CLIENT_SPIFFE), enforce the three
  # per-audience allowlists. The dev fleet shares ONE client cert across conctl +
  # workers + the broker mesh leg, so its SPIFFE id goes in ALL THREE allowlists —
  # the fleet stays functional while every listener runs the SPIFFE enforcement path.
  # (Cross-audience denial is covered by broker.TestSpiffeGatePerServiceAuthz.)
  API_SPIFFE=""; WORKER_SPIFFE=""; MESH_SPIFFE=""
  if [ -f "$MTLS_DIR/spiffe-ids.env" ]; then
    # shellcheck disable=SC1091
    . "$MTLS_DIR/spiffe-ids.env"
    if [ -n "${CLIENT_SPIFFE:-}" ]; then
      # conctl + workers present the CLIENT cert → API + WORKER allowlists.
      API_SPIFFE="$CLIENT_SPIFFE"; WORKER_SPIFFE="$CLIENT_SPIFFE"
      # A broker dialing a PEER broker's mesh presents its SERVER cert (TLS_CERT_FILE,
      # serverAuth+clientAuth), so the MESH allowlist must hold the SERVER id, not the
      # client one. Fall back to the client id if the certs predate spiffe-ids.env.
      MESH_SPIFFE="${SERVER_SPIFFE:-$CLIENT_SPIFFE}"
      echo "--- SPIFFE authz ENABLED: API/WORKER=$CLIENT_SPIFFE MESH=$MESH_SPIFFE ---"
    fi
  fi
else
  MTLS_CA=""; MTLS_SERVER_CRT=""; MTLS_SERVER_KEY=""; MTLS_CLIENT_CRT=""; MTLS_CLIENT_KEY=""
  API_SPIFFE=""; WORKER_SPIFFE=""; MESH_SPIFFE=""
fi

# The ONLY execution model: a CONTROL PLANE + a BROKER/GATEWAY tier on the
# server side, and DUMB client workers. ROLE=broker processes own the
# work_queue shard tiles and serve the BrokerService over Connect; ROLE=worker
# processes are dumb clients (BROKER_ADDR set) that pull stage tasks and NEVER
# touch Postgres. There is no native-SQL worker path. Plain HTTP locally (no TLS)
# — Connect speaks its protocol over HTTP/1.1, so no certs needed.
NUM_BROKERS="${NUM_BROKERS:-3}"               # broker/gateway tier size
BROKER_CONNECT_BASE="${BROKER_CONNECT_BASE:-9090}"  # broker Connect listeners on 9090..9090+C-1
BROKER_HEALTH_BASE="${BROKER_HEALTH_BASE:-19500}" # broker probes on 19500..
# Worker probe ports. Deliberately NOT in the 9000 block: on macOS / Docker
# Desktop ports 9000 and 9010 are frequently held by other services, which
# made exactly worker-0 (:9000) and worker-10 (:9010) fail to bind their probe
# listener. 19000+ is high and uncontended. Override with WORKER_HEALTH_BASE.
WORKER_HEALTH_BASE="${WORKER_HEALTH_BASE:-19000}" # worker probes on 19000..19000+M-1
# Per-worker task concurrency (WORKER_MAX_PARALLEL): how many tasks each worker
# runs at once — also the credit it advertises to its broker. Override to model a
# thinner/fatter fleet; 100 is the dev default (fast local drain).
WORKER_MAX_PARALLEL="${WORKER_MAX_PARALLEL:-100}"

[ -x "$BIN" ] || { echo "missing $BIN — run 'just build' first"; exit 1; }

# ---------------------------------------------------------------------------
# 1+2. Bring up the testcontainer primary+replica and capture the DSNs.
# Run the DB harness with its stdout on a FIFO we read line-by-line until we
# see DEV_DB_READY. -timeout 0 = no test timeout (it blocks until we kill it).
# ---------------------------------------------------------------------------
PIDS=()           # converge binary PIDs
DB_PID=""         # compiled dev-db test binary (testcontainers harness) PID
DRAIN_PID=""      # background `cat "$FIFO"` drainer PID
CRD_PID=""        # background CRD-apply subshell (curl-wait + conctl loop) PID
BOM_PID=""        # background BOM-submit subshell PID
CLEANING=""       # "" → not started, "running" → in progress, "finished" → done
                  # (re-entrancy guard so a 2nd Ctrl+C force-exits, never re-waits)
FIFO="$(mktemp -u)"; mkfifo "$FIFO"

cleanup() {
  case "$CLEANING" in
    # Already finished one teardown — this is just the EXIT trap firing after a
    # signal-triggered cleanup already ran. No-op.
    finished) return 0 ;;
    # A signal arrived WHILE we were tearing down (e.g. a 2nd Ctrl+C). Stop being
    # polite and exit NOW. Without this, a cleanup blocked in a wait just re-ran
    # on the next signal and hung (the original bug: Ctrl+C printed "shutting
    # down" twice and never returned).
    running)
      echo "--- force exit ---"
      kill -9 "$DB_PID" "$DRAIN_PID" "$CRD_PID" "$BOM_PID" "${PIDS[@]:-}" 2>/dev/null || true
      exit 130
      ;;
  esac
  CLEANING="running"
  echo "--- shutting down fleet ---"
  for pid in "${PIDS[@]:-}"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done
  # Backstop: kill any converge binary this script started that escaped
  # the PID list (e.g. if the script itself was SIGKILLed mid-launch). Scoped
  # to THIS binary path so we never touch an unrelated process.
  pkill -f "$BIN" 2>/dev/null || true
  # Stop the FIFO drainer explicitly. It sits in a blocking read until the
  # FIFO's last writer closes; killing it here means the bounded wait below
  # can't hang on it. (An unbounded bare `wait` blocked on this drainer was
  # the original Ctrl+C hang.)
  [ -n "$DRAIN_PID" ] && kill "$DRAIN_PID" 2>/dev/null || true
  # Stop the CRD-apply / BOM-submit background subshells. They loop over
  # curl/sleep (the CRD subshell waits for /livez), so Ctrl+C mid-apply would
  # otherwise leave the subshell — and its blocking `sleep`/`curl` child —
  # running, and the launcher's exit would hang on it (a reparented `sleep 1`
  # keeping the process alive). Kill the subshell AND its current child.
  for p in "$CRD_PID" "$BOM_PID"; do
    [ -n "$p" ] || continue
    pkill -P "$p" 2>/dev/null || true
    kill "$p" 2>/dev/null || true
  done
  # SIGTERM the DB harness. DB_PID is the compiled test BINARY (not `go test`,
  # which wouldn't forward the signal), so this reaches TestDevDBOnly's
  # signal.NotifyContext → it returns and testcontainers t.Cleanup tears the
  # containers + network down. Then a BOUNDED wait, never a bare unbounded `wait`.
  [ -n "$DB_PID" ] && kill "$DB_PID" 2>/dev/null || true
  # Give the harness a few seconds to tear its containers down, then SIGKILL and
  # move on regardless so the script can never hang on a wedged child. The Ryuk
  # sidecar reaps the DB container as a backstop if t.Cleanup didn't finish.
  for _ in $(seq 1 50); do
    [ -n "$DB_PID" ] && kill -0 "$DB_PID" 2>/dev/null || break
    sleep 0.1
  done
  kill -9 "$DB_PID" 2>/dev/null || true
  rm -f "$FIFO"
  CLEANING="finished"
}

# on_signal handles Ctrl+C / SIGTERM: tear down, then exit explicitly so the
# interrupted foreground `wait` can't resume and linger. The EXIT trap re-enters
# cleanup, sees CLEANING=finished, and no-ops — so teardown runs exactly once.
on_signal() { cleanup; exit 130; }
trap on_signal INT TERM
# EXIT runs cleanup once for the NORMAL paths (early-failure `exit 1`, or a
# binary-crash fallthrough), preserving the script's own exit code.
trap cleanup EXIT

# Refuse to launch on top of a previous fleet that's still holding the API /
# health ports — those orphans would make the new pods fail to bind (and the
# API port would silently serve the OLD fleet). Clear them and WAIT until they
# are actually gone: a fixed sleep races (a few of 50 may still be dying when
# the new fleet binds), so poll pgrep until the binary has no live process.
if pgrep -f "$BIN" >/dev/null 2>&1; then
  echo "--- found orphaned converge processes from a prior run; killing them ---"
  pkill -f "$BIN" 2>/dev/null || true
  for _ in $(seq 1 30); do
    pgrep -f "$BIN" >/dev/null 2>&1 || break
    sleep 0.5
  done
  if pgrep -f "$BIN" >/dev/null 2>&1; then
    echo "--- orphans survived SIGTERM; SIGKILL ---"
    pkill -9 -f "$BIN" 2>/dev/null || true
    sleep 1
  fi
fi

echo "--- starting primary+replica Postgres (testcontainers) ---"
# CRITICAL for clean Ctrl+C teardown: run the COMPILED test binary directly, NOT
# `go test`. `go test` does NOT forward SIGINT/SIGTERM to the test process it
# spawns, so a TERM to a `go test` PID would never reach TestDevDBOnly's
# signal.NotifyContext — its t.Cleanup (container teardown) would never run and
# the Postgres containers would leak (only Ryuk eventually reaps them). Compiling
# first and `exec`ing the binary makes DB_PID the actual testcontainers process,
# so the signal lands on its handler and it tears the containers down at once.
DEVDB_BIN="$(mktemp -d)/devdb.test"
( cd "$ROOT" && go test -c -o "$DEVDB_BIN" ./test/ ) || {
  echo "failed to compile the dev-db test binary"; exit 1; }
( exec "$DEVDB_BIN" -test.run '^TestDevDBOnly$' -test.v -test.timeout 0 -dev-db ) >"$FIFO" 2>&1 &
DB_PID=$!

PRIMARY_DSN=""; REPLICA_DSN=""
while IFS= read -r line; do
  echo "[db] $line"
  case "$line" in
    PRIMARY_DSN=*) PRIMARY_DSN="${line#PRIMARY_DSN=}" ;;
    REPLICA_DSN=*) REPLICA_DSN="${line#REPLICA_DSN=}" ;;
    *DEV_DB_READY*) break ;;
  esac
  # If the harness died before READY, bail (cleanup runs via the EXIT trap).
  kill -0 "$DB_PID" 2>/dev/null || { echo "db harness exited early"; exit 1; }
done <"$FIFO"
# Drain remaining harness output so the pipe never blocks. Track its PID so
# cleanup can stop it (otherwise an unbounded `wait` blocked on this drainer).
cat "$FIFO" >/dev/null 2>&1 &
DRAIN_PID=$!

[ -n "$PRIMARY_DSN" ] || { echo "did not capture PRIMARY_DSN"; exit 1; }
echo "--- primary: $PRIMARY_DSN"
echo "--- replica: $REPLICA_DSN"

# ---------------------------------------------------------------------------
# 3. Migrate. The control binaries auto-migrate on boot too, but front-load
# it so a schema error surfaces here, before launching 53 processes.
# ---------------------------------------------------------------------------
echo "--- migrating schema ---"
DATABASE_URL="$PRIMARY_DSN" "$BIN" migrate up

# ---------------------------------------------------------------------------
# 4. Launch the real binaries.
# ---------------------------------------------------------------------------
LOGDIR="$ROOT/bin/dev-logs"; mkdir -p "$LOGDIR"

echo "--- starting $NUM_CONTROL control binaries (ROLE=control) ---"
for ((i=0; i<NUM_CONTROL; i++)); do
  port=$((CONTROL_API_BASE + i))
  # https:// scheme (+ cert/key) when CONTROL_TLS_CERT/KEY are set, else :port
  # plain HTTP. The cert/key env vars are harmless (ignored) when empty.
  if [ "$CONTROL_SCHEME" = "https" ]; then listen="https://:$port"; else listen=":$port"; fi
  ROLE=control \
  DATABASE_URL="$PRIMARY_DSN" \
  DATABASE_READ_URL="$REPLICA_DSN" \
  LISTEN_ADDR="$listen" \
  TLS_CERT_FILE="$CONTROL_TLS_CERT" \
  TLS_KEY_FILE="$CONTROL_TLS_KEY" \
  TLS_CLIENT_CA_FILE="$MTLS_CA" \
  API_AUTHZ_SPIFFE_IDS="$API_SPIFFE" \
  LOG_FORMAT=json LOG_LEVEL=info \
    "$BIN" >"$LOGDIR/control-$i.log" 2>&1 &
  pid=$!
  PIDS+=("$pid")
  echo "  control-$i pid $pid -> $CONTROL_SCHEME://localhost:$port"
done

# The BROKER TIER: the bounded set of pods that own the work_queue shard tiles
# and serve the BrokerService Connect API. They run EVERY provider stage's DB
# reads/writes (compose/work/rollup/delete/operate); the dumb workers below run
# only the pure stage logic over Connect. This IS the execution model now — there
# are no native-SQL worker pods.
echo "--- starting $NUM_BROKERS broker binaries (ROLE=broker, Connect on $BROKER_CONNECT_BASE+) ---"
for ((c=0; c<NUM_BROKERS; c++)); do
  connect_port=$((BROKER_CONNECT_BASE + c))
  chealth=$((BROKER_HEALTH_BASE + c))
  # RELAY_ADVERTISE_ADDR is the dial-able URL PEER brokers use to reach THIS
  # broker (published in cluster_members.Config[connect_addr]). Without it the mesh is
  # SILENTLY OFF: peers can't open a Route to this broker, so a broker with no local
  # worker for a kind can't push that tile's work to a peer that has one — work
  # whose worker landed elsewhere strands. These are host processes, so peers reach
  # each other on localhost:$connect_port. REQUIRED in any multi-broker fleet; k8s sets its
  # own pod-IP form (deploy/converge-deployment.yaml).
  # mTLS: broker listens https:// with the server cert + requires a CA-signed
  # client cert (workers + peer brokers). RELAY_ADVERTISE_ADDR flips to https://
  # so peers dial with TLS; the broker-as-client leg reuses TLS_CERT_FILE (the
  # server cert carries clientAuth EKU) + BROKER_CA_FILE to trust peer servers.
  if [ -n "$MTLS_DIR" ]; then
    broker_listen="https://:$connect_port"; broker_advertise="https://localhost:$connect_port"
    broker_scheme="https"
  else
    broker_listen=":$connect_port"; broker_advertise="http://localhost:$connect_port"
    broker_scheme="http"
  fi
  ROLE=broker \
  DATABASE_URL="$PRIMARY_DSN" \
  BROKER_ADDR_LISTEN="$broker_listen" \
  RELAY_ADVERTISE_ADDR="$broker_advertise" \
  TLS_CERT_FILE="$MTLS_SERVER_CRT" \
  TLS_KEY_FILE="$MTLS_SERVER_KEY" \
  TLS_CLIENT_CA_FILE="$MTLS_CA" \
  BROKER_CA_FILE="$MTLS_CA" \
  WORKER_AUTHZ_SPIFFE_IDS="$WORKER_SPIFFE" \
  MESH_AUTHZ_SPIFFE_IDS="$MESH_SPIFFE" \
  HEALTH_ADDR=":$chealth" \
  LOG_FORMAT=json LOG_LEVEL=info \
    "$BIN" >"$LOGDIR/broker-$c.log" 2>&1 &
  pid=$!
  PIDS+=("$pid")
  echo "  broker-$c pid $pid -> connect $broker_advertise"
done
# No wait before launching workers: a worker retries its boot config-load with
# backoff until its broker answers, so it tolerates being started before the
# broker is up (the same robustness k8s rollout ordering / broker restarts need).

# The WORKERS: dumb clients (bin/worker, the ONLY binary with provider
# code). Each connects to ONE broker via BROKER_ADDR, pulls its default config +
# stage tasks over Connect, runs them, and reports back — it holds NO database handle
# (no DATABASE_URL). It retries the boot config-load with backoff if its broker
# isn't up yet, so it never dies on a startup race.
#
# WORKER→BROKER ASSIGNMENT. Default: round-robin (worker j → broker j%NUM_BROKERS),
# an even spread. Set WORKER_BROKER_DIST to a comma-separated per-broker count to
# IMBALANCE the fleet and exercise broker-to-broker mesh forwarding — e.g.
# WORKER_BROKER_DIST="35,8,7" puts 35 workers on broker-0, 8 on broker-1, 7 on
# broker-2. The counts should sum to NUM_WORKERS (any remainder round-robins).
# Build a worker→broker-index array up front from the distribution (or round-robin).
worker_broker=()
if [ -n "${WORKER_BROKER_DIST:-}" ]; then
  IFS=',' read -ra _dist <<< "$WORKER_BROKER_DIST"
  for bidx in "${!_dist[@]}"; do
    for ((k=0; k<${_dist[$bidx]}; k++)); do worker_broker+=("$bidx"); done
  done
  echo "--- worker distribution across brokers: $WORKER_BROKER_DIST (assigned ${#worker_broker[@]} of $NUM_WORKERS) ---"
fi
echo "--- starting $NUM_WORKERS dumb workers (bin/worker, DB-less) ---"
for ((j=0; j<NUM_WORKERS; j++)); do
  health=$((WORKER_HEALTH_BASE + j))
  if [ "${#worker_broker[@]}" -gt "$j" ]; then
    broker_idx=${worker_broker[$j]}
  else
    broker_idx=$((j % NUM_BROKERS)) # default / remainder: round-robin
  fi
  broker_port=$((BROKER_CONNECT_BASE + broker_idx))
  # mTLS: dial the broker over https:// presenting client.crt/.key (CA-signed, so
  # the broker's RequireAndVerifyClientCert accepts it) and pin the broker's server
  # CA via BROKER_CA_FILE. Plain http:// when mTLS is off.
  if [ -n "$MTLS_DIR" ]; then
    worker_broker_addr="https://localhost:$broker_port"
  else
    worker_broker_addr="http://localhost:$broker_port"
  fi
  # The worker is the ONLY binary with provider code. It holds NO database
  # handle — it pulls its default providerconfig from the broker over Connect
  # (GetProviderConfig) and pulls work over PollWork; BROKER_ADDR is required.
  BROKER_ADDR="$worker_broker_addr" \
  TLS_CERT_FILE="$MTLS_CLIENT_CRT" \
  TLS_KEY_FILE="$MTLS_CLIENT_KEY" \
  BROKER_CA_FILE="$MTLS_CA" \
  HEALTH_ADDR=":$health" \
  WORKER_MAX_PARALLEL="$WORKER_MAX_PARALLEL" \
  LOG_FORMAT=json LOG_LEVEL=info \
    "$WORKER_BIN" >"$LOGDIR/worker-$j.log" 2>&1 &
  PIDS+=($!)
done

echo "--- fleet up: $NUM_CONTROL control + $NUM_BROKERS broker + $NUM_WORKERS worker (${#PIDS[@]} processes) ---"
echo "    API: $CONTROL_SCHEME://localhost:$CONTROL_API_BASE (and the next $((NUM_CONTROL-1)) ports)"
echo "    logs: $LOGDIR/{control,worker}-N.log"

# Optional: apply CRDs (kind manifests) via conctl once the API is up. CRDs are
# applied over the API like everything else. KIND_FIXTURES_DIR is an OS-path-list
# of dirs (colon-separated); every *.kind.json under them is PUT via
# `conctl apply --type manifest`. This is a CLIENT-side step: the value is passed
# to conctl, not to the converge binary. Runs before the BOM submit below (a
# resource needs its kind's manifest to validate).
if [ -n "${KIND_FIXTURES_DIR:-}" ]; then
  CONCTL="${CONCTL:-$ROOT/bin/conctl}"
  api_url="$CONTROL_SCHEME://localhost:$CONTROL_API_BASE"
  # A local https fleet serves a self-signed cert, so skip server-cert verify
  # then. Empty on plain HTTP; a plain string (not an array) so it expands to
  # nothing under `set -u` on bash 3.2 (macOS).
  conctl_tls=""
  if [ -n "$MTLS_DIR" ]; then
    # mTLS: present the client cert (CP RequireAndVerifyClientCert) + trust the CA
    # (verifies the server cert, whose SAN includes localhost). NOT --insecure:
    # --tls-ca does real verification, and --insecure conflicts with it.
    conctl_tls="--tls-cert $MTLS_CLIENT_CRT --tls-key $MTLS_CLIENT_KEY --tls-ca $MTLS_CA"
  elif [ "$CONTROL_SCHEME" = "https" ]; then
    conctl_tls="--insecure"
  fi
  ( for _ in $(seq 1 60); do
      curl -fsSk "$api_url/livez" >/dev/null 2>&1 && break || sleep 1
    done
    echo "--- applying CRDs from $KIND_FIXTURES_DIR via conctl ($api_url) ---"
    IFS=: read -ra _crd_dirs <<< "$KIND_FIXTURES_DIR"
    for _d in "${_crd_dirs[@]}"; do
      for _k in "$_d"/*.kind.json; do
        [ -e "$_k" ] || continue
        "$CONCTL" --server "$api_url" $conctl_tls apply --type manifest -f "$_k" 2>&1 | sed 's/^/[crd] /'
      done
    done
  ) &
  CRD_PID=$!
fi

# Optional: auto-submit a manifest once the API is up. submit-boms round-robins
# across the control API ports — which for `just dev` are CONTROL_API_BASE..
# +N-1 (default 8080..8082), NOT the run-multipod 18080-base default. Pass them
# explicitly via PORTS so the submit hits THIS fleet's API.
if [ -n "${BOM:-}" ]; then
  dev_ports=""
  for ((i=0; i<NUM_CONTROL; i++)); do dev_ports="${dev_ports:+$dev_ports }$((CONTROL_API_BASE + i))"; done
  ( sleep 3
    echo "--- submitting BOM $BOM (ports: $dev_ports) ---"
    if [ -n "$MTLS_DIR" ]; then
      # mTLS: submit via conctl (which presents the client cert), not the raw-curl
      # submit-boms recipe — the CP now RequireAndVerifyClientCert, so plain curl
      # would fail the handshake. conctl apply --type resource is the mTLS BOM path.
      CONCTL="${CONCTL:-$ROOT/bin/conctl}"
      api_url="$CONTROL_SCHEME://localhost:$CONTROL_API_BASE"
      "$CONCTL" --server "$api_url" --tls-cert "$MTLS_CLIENT_CRT" --tls-key "$MTLS_CLIENT_KEY" --tls-ca "$MTLS_CA" \
        apply --type resource -f "$BOM" 2>&1 | sed 's/^/[bom] /'
    else
      # just args are POSITIONAL: submit-boms <count> <ports> <bom>
      just -f "$ROOT/justfile" submit-boms 1 "$dev_ports" "$BOM" 2>&1 | sed 's/^/[bom] /'
    fi
  ) &
  BOM_PID=$!
fi

echo "--- running — Ctrl+C to stop everything (binaries + DB containers) ---"
# Wait on the DB harness; if a binary crashes we keep running (mirrors k8s
# restart semantics loosely — the others keep serving). Ctrl+C hits cleanup.
wait "$DB_PID"
