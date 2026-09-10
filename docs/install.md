# Installing & running converge

This guide covers getting converge running once you have the code. It assumes you
have a **PostgreSQL** database reachable (self-managed Postgres, AWS RDS for
PostgreSQL, or Aurora PostgreSQL) — that is the **only** external dependency.
converge does not use Redis or any other datastore; all state (resources, the work
queue, the outbox, cluster membership) lives in Postgres.

---

## 1. Get the binaries

You can install a prebuilt release or build from source.

### Install a release build (recommended)

The install script detects your OS/arch, downloads the matching release archive,
verifies its checksum, and installs the three binaries onto your `PATH`:

```sh
curl -fsSL https://raw.githubusercontent.com/salesforce/converge/main/install.sh | sh
```

Overrides: `CONVERGE_VERSION=vX.Y.Z` (default: latest), `CONVERGE_BIN_DIR=/path`
(default: `/usr/local/bin`, else `~/.local/bin`), `CONVERGE_BINARIES="conctl"` to
install a subset. Prebuilt archives for **linux** and **darwin** on **amd64** and
**arm64** are attached to each [GitHub Release](https://github.com/salesforce/converge/releases/latest)
— one tarball per platform holds all three binaries, with a `SHA256SUMS` to verify.
To install by hand:

```sh
# e.g. darwin/arm64 — pick the archive matching your platform
VER=v0.1.0; OS=darwin; ARCH=arm64
curl -fsSLO "https://github.com/salesforce/converge/releases/download/${VER}/converge_${VER}_${OS}_${ARCH}.tar.gz"
tar -xzf "converge_${VER}_${OS}_${ARCH}.tar.gz"
sudo install -m 0755 converge conctl stdworker /usr/local/bin/
```

### Build from source

A fresh checkout needs exactly these steps, in order:

```sh
# 0. install the `just` command runner — https://github.com/casey/just  (e.g. `brew install just`)
just setup   # 1. verify the toolchain (Go, just, Node, Docker) + warm the build cache
just build   # 2. compile the UI + all binaries into ./bin
```

`just build` produces:

| Binary | Role | Notes |
|--------|------|-------|
| `bin/converge` | control **and/or** broker server | carries **no** provider code; embeds the UI |
| `bin/stdworker` | the shipped **generic** worker | hosts all five built-in providers — `stdio` (bridge any external program over the stdio protocol), `stdshell` (inline script), `stdterraform` (real Terraform/OpenTofu apply+destroy), `stdcel` (kro-style CEL resource-graph composer), `stdstarlark` (Starlark program composer). Runs all by default; scope with `WORKER_KINDS` |
| `bin/conctl` | CLI | kubectl-style `apply`/`get`/`list`/`delete` over the REST API |

To put the shipped binaries on your `PATH` (into `GOBIN`, else `$(go env GOPATH)/bin`)
instead of `./bin`:

```sh
just install       # go install: converge, stdworker, conctl
```

### Container images

There are **two** images (binaries are built on the host — no Go toolchain runs in
either image):

- **`converge`** — the lean control/broker image carrying `converge` + `conctl`
  on an alpine base. This is the tier that talks to Postgres and serves the API/UI;
  it carries **no** provider code.
- **`converge-stdworker`** — the default worker image (`Dockerfile.stdworker`):
  `stdworker` plus everything the std\* providers shell out to (tofu/terraform via
  tfenv/tofuenv, aws/gcloud/az, git, curl, bash, jq). Separate because it's heavier.
  Bring your own worker image instead if you ship your own providers.

Each release publishes both to **GHCR** (`ghcr.io/salesforce/converge` and
`ghcr.io/salesforce/converge-stdworker`), tagged with the version and `:latest`:

```sh
docker pull ghcr.io/salesforce/converge:latest
docker pull ghcr.io/salesforce/converge-stdworker:latest
# or pin an immutable version tag (recommended for deployments):
docker pull ghcr.io/salesforce/converge:v0.1.0
```

To build them locally instead:

```sh
just docker                                 # control/broker → converge:latest (the `image` variable's default)
just image=myrepo/converge:v1 docker        # custom tag (image= is a variable, so it precedes the recipe)
just docker-stdworker                       # worker → converge-stdworker:latest
just docker-stdworker myrepo/stdworker:v1   # custom tag (positional recipe argument)
```

---

## 2. Point converge at your database

The control and broker tiers need the Postgres **writer** DSN in `DATABASE_URL`.
(Workers are DB-less — they never connect to Postgres.) converge supports several
ways to supply credentials:

> For the optional read-replica DSN (`DATABASE_READ_URL`), running against a
> managed cluster with automatic failover (Aurora / AlloyDB / Cloud SQL / Azure),
> and how converge behaves across a database failover, see
> **[High availability & disaster recovery](ha-dr.md)**.

> **Quickest for local dev — `just dev`.** Skip all the wiring below: `just dev`
> brings up a full local fleet in one command — a throwaway primary + replica
> Postgres (Docker), then 3 control + 3 brokers + 50 workers over Connect, schema
> migrated, API on http://localhost:8080 — and tears it all down on Ctrl+C. Override
> the shape with env vars, e.g. `NUM_CONTROL=1 NUM_BROKERS=1 NUM_WORKERS=2 just dev`.
> Use it for iterating on the engine; the sections below are for running converge for
> real against your own database.

### Local dev: a throwaway Postgres via `just dev-db`

For local iteration you don't need to install or seed Postgres. `just dev-db`
spins up a single throwaway Postgres container (Docker required), applies the
converge schema, prints its DSN, and blocks until Ctrl+C (tearing the container
down on exit):

```sh
just dev-db
# → DATABASE_URL=postgres://test:test@localhost:<random-port>/orchestrator_test?sslmode=disable
#   DEV_DB_READY
```

Copy the printed `DATABASE_URL=` line into another shell and run converge against it:

```sh
export DATABASE_URL='postgres://test:test@localhost:<port>/orchestrator_test?sslmode=disable'
ROLE=all go run ./cmd/converge          # or ./bin/converge
```

The port is random per run. This is a single master (no replica) — for the full
dev fleet (control + brokers + workers over Connect, primary + replica DB) use
`just dev` instead. For anything beyond local dev, use one of the options below.

### a. Full DSN (self-managed Postgres, RDS, or Aurora)

```sh
export DATABASE_URL='postgres://converge:s3cret@your-writer-host:5432/orchestrator?sslmode=require'
```

Use `sslmode=require` (or stronger) against RDS/Aurora when the cluster enforces
TLS (`rds.force_ssl=1`); `sslmode=disable` is fine for a local dev Postgres.

### b. AWS IAM database authentication (RDS / Aurora)

Set `DB_IAM_AUTH=true`. The DSN's **password is ignored** and every connection —
the primary pool, the read-replica pool, and the `migrate` path — is opened with a
short-lived IAM auth token minted from the pod/host's **AWS default credential
chain** (environment variables, shared config/profile, EKS IRSA, or EC2 IMDS). The
30-minute connection recycle transparently picks up rotated credentials, no restart.

The DSN still supplies **host/port/user/dbname**, and TLS is mandatory:

```sh
export DATABASE_URL='postgres://app_user@your-aurora-writer:5432/orchestrator?sslmode=require'
export DB_IAM_AUTH=true
# AWS creds resolved from the default chain (env / ~/.aws / IRSA / IMDS).
```

The database user must be `GRANT`ed `rds_iam`.

### c. Partial DSN + standard libpq sources

converge parses the DSN with `pgx`, which honors the standard libpq environment
variables and `~/.pgpass` for any field the DSN omits. So you can keep secrets out
of `DATABASE_URL`:

```sh
export DATABASE_URL='postgres:///orchestrator?sslmode=require'   # host/user/pass omitted
export PGHOST=your-writer-host
export PGUSER=converge
export PGPASSWORD=s3cret        # or use ~/.pgpass
```

### Optional: read replica

Point the API's UI read traffic at an Aurora reader endpoint (writes and all engine
traffic stay on the primary):

```sh
export DATABASE_READ_URL='postgres://converge:s3cret@your-reader-host:5432/orchestrator?sslmode=require'
```

---

## 3. Run the tiers

converge is one binary run in one of three roles (`ROLE`): `control`, `broker`, or
`all` (control + broker in one process, handy for a single-node dev run). Workers
are the separate `stdworker` (or your own provider) binary and dial a broker.

### Single-node (everything in one process)

```sh
DATABASE_URL=... ROLE=all ./bin/converge          # API on :8080, broker Connect on :9090
BROKER_ADDR=http://localhost:9090 ./bin/stdworker
```

For a full local dev fleet (3 control + 3 brokers + 50 workers over Connect) use
the recipe instead of wiring this by hand:

```sh
just dev
```

### Three-tier (production shape)

```sh
# Control plane — sweepers + drainer + reaper + HTTP API.
DATABASE_URL=... ROLE=control LISTEN_ADDR=:8080 ./bin/converge

# Broker/gateway — owns work_queue shard tiles, claims + fans work out over Connect.
DATABASE_URL=... ROLE=broker BROKER_ADDR_LISTEN=:9090 \
  RELAY_ADVERTISE_ADDR=http://<this-host>:9090 HEALTH_ADDR=:8081 ./bin/converge

# Worker — the only tier with provider code + (if it calls a cloud) an AWS identity.
BROKER_ADDR=http://<broker-host>:9090 HEALTH_ADDR=:8081 \
  WORKER_MAX_PARALLEL=10 ./bin/stdworker
```

**`RELAY_ADVERTISE_ADDR` is required once you run more than one broker.** It is this
broker's own dial-able Connect URL, published so peer brokers can open a mesh route to it
and forward work: a task claimed on a shard tile whose kind has no locally-connected worker
is relayed to a peer broker that has one. With a single broker there are no peers and it is a
no-op; with two or more, a broker that advertises no address is unreachable by its peers, so
work on its tiles can strand. Set it to a per-broker address (e.g. `http://<this-host>:9090`,
or the pod IP in Kubernetes). The Helm chart wires it automatically from the pod IP.

**Spread workers evenly across brokers.** A worker holds ONE long-lived connection to the
`BROKER_ADDR` it dials, so with several brokers you want the worker fleet balanced across
them — an idle broker claims no work its workers could do. Point `BROKER_ADDR` at a
**load-balanced endpoint** that fans connections across the broker pool (a Kubernetes
`Service` / headless DNS round-robin, or an L4 balancer), rather than pinning every worker to
one broker's address. In the Helm chart the workers target the brokers' Service, so this is
automatic; for a hand-rolled deployment, put the brokers behind one balanced address.

Health/readiness probes: control pods serve `/livez` and `/readyz` on the API
port; broker and worker pods serve them on `HEALTH_ADDR`.

**TLS / mTLS / SPIFFE.** The API and broker listeners can require + verify client certs
(`TLS_CERT_FILE`/`TLS_KEY_FILE`/`TLS_CLIENT_CA_FILE`, hot-reloaded) and enforce a
per-audience SPIFFE-ID allowlist on each — `API_AUTHZ_SPIFFE_IDS` (the API),
`WORKER_AUTHZ_SPIFFE_IDS` (the broker's workers), `MESH_AUTHZ_SPIFFE_IDS` (peer brokers) —
so a worker cert can't reach the API and a peer-broker cert can't pull work. See
[docs/tls.md](tls.md) for how the three listeners map to allowlists and how to generate
certs (with the required SPIFFE URI SAN) and test them with `curl`.

---

## 4. Kubernetes (Helm)

A Helm chart deploying all three tiers (with PDBs, IRSA for workers, optional HPA
and Ingress, and the DSN / IAM / existing-Secret credential options above) lives at
[`deploy/helm/converge`](../deploy/helm/converge/) — see its
[README](../deploy/helm/converge/README.md) for the full options, a prod-shaped
install, and a copy-paste **local full-stack** recipe (Docker Desktop k8s, locally
built images):

```sh
helm install converge deploy/helm/converge \
  --namespace converge --create-namespace \
  --set database.url='postgres://user:pass@writer:5432/orchestrator?sslmode=require'
```

The image defaults to the published `ghcr.io/salesforce/converge:latest` — override
`image.repository`/`image.tag` (or pin `image.digest`) only to use your own registry
or a specific version. The chart runs control + broker by default; opt the generic
worker in with `--set stdWorker.enabled=true` (it uses the published
`ghcr.io/salesforce/converge-stdworker`, the heavier tooling image; point
`stdWorker.image.repository` at your own image if you ship your own providers). By
default the worker advertises **all** the std\* kinds; narrow a pool with
`--set stdWorker.kinds=stdterraform,stdshell`.

---

## 5. Configuration reference

Every knob is an environment variable (twelve-factor). For the exhaustive, commented
lists see the two binary READMEs: [`cmd/converge`](../cmd/converge/README.md) for the
server (control/broker) and [`cmd/stdworker`](../cmd/stdworker/README.md) for the worker.

---

## 6. First resources

After the fleet is up, apply kind manifests (CRDs) and providerconfigs over the API,
then submit resources — all by name via `conctl`:

```sh
conctl apply --type manifest -f <kind>.json --server http://<api-host>:8080
conctl apply -f <resource>.json --server http://<api-host>:8080
conctl get resource <kind>/<name> --server http://<api-host>:8080
conctl cluster --workers --server http://<api-host>:8080   # fleet + connected workers
```

See [implementing-a-provider.md](implementing-a-provider.md) for authoring a kind,
and [architecture.md](architecture.md) for how the tiers fit together.
