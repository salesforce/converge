# converge — the control-plane / broker server

`converge` is the server binary of the orchestrator: the **control plane**
(sweepers + the HTTP/OpenAPI API that serves the UI and [conctl](../conctl/))
and/or the **broker** tier (owns a `work_queue` shard tile, claims work, and
fans stage tasks out to workers).

It carries **no provider code** — all provider handlers live in a separate
worker binary (e.g. the demos' `bin/*-worker`), which connects *into* a broker
and pulls work. So `converge` is only ever a control plane and/or a broker,
never a worker.

## Build & run

```sh
just build            # builds bin/converge (+ worker, conctl, embedded UI)
# or:
go build -o bin/converge ./cmd/converge

DATABASE_URL=postgres://… ./bin/converge          # run the server
```

For a local multi-pod fleet (control + brokers + workers + Postgres), use
`just dev` / `just demo`, which wrap [`deploy/dev-local.sh`](../../deploy/dev-local.sh).

## Roles

The `ROLE` env var selects what the process runs (resolved by `parseRole` in
[config.go](config.go)):

| ROLE            | runs |
|-----------------|------|
| `all` (default) | control plane + broker for every kind + API |
| `control`       | control plane only (sweepers + API; no claiming) |
| `broker`        | broker for every kind (owns a shard tile, claims + fans out) |

A broker learns which kinds to claim **live** from `kind_manifest` (the
`KindManifestCache`), not from a compiled-in list, so a CRD applied after boot is
picked up without a restart.

## Configuration

All configuration is via environment variables, parsed with `envconfig` into the
**`Config` struct in [config.go](config.go)** (search `type Config struct`). That
struct is the single source of truth — every knob is a field there with its
`envconfig:"…"` tag, default, and a doc comment explaining it. Rather than
duplicate them here (and risk drift), read the struct; the highlights are:

- **Database** — `DATABASE_URL`, optional read replica `DATABASE_READ_URL`,
  AWS IAM auth `DB_IAM_AUTH`.
- **Listener / TLS** — `LISTEN_ADDR` (`:8080` plain, or `https://…` for TLS),
  `TLS_CERT_FILE` / `TLS_KEY_FILE` (hot-reloaded), optional mTLS via
  `TLS_CLIENT_CA_FILE`, and PER-AUDIENCE SPIFFE-ID allowlists — one per mTLS listener,
  so a worker cert can't reach the API and a peer-broker cert can't pull work:
  `API_AUTHZ_SPIFFE_IDS` (the control API — conctl/UI/CI), `WORKER_AUTHZ_SPIFFE_IDS`
  (the broker's WorkerService — workers), `MESH_AUTHZ_SPIFFE_IDS` (the broker's
  MeshService — peer brokers). `PEER_IDENTITY_SOURCE` (`mtls` default | `mesh-header`)
  selects how the broker derives a connected client's OBSERVED identity for the cluster
  view + "running on" attribution — the client's mTLS cert SPIFFE ID, or a trusted
  service-mesh header (Istio/Linkerd) when a sidecar terminates mTLS; never a client
  self-report (a worker sends no id). See [tlsreload.go](tlsreload.go), and
  [docs/tls.md](../../docs/tls.md) for cert generation + the peer-identity model.
- **Role** — `ROLE` (above): `all` | `control` | `broker`.
- **Broker wiring** — `BROKER_ADDR_LISTEN` (the broker's Connect listener) and
  `RELAY_ADVERTISE_ADDR` (this broker's dial-able URL, so peers can open a mesh
  route to it — required in a multi-broker fleet, so work claimed on a tile whose
  kind has no local worker is pushed to a peer that has one). The per-worker
  concurrency cap (`WORKER_MAX_PARALLEL`) lives on the separate worker binary.
- **Ops** — `LOG_LEVEL`, `LOG_FORMAT`, `HEALTH_ADDR`, `CORS_ALLOWED_ORIGINS`,
  `SHUTDOWN_DRAIN`.

CRDs (kind manifests) are applied over the API (`conctl apply --type manifest` /
`PUT /api/kinds/{kind}/manifest`), not bootstrapped by the server — a fresh
cluster comes up empty.

The client CLI's TLS flags mirror this server's `TLS_*_FILE` scheme — see
[conctl](../conctl/#tls).
