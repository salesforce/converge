# Converge at a glance

A declarative, Kubernetes-style control plane — built for scale and simplicity. Every
capability below is covered in depth elsewhere in the docs.

## Declarative resource model

- **Declarative, K8s-like resources** with schema-validated specs.
- **JSON-Schema-typed kinds** — a kind's spec, status, and config are JSON Schema in its
  manifest; the core validates every apply against it and publishes it at `/docs`
  (OpenAPI). Generate matching Go & TypeScript types from the same schema — one source of
  truth, no drift.
- **Two-axis status** — Synced (reconciled the spec) and Ready (health-ok) collapse into
  one **phase**: Ready, Degraded, Failed, Reconciling, Orphaned, Quarantined, Deleting.
- **Resync / drift engine** — re-observe live health out-of-band, independent of any spec
  change; a drifted resource flips Ready=False and re-reconciles. Runs continuously across
  millions of resources, at **zero cost** for kinds that don't opt in.
- **Spec revisions & rollback** — the last N spec revisions are kept per resource; roll
  back to any earlier one with a single API/CLI call (`POST …/rollback`).
- **Opaque labels** with indexed search.

## Composition & dependencies

- A resource **composes** a whole child graph; the logic is pluggable — Go, CEL, Starlark,
  or any external program (logic-as-data, edited live, no redeploy).
- **First-class dependency edges** with **value flows** — an upstream's output (id, ARN,
  URL) flows into a dependent's spec; the engine gates it until the upstream is ready, and
  re-substitutes on every upstream status change, not just at compose time.
- **Child readiness rolls up** to the root's status — green only when every child is
  healthy, and it names the one that degraded.
- **Reverse-dependency delete cascade** — on teardown, dependents are removed *before* the
  resources they depend on, so nothing is orphaned mid-delete.
- **Imperative operations / verbs** beyond reconcile.

## Reactors & lifecycle side effects

- **Reactors** — durable side effects on a lifecycle transition ("after X, do Y"): an
  emergent saga, with no saga type to write.
- **Editable subscriptions** — a `reactor_bindings(watch_kind, transition, reactor)` row
  wires a reactor to a transition, applied live over the API with no code.
- **Five transitions** to react to — `created`, `synced`, `degraded`, `failed`, `deleted`.
- **At-least-once, deduplicated** — a durable lifecycle outbox delivers each binding's
  reaction at-least-once, fenced against duplicates.

## Simple, agnostic architecture

- **One binary + Postgres** — no etcd, Redis, Kafka, or message broker; SQL & PL/pgSQL do
  the heavy lifting. **Kubernetes is optional, not required** — run it on a single VM, a
  container, or any cloud.
- **Kind-blind core** — the engine knows no provider; a **kind** is a CRD applied to the DB
  plus handler code in a worker.
- **Three roles, one binary** — `converge` runs as **control** (sweepers + API) and/or
  **broker** (the mesh workers pull from); **workers** are separate, DB-free processes.
- **Workers do anything** — Terraform, a shell script, a Kubernetes Job, any cloud/API
  call — over a language-agnostic **Connect** contract.
- **Multi-broker mesh** — a broker that can't serve a kind locally forwards the work to a
  peer broker that can, and the result routes home; scale brokers horizontally.
- **Leaderless auto-sharding** — pods discover each other through cluster membership and
  divide the keyspace dynamically; no leader election, no StatefulSet ordinals.

## Language-free kinds & composers

- **No Go required** — the **stdio** provider makes any external program a kind: task JSON
  in, outcome JSON out.
- **Composers as data** too — a kro-style CEL resource-graph program, or a Starlark
  program, shipped in a config bundle and edited live, no redeploy.
- **Built-in providers**: `stdio`, `stdshell`, `stdterraform`, `stdcel`, `stdstarlark`.
- **Two-tier provider config** — a kind's bootstrap dependency vs. its live tunables are
  split; the effective per-task config is the kind default merged with a per-resource
  override, pushed to workers live.

## Live operability & lifecycle

- **Web-API-style kind versioning** — `vpc/v1` → `vpc/v2`; the core (kind, version) pair
  migrates a resource live: Apply at a different version.
- **Per-kind global concurrency caps** — throttle a kind cluster-wide (e.g. respect a
  cloud API rate limit), **editable live via the API** — the next claim reads the new cap,
  no restart.
- **Per-task deadlines** — a per-kind deadline cancels a wedged task's context and records
  it as a transient failure, so one slow handler can't pin a slot forever.
- **Runtime-editable config** — change a kind's config or CRD live; no redeploy.
- **Soft-delete + reverse-dependency cascade** — finalizers, extended teardown,
  orphan-grace pruning (a configurable grace window with re-adoption), operator quarantine
  (hard-freeze a resource aside from all schedulers).

## Failure handling & resilience

- **Terminal vs. transient failures** — a worker (or the engine) marks a failure terminal
  (won't succeed on retry) or transient (retried with backoff); terminal failures stop
  immediately.
- **Poison-pill dead-lettering** — a transient failure that reaches the kind's
  `max_transient_attempts` is escalated to terminal, so nothing re-queues forever.
- **At-least-once, idempotent** — work is redelivered safely; a claim-epoch fence makes a
  stale, zombie, or misrouted result a no-op.
- **Graceful drain** — on shutdown a worker/broker finishes in-flight work within a budget
  and releases its claims; a worker buffers a computed result across a reconnect rather
  than re-running it.
- **Lease heartbeats** — a live worker attests the tasks it's actively running; a silent or
  wedged worker's lease goes stale and the reaper reclaims and re-dispatches its work.

## Visibility

- **Rich embedded UI**: topology viewer, dependency graph, resource events, and a live
  **cluster/fleet view** — connected pods, their heartbeats, and in-flight work.
- **Powerful label search** — CQRS list & read queries can target a read replica.
- **Reactive, not polled** — work is pushed, so changes propagate immediately; the push
  path coalesces so a burst of enqueues doesn't storm the fleet.

## Security

- **mTLS everywhere** — the API and broker listeners require and verify client certs;
  short-lived TLS material (cert, key, CA) **hot-reloads** on both sides, so a rotation
  needs no restart.
- **SPIFFE-ID authorization, per audience** — three separate allowlists (control API,
  broker WorkerService, broker MeshService) so a worker cert can't reach the API and a
  peer-broker cert can't pull work.
- **Identity is observed, never self-reported** — a connection's identity comes from its
  mTLS cert (or a trusted service-mesh header), not a client-supplied field.
- **Hardened by default** — parameterized SQL only, gateway-validated specs, bounded
  request bodies and lists, tuned security headers on the embedded UI, default-off CORS.

## Deployment, HA & DR

- **Single binary or Kubernetes** — run the binary directly, as a container image, or via
  the Helm chart (PodDisruptionBudgets, IRSA, optional HPA/Ingress).
- **Bring your own Postgres** — self-managed, or a managed service (Amazon RDS/Aurora,
  Google Cloud SQL/AlloyDB, Azure Database). Converge relies on the database for failover
  and promotion; it doesn't reinvent it.
- **AWS IAM database auth** — `DB_IAM_AUTH` mints short-lived IAM tokens per connection
  (RDS/Aurora), so no static DB password is stored.
- **Read-replica offload** — point `DATABASE_READ_URL` at a reader endpoint and UI/list
  reads route there while writes and engine traffic stay on the primary.

## Performance & scale

- **Thousands of reconciliations per second** (provider time excluded).
- **Tuned for millions of resources** on one Postgres primary.

## Testability

- **End-to-end testable in parallel** — with chaos / HA / replica suites.
- **Real resources & policies** via AWS LocalStack.
