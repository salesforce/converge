# Converge

A declarative, provider-agnostic control plane for resources: describe resources as
specs; the engine reconciles the world to match — composing, ordering, healing, and
rolling up status. Built for scale (millions of resources, thousands of
reconciliations/sec), simplicity (single binary + Postgres), and reactivity (work is
pushed, not polled). Designed to be open-sourced — the core knows no concrete resource
type and must stay generic.

## How we work

- **Author changes through the AI agent — even small ones.** The agent applies these
  conventions uniformly (naming, error wrapping, comment style, file organization, the
  SOLID/security rules below), so code stays consistent regardless of who's driving or how
  large the change is. A one-line fix done by hand tends to drift from the house style; the
  same fix described to the agent doesn't.
- **The developer owns the decision; the agent owns the typing.** When you already know the
  right solution, don't hand-write it — describe the concrete implementation (the approach,
  the files, the edge cases, the trade-off you've decided) to the agent and let it
  implement, then review. You bring the judgment and the design; the agent produces the
  consistent, fully-commented, convention-following code and keeps the docs in sync.
- **Commit messages + PR titles follow Conventional Commits** — a real, documented spec
  (conventionalcommits.org): a `type(scope): summary` prefix, where `type` is one of `feat`
  (a new feature → MINOR bump), `fix` (a bug fix → PATCH bump), `docs`, `refactor`, `test`,
  `perf`, `build`, `chore`; the optional `scope` names the touched area (`reactor`, `broker`,
  `sdk`, `dbq`, …); a `!` after the type/scope or a `BREAKING CHANGE:` footer marks a MAJOR
  bump. We squash-merge, so the PR title BECOMES the commit on `main` and feeds the
  auto-generated release notes — the prefix is what maps a change to its SemVer bump (see
  the release/branching model in `CONTRIBUTING.md`).

## Architecture

- **converge** binary runs as one of three roles: **control** (sweepers, API) and
  **broker** (mesh that dumb workers dial and pull work from); every role runs the
  schema migrations once at boot regardless of role (advisory-locked). **workers**
  are separate binaries — the ONLY ones that carry provider/handler code (`bin/worker`
  is the classic demo's; `bin/stdworker` is the shipped default, hosting the built-in
  `std*` providers). **conctl** is the kubectl-style CLI over the REST API.
- **Workers are built on the `sdk-go/converge` CLIENT SDK** — a cloud-SDK-shaped client, not
  a framework. It has a TWO-TIER public surface, the AWS-SDK shape (a high-level
  batteries-included entrypoint over a low-level client-with-options): the HIGH tier is
  `converge.Serve(ctx, []converge.Provider)`; the LOW tier is `converge.RunWorker(ctx,
  client converge.Transport, providers, converge.RunOptions)` for a dev who wants to build
  the transport + options themselves. Both funnel through the same WorkStream loop; the
  author contract is the same `converge.Provider` interface + the types it references
  (`KindVersion`, `ProviderConfig`) + the value types a handler reads/writes
  (`Resource`/`Outcome`/`ReactionRequest`/…). `Serve` reads config from the environment
  (env-ONLY: BROKER_ADDR, TLS_CERT_FILE/TLS_KEY_FILE/BROKER_CA_FILE + TLS_RELOAD_INTERVAL,
  WORKER_MAX_PARALLEL, WORKER_DRAIN_TIMEOUT, WORKER_KINDS filter, HEALTH_ADDR probes,
  LOG_*), OWNS the whole broker relationship (dialing h2c or mTLS with cert hot-reload,
  reconnect-with-backoff, config prime/refresh, the readiness poller, graceful drain), and
  BLOCKS until ctx ends. A worker main is: build a signal ctx, `converge.Serve(ctx,
  providers)`; the CALLER owns the signal ctx + exit code (Serve touches no signals and
  never `os.Exit`s, so it embeds cleanly). `ExecuteTask(ctx, providers, *workerpb.StageTask)`
  is the SDK's in-process single-task codec seam — used by the test harness's in-process
  executor to run a task through the REAL proto decode→Work→encode without a broker; a
  shipped worker never calls it. There is exactly ONE provider type —
  **`converge.Provider`** = `Kind() KindVersion` / `Work` / `OnConfig(ProviderConfig)` /
  `Ready() bool` — and the SDK calls ONLY these, NEVER a setup step: a provider dials its
  OWN downstreams on its own schedule and reports readiness through `Ready` (that IS the
  bring-up contract). ONE `converge.Provider` serves ONE `(kind, version)` (so
  Work/OnConfig/Ready take no kind/version arg — the pair is fixed by `Kind`); a worker
  serving several pairs lists one per pair in the slice passed to `Serve`, and a provider
  that owns a set of pairs (a shared backend, or an env-driven set like `stdio`) exposes a
  `[]converge.Provider` constructor spliced into that slice (see `networking.New`,
  `stdio.Providers`). A `converge.Provider` runs on the dumb worker (Work over the
  WorkStream); the SAME provider's Work is also exercised in the integration test harness
  WITHOUT a broker, via a test-only in-process executor (`test/internal/inproc`) injected
  into the dispatcher's StageDispatcher seam. That executor is NOT a hand-written type
  bridge: it encodes the stage to a proto `StageTask` (via `internal/wire`), runs it through
  the SDK's OWN `converge.ExecuteTask` (the same decode→Work→encode a shipped worker does per
  task), and decodes the `StageComplete` back — so the test path and the Connect path share
  ONE proto codec, no dual dispatch world. There is NO in-process executor in a shipped
  binary — production runs exactly ONE dispatcher, the broker's remote fanout over Connect.
  There is no `framework.Provider`/`Setup`/`Bootstrap` — those were deleted.
- **The ONLY tissue between a worker (in ANY language) and a broker is the PUBLIC
  worker proto.** `proto/converge/worker/v1/worker.proto` (`WorkerService` +
  WorkStream/GetProviderConfig + the task/result messages) generates into
  `sdk-go/workerpb` — the single cross-language contract every SDK (Go, and future
  TS/Java) wraps. The internal broker↔broker mesh is a SEPARATE proto,
  `proto/converge/mesh/v1/mesh.proto` (`MeshService` + Route: a single bidi
  push-routing stream), generated into `internal/meshpb` so no SDK can reach it: the
  mesh may evolve (new frames, new routing) WITHOUT regenerating the public worker
  contract. The dependency is one-way (mesh imports worker to reuse
  StageTask/Interest byte-for-byte; worker never references mesh). An archtest guard
  (`TestSDKSpeaksOnlyWorkerProto`) fails the build if `sdk-go/` ever imports `meshpb`.
- **Package layout: `sdk-go/` is the worker CLIENT (self-contained), `internal/` is the
  core, `pkg/` is ONLY shared leaf utils.** The Go SDK lives under `sdk-go/` — `converge`
  (its OWN contract types `types.go`, proto↔types converter `convert.go`, the `Serve` +
  low-level `RunWorker` runner, and the folded WorkStream loop) + its OWN generated worker
  proto `sdk-go/workerpb` — mirroring the TypeScript SDK under `sdk-ts/` (`src/` + `gen/`).
  Every language binding owns its wire stubs AND its ergonomic types; the ONLY thing shared
  with the broker is `proto/converge/worker/v1/worker.proto`. The control plane
  (`internal/*`, `cmd/converge`) has its OWN twin of that contract — `internal/model` holds
  ALL the core-side types (kind identity, the reaction request/response + composition value
  types, the Trigger/Transition enums, and the CRD-style `KindManifest`), and `internal/wire`
  is the core-side proto↔model converter — which the SDK NEVER imports. The two type worlds
  meet ONLY at the proto: the broker's `internal/wire` and the SDK's `convert.go` both
  encode/decode `workerpb`. `pkg/` holds ONLY the genuinely-shared, domain-free leaf utils
  both sides use: `wirelimits`, `drainctx`, `tlsreload`. A worker author imports
  `sdk-go/converge`; nothing else. The archtest boundary guards (`internal/archtest`) fail
  the build if `sdk-go/`→`internal/` (incl. `internal/model` + `internal/wire` —
  `TestSDKSharesNoCoreTypes`), if `sdk-go/` reaches the mesh proto, or if the core reaches
  into a provider. See [[project_sdk_proto_only_tissue]].
- A **kind** is taught by applying a CRD-style manifest (to the `kind_manifest` DB table,
  operator-applied out of band) plus handler code in a worker.
- **Postgres is the substrate.** Workers claim `work_queue` rows with
  `FOR UPDATE SKIP LOCKED`; graph/rollup/value-flow logic lives in PL/pgSQL, not the app.

## Design (SOLID — this is a library-grade, open-source core)

The core must stay generic and testable. Apply SOLID as a default, not an afterthought:

- **DIP — depend on interfaces, construct at the root.** `store.New` (and every
  concrete constructor) is called ONLY at the composition root (`cmd/converge`, or a
  package's one construction helper — `api.DepsFromPools`, the duty factories, the
  `New*` constructors). A component holds the narrow ROLE INTERFACE it uses (e.g.
  `ReaperRepo`, `DispatcherRepo`, `api.Repo`), never a `*pgxpool.Pool` or `*store.Store`.
  A raw `*pgxpool.Pool` field on a logic struct is a smell — inject the capability
  (`runtime.Listener` for LISTEN, `txBeginner` for a tx) instead. The pool lives only on
  wiring carriers (`Deps`, `*Config`) that exist to build the interfaces.
- **ISP — narrow, composable interfaces.** Prefer several small role interfaces over one
  fat one; a fat interface is fine ONLY as a `type X interface { RoleA; RoleB }`
  composition of them, so a consumer can depend on the one role it needs (see
  `api.Repo`, `engine.dispatchSurface`).
- **OCP — register, don't switch.** Adding a kind/stage/duty/route should add an entry
  to a table/registry, not edit a central `switch` or type-assert a concrete type. See
  the broker fanout's stage dispatch and `engine.HandlerProvider` (ranged for, not asserted).
- **SRP — one reason to change per type/file.** Split god-objects into cohesive
  collaborators (e.g. `api.schemaRegistry` out of `Server`; `providerConfigSnapshot` —
  serving — out of `ProviderConfigCache` — refresh). But do NOT fragment genuine cohesion: a bundle that
  shares one lifecycle/lock/shard-range (e.g. the `ControlPlane` sweepers) stays one unit.
- **LSP** — an implementation must honor its interface's contract (respect `ctx`, no
  panic-on-method, uniform sentinel handling).
- Every interface a concrete type must satisfy gets a compile-time guard:
  `var _ Iface = (*T)(nil)` — drift fails the build at the boundary, not at a call site.
- Judgment over dogma: when the idiomatic-Go / cost-vs-benefit call is to leave something
  (a documented process-wide singleton, a per-process wide struct), leave it and say why
  in a comment describing the CURRENT design.

## Code organization

- **One concern per file; group by responsibility, not by accident.** A file should be
  navigable from its name. When a file becomes a catch-all of unrelated functions, split
  it along its concerns (one type / one duty / one role per file). Reference layout:
  `internal/engine` — `duty.go` (interfaces), `control_duty.go` / `claim_duty.go` (one
  duty each), `config.go` (config structs + resolver), `engine.go` (the Engine + its
  forwarders), `shard_verify.go` (the boot assert). Do the same everywhere; don't let
  functions scatter across files.
- **Comment exhaustively; keep it truthful and current.** Every exported symbol and every
  non-obvious block gets a doc comment that explains WHAT it is and WHY it's shaped that way
  (the trade-off, the invariant, the gotcha) — this codebase is dense and the comments are
  load-bearing. But comments describe what the code IS, not how it got here: NO
  refactor/history narration ("was X", "no longer", "split out of", "instead of the old",
  "SOLID/audit"). There is one reader — that history lives in git, not in the source. Design
  RATIONALE (why this shape) is welcome; change LOG is not.
- **No doc drift — code and docs move together.** A change that alters behavior, a field, a
  flag, an endpoint, a recipe, or a file's role MUST update every comment/doc that describes
  it IN THE SAME CHANGE: the local doc comment, package doc, `README.md`, `docs/*`, the Helm
  `values.yaml`/`README.md`, and this file. A stale comment is a bug. When you touch a
  symbol, re-read its comment and the docs that reference it; if they no longer match, fix
  them.
- **No magic literals — use named consts / typed enums.** A string or number that carries
  MEANING is NEVER written as a bare literal at a use site. It gets a named `const`,
  referenced everywhere. This is a hard rule: `if phase == "Failed"` is wrong —
  `if phase == phaseFailed` is right; `Interval: 30 * time.Second` inline is wrong when it's
  a policy default — name it (`DefaultSweeperInterval`). A set of related values is a TYPED
  enum, not loose strings:

  ```go
  type Phase string
  const (
      PhaseReady       Phase = "Ready"
      PhaseReconciling Phase = "Reconciling"
      // …
  )
  ```

  so the compiler + `exhaustive` linter catch a missed case and a typo can't compile. Only
  a value with NO reuse and NO meaning beyond the one line (a loop bound, a test fixture) may
  be inline. Existing anchors to follow: resource phases (`internal/api/phases.go`, tied to
  the schema's GENERATED `phase` column), sweeper cadence defaults (`engine.Default*`), the
  `dbq.*SQL` query constants, wire enums (`workerpb.Stage_*`). When a Go const mirrors a
  value defined elsewhere (schema/proto), say so in a comment so the two are known to be
  linked and kept in sync.

## Commands

`just` (no args) lists every recipe; `just setup` verifies a new dev's environment.
The only local prerequisites are Go, `just`, node (embedded UI), Docker (integration
tests), and `jq` (demos). Dev tools (sqlc/buf/golangci-lint/oapi-codegen) are run via
pinned `go run @version`, not installed.

- `just gen` — regenerate ALL generated code (sqlc → `internal/dbq`, proto →
  `sdk-go/workerpb` (public worker contract) + `internal/meshpb` (broker↔broker mesh),
  goldens, conctl client). Run after any query/proto/schema/API change.
- `just build` — build converge + workers + conctl (embeds the UI).
- `just test` — integration suite + chaos + HA + replica (starts a Postgres
  testcontainer). Run tests in the BACKGROUND so questions can be answered in parallel.
- `just fmt` — format hand-written Go (gofmt + import grouping/pruning) via pinned
  goimports; excludes generated code. Run before committing.
- `just lint` — pinned golangci-lint.
- `just vuln` — scan for known vulnerabilities via pinned govulncheck (source-reachable
  only; exits non-zero on a finding, so it doubles as a CI gate).
- `just sqlc` / `just proto` / `just golden` — regenerate one artifact.
- `just dev` / `just dev-inproc*` — local fleets (real Connect binaries / in-process).
- `just dev-db` — one throwaway migrated Postgres, prints its DSN.

## Conventions

- **Migrations:** until the first stable release, there is ONE migration file,
  `db/migrations/00001_schema.sql`, kept as a single file for simplicity. Change tables
  in place (incremental `ALTER TABLE` / `ADD/DROP COLUMN`); NEVER add a second migration
  file. `db/schema.golden.sql` is the final schema — regenerate via `just golden-schema`
  (`migrations_test.go`). Every pod runs the migrations automatically at boot
  (advisory-locked, so concurrent pods are safe) — a normal deploy needs no manual
  step; a `converge migrate up|down|reset|status` subcommand exists for out-of-band
  operator use but is not required on the startup path.
- **SQL queries:** ALL SQL lives in the query layer. Put a new query in `db/queries` and
  run `just sqlc`; if sqlc can't express it (RETURNS TABLE, `int4range`, dynamic
  filters), hand-write it as a `dbq.*SQL` constant in `internal/dbq/*_manual.go`. NEVER
  inline a SQL string literal at a `s.db.Query/QueryRow/Exec` call site in the store (or
  anywhere). The store is the sole importer of `dbq`. (Only non-data-layer exceptions:
  PG catalog introspection for a boot assert, and `LISTEN <channel>`.)
- **REST & proto discipline:** the HTTP API follows REST conventions — plural-noun
  resources, kebab-case multi-word path segments, the METHOD is the verb (GET reads,
  POST creates-in-a-collection / runs an action, PUT idempotently replaces a
  URL-addressed object, DELETE removes; PATCH only for genuine partial updates).
  IDENTITY lives in the PATH, filters/pagination/projection in QUERY params, the
  representation in the BODY — a value that selects WHICH resource is a path segment,
  never a required query param; a value that shapes a LIST is a query param. Status
  codes carry the outcome (201+`Location` on create vs 200 on update, 202 for
  async/queued, 204 for empty success, 400 malformed vs 422 well-formed-but-invalid,
  404 vs 409-conflict) and errors are RFC-7807 `application/problem+json`. One concept
  is named + placed identically across every endpoint. The proto contracts
  (`worker.proto`, `mesh.proto`) number fields from 1 with no reserved/deprecated
  gaps. **Until the FIRST STABLE RELEASE there are NO compatibility guarantees on
  either wire (the REST API or the protos): there is no external user, so prefer the
  CORRECT shape over compatibility — freely rename a field, renumber a proto tag,
  change a path/method/status, or drop an endpoint when it makes the contract right,
  rather than carrying a shim or a reserved gap.** (This REVERSES post-stable: a
  breaking change then bumps the API version / ships as a new field number and the
  old shape is retained.) A change that isn't clearly correct here is a bug; fix the
  contract, don't paper over it.
- **Config structs:** group fields by functionality, then sort alphabetically.
- **Tidiness:** remove dead/unused code; keep comments, docs, and the README current —
  no stale references to the pre-refactor system.
- **Verify before done:** `just fmt` + `just build` + `just lint` + `just gen` (no drift) must be
  clean; run the affected tests. State outcomes plainly — if a test flakes (e.g. the
  testcontainer `port "5432/tcp" not found` startup race), re-run it in isolation to
  confirm it's infra, not the change.

## Performance

Performance is not optional — nothing may get slower. Validate every new SQL pass at 1M
scale (the target is 10M+), not just integration tests.

- Push graph/resource/rollup logic into PL/pgSQL to avoid app roundtrips and keep data in
  the DB. Keep it simple; avoid complexity unless speed/resiliency/correctness clearly
  benefits.
- Avoid JSON marshal/unmarshal over thousands of objects; prefer native types.
- Use goroutines (a worker pool where needed) for independently-computable work.
- Pass `context.Context` to anything that can block (I/O especially); support
  cancellation and deadlines.

## Concurrency & correctness (critical for an orchestrator)

- Make claims and state transitions idempotent and at-least-once safe — work may be
  redelivered; never assume exactly-once delivery.
- Use `SELECT ... FOR UPDATE SKIP LOCKED` for queue claims; document lock ordering to
  avoid deadlocks.
- Keep transactions short; NEVER hold one open across an external call (provider or API
  roundtrip).

## Security

Treat every external input as hostile. This is a control plane with real blast radius.

- **SQL injection — parameterize, always.** All queries go through sqlc or a `dbq.*SQL`
  constant with bind parameters (`$1`, `$2`, …); NEVER build SQL by concatenating or
  `fmt.Sprintf`-ing a caller-supplied VALUE. Dynamic SQL (the topology filters) may only
  `fmt.Sprintf` a WHITELISTED fragment / a fixed placeholder count — values stay bind
  params. A user string reaching a query as anything but a bind parameter is a bug.
- **HTTP / output injection.** JSON responses are `json.Marshal`ed (auto-escaped); never
  hand-concatenate JSON or HTML. The embedded SPA is hardened by `securityHeadersMiddleware`
  (nosniff, `X-Frame-Options: DENY`, a tuned Content-Security-Policy) — keep those on every
  response; don't widen the CSP without cause. CORS is default-off (same-origin);
  `CORS_ALLOWED_ORIGINS` opts specific origins in, never reflect an arbitrary Origin.
- **Validate at the gateway.** Specs are schema-validated at the data boundary (the store's
  validator), and kind/name/version are checked before use — don't trust a resource shape
  because it "came from the API." Bound every list/page (`LIMIT`), cap request bodies
  (`bigBody`), and rate-limit when configured (`API_RATE_LIMIT_*`).
- **AuthN/AuthZ is mTLS + SPIFFE.** The API + broker listeners do
  `RequireAndVerifyClientCert` against a client-CA and (optionally) enforce a SPIFFE-ID
  allowlist — that mutual-TLS handshake is the ONLY authn; don't add a parallel trust path.
  The allowlist is PER-AUDIENCE: three separate lists, one per mTLS listener, so a worker
  cert can't reach the API and a peer-broker cert can't pull work — `API_AUTHZ_SPIFFE_IDS`
  (control API), `WORKER_AUTHZ_SPIFFE_IDS` (broker WorkerService), `MESH_AUTHZ_SPIFFE_IDS`
  (broker MeshService, peer brokers); the broker's two services share one listener whose
  handshake admits the union and a per-service gate (`pkg/spiffeauthz`) narrows each RPC.
  The broker NEVER trusts a client-reported id — a worker's `Subscribe` carries no id
  field. The connected-client identity (cluster view + "running on" attribution, both
  display-only; `broker_id` is advisory — the RESULT-WRITE fence is `claim_epoch`, see
  below) is OBSERVED from the connection: the mTLS cert's SPIFFE ID, or a trusted
  service-mesh header
  (`PEER_IDENTITY_SOURCE=mesh-header` for Istio/Linkerd), else the peer IP — never a
  self-report. See [[project_sdk_proto_only_tissue]] and docs/tls.md.
- **Secrets & least privilege.** Never log or error-wrap a secret (cert key, DB password,
  provider credential); short-lived certs hot-reload (`pkg/tlsreload`). Prefer IAM/workload
  identity (`DB_IAM_AUTH`, SPIFFE) over static creds. A provider bundle is opaque
  attacker-controllable data — handlers run it sandboxed on the worker, never on control/
  broker.
- **Fail closed.** A missing/invalid manifest, an unversioned kind, a failed validation →
  reject (422 / terminal fail), never a silent default that could mis-route or mis-apply.
- **Dependency hygiene.** Run `just vuln` (pinned govulncheck, source-reachable analysis)
  before a release and in CI — it exits non-zero on a reachable vulnerability. When it
  flags a stdlib/toolchain CVE, bump the Go `toolchain` in `go.mod`; for a module, update
  it. Don't add a dependency the core doesn't need — the open-source core stays lean.

## Error handling & resilience

- No silent failures. Wrap errors with context (`fmt.Errorf("...: %w", err)`); never
  discard a returned error.
- External calls (providers, TLS, k8s) need timeouts, retries with backoff, and
  cancellation via `context.Context`.
- **Reconnect/retry against a shared dependency uses EXPONENTIAL BACKOFF + JITTER**
  (capped, ctx-cancellable). A whole fleet loses a broker at once, so an un-jittered
  backoff reconnects in lockstep — a thundering herd on the recovered listener. Jitter
  the sleep (shared `pkg/backoff.Jitter`), escalate on the raw value, reset only after a
  genuinely healthy session (see the `sdk-go/converge` reconnect loop + the
  `sdk-go/converge` boot config-load loop).
- **Per-kind worker READINESS is advisory, over the WorkStream, never a fence.** A
  worker hosts many providers over ONE stream; when one provider's downstream degrades
  it advertises an `Interest` RS- (`has_worker=false`) for just that `(kind,
  kind_version)` so the broker stops CLAIMING + PUSHING that kind (its rows park for a
  healthy worker) while the stream's other kinds keep flowing; RS+ resumes it. Driven by
  `converge.Provider.Ready` (the SDK polls it → `remoteworker` `SendReadiness` on an edge,
  `InitialUnready` in the Subscribe burst so a boot-degraded kind gets zero tasks); the
  broker keys its claim gate on `readySubscribers`, not mere connection. A health outage
  NEVER poison-pills: an in-flight task finishes, a race-window arrival fails TRANSIENT
  (re-dispatched elsewhere), never terminal. The claim/fence steer independently of
  readiness; it only steers where work lands. Reuses the mesh's RS+/RS- `Interest`, so an
  RS- retracts the mesh advertisement too (peers stop forwarding the degraded kind).
- **The result-write fence is a monotonic `claim_epoch`, owner-independent.** Claim
  exclusivity is `FOR UPDATE SKIP LOCKED` + the `broker_id IS NULL` claim-flag — the
  identity VALUE fences nothing. The RESULT write (`AppendOutbox`, `StampComposedGen`) and
  the reactor ack (`ack_reactor_delivery`) fence on `work_queue.claim_epoch` /
  `lifecycle_outbox.claim_epoch`: a strict monotonic per-row token bumped by ONE in the
  SAME statement that (re)makes the row claimable (the claim, `schedule_eligible`'s re-point,
  `reap_stale_work`, a scoped release, a reactor claim/reap). It starts at 1 (never 0) so a
  fresh row is already fenced — the sole ownership gate for operate/delete, which bypass the
  generation gate. The claim stamps it, the `StageTask` carries it, the worker echoes it on
  `StageComplete`, and the write lands only if the row still bears that exact epoch — so a
  stale/zombie/misrouted result no-ops. Because the epoch dominates identity, a result is
  no longer bound to the ORIGINAL claiming broker: a worker that RECONNECTS to a different
  broker redelivers its buffered `StageComplete` there and that broker writes it (the SDK
  BUFFERS a computed result across a stream drop, never re-runs it). A mesh-forwarded task's
  result is RETURNED to the owning broker over the Route stream (a `complete` frame) — the
  owner holds the parked stage + the durable write path — and the epoch fence makes a
  stale/dup return inert. `generation` + `manifest_version` stay as the reconcile-only
  staleness axes (belt-and-suspenders with the epoch).
- **The lease heartbeat attests live-WORKER execution, worker-ATTESTED.** A worker sends
  ONE coalesced `WorkHeartbeat` per tick UP the WorkStream listing the tasks whose handlers
  are PROGRESSING (a handler marks progress via `Env.Heartbeat`); the broker relays those to
  its dispatcher (`Attest`), which refreshes `work_queue.heartbeat_at` (epoch-fenced) ONLY
  for attested tasks. A silent or wedged worker on a live connection stops attesting, so its
  lease goes stale within `RequireWithin` (default 15s, ≤ half the reaper `StaleAfter`) and
  the reaper reclaims + re-dispatches it — heartbeat_at means "a live worker is executing
  this", not "the broker is up". A clean worker-stream drop still RELEASES the row in ~1ms
  (`abandon`+`releaseAbandoned`); the attestation window is the backstop for a SILENT hang. A
  reconnecting worker re-advertises its still-live tasks (`Subscribe.resumed`) so the broker
  it lands on adopts their liveness. There is no down-direction keepalive.
- **A providerconfig is a MONOLITH of `{spec (json), data (bytes)}`, pushed WHOLE.** The
  broker pushes one `ProviderConfigUpdate` (spec + bundle together) — symmetric with the
  `GetProviderConfig` pull — never two independent frames; the ProviderConfigCache's single
  `OnKindConfigChange(spec, data)` fires on either axis. `converge.Provider.OnConfig`
  receives the full `ProviderConfig{Spec, Data}` — the kind's DEFAULT only (a change
  NOTIFICATION; empty `{nil,nil}` = the default was deleted), NOT a per-resource override.
  The SDK PRIMES that default at startup + re-pulls on reconnect, so `Work` sees it from
  task one even before any push. The EFFECTIVE config per task is `default ⊕ the resource's
  per-resource override`, and that merge happens IN `Work` (the override rides the task in
  `req.Env.ProviderConfig`/`req.Env.ProviderBundle`), never in `OnConfig`: `spec`
  deep-merges via `converge.EffectiveConfig` (override wins per key), the
  `data` bundle whole-replaces via `converge.EffectiveBundle` (opaque bytes can't
  deep-merge). Base64 is ONLY the public/REST-API transport for the `data` bytes; on the
  worker wire and in the SDK it is raw bytes.
- **Short-lived TLS material hot-reloads** on BOTH sides of the handshake (poll +
  fingerprint + atomic swap) so a cert rotation needs no restart — shared
  `pkg/tlsreload`; the worker dials via `GetClientCertificate`, servers via
  `GetCertificate`.

## Testing

- Run tests in the background so you can answer questions in parallel.
- When checking integration results against the Postgres container, poll every 5–10s —
  not every minute.
