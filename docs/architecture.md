# Converge — architecture & internals

The deep dive: the two-axis status model, the manifest (CRD) model, the
reconcile pipeline, scheduling, reactors, schema, sharding, the
per-kind concurrency cap, dependencies/value-flows, soft-delete + reverse-dependency cascade, the HTTP API,
topology, and build/run/test. For the quick start, see the [README](../README.md).

A Postgres-backed control plane that turns declarative resource specs into
provisioned resources, in the shape of Kubernetes + Crossplane but running on
**Postgres only** — no etcd, no Redis, no Kafka, no message broker. Single Go
binary, multi-pod ready, with an embedded React UI. Built and tuned to
reconcile **~1M resources** on one Postgres primary (plus an optional
streaming read replica for the UI).

The core idea: a *kind* is a `KindManifest` (a CRD-style manifest)
**applied to the DB**, not a compiled-in record. A manifest declares a
list of **reactions**, each a `(Trigger, Emits)` pair — `specChange+children`
is a composer, `specChange+status` a worker, `childrenSettled+status` a rollup,
`deleteRequested` a finalizer teardown, `operation+verb` an operator verb,
`reactor+sideEffect` a reactor. The core dispatches purely off these masks and never
reads a kind name; provider code supplies only the matching handler **code**.
Everything — roots and their composed children — is a row in one `resources`
table. Scheduling, gating, cascade, and status application live in **plpgsql**
so a reconcile cascade is a transaction commit, not a controller wakeup.
Workers pull tasks from a sharded `work_queue` and write results to a
transactional `work_outbox` that a drainer applies back to `resources`.

```
   user ──POST /api/resources──> root resource        synced_gen<generation (is_ready=false)
                                      │
                                      ▼  schedule_eligible() → work_queue (task_type=reconcile)
                              ┌────────────────────────┐
                              │  reconcile task        │   one worker invocation runs the
                              │  reactions matching    │   reactions the manifest declares:
                              │  the trigger:          │     → compose:  children + dep edges (one tx)
                              │   compose / work /     │     → work:     observed status
                              │   rollup               │     → rollup:   aggregate, once descendants settled
                              └────────────────────────┘
                                      │ append result
                                      ▼
                                 work_outbox ──(coalesced outbox_ready NOTIFY → drainer wakes, LISTEN)──┐
                                      │                                                                 │
                                      └──drain_outbox_batch()──> resources  <───────────────────────────┘
                                      │                                   • status, synced_gen, health_ok
                                      │                                   • resource_conditions (on transition)
                                      ▼  AFTER-STATEMENT cascade_on_ready_change (reactive)
                            schedule_eligible(direct dependents ∪ rollup-having roots)
                                      │
                                      ▼  coalesced work_ready NOTIFY → pod Dispatcher wakes (LISTEN), re-sweeps work_queue
```

---

## The two-axis status model

A resource carries **two orthogonal readiness axes**. This is the
load-bearing distinction in the whole design.

- **Synced** — `synced_gen` vs `generation`. "Have the controllers reconciled
  the spec I declared?" `generation` is bumped on every spec change (a BEFORE
  UPDATE trigger); `synced_gen` advances only on a successful full-pipeline
  reconcile. This is the **only** axis the DAG scheduler/gates consult.
  (≈ Crossplane `Synced`, K8s `observedGeneration`.)
- **Ready / health** — `health_ok BOOLEAN` (default `true`). "Is the resource
  observed healthy *right now*?" Can flip `true→false` with **no** generation
  bump — e.g. a periodic resync probe finds a resource degraded long after its
  spec last changed. (≈ Crossplane `Ready`, Available/Unavailable.)

`is_ready` collapses both for the fast path and partial indexes:

```sql
is_ready BOOLEAN GENERATED ALWAYS AS (
    synced_gen >= generation AND health_ok AND deletion_requested_at IS NULL
) STORED
```

So `is_ready=false` can mean several distinct things — still reconciling,
never started, synced-but-unhealthy, failed, or being deleted — and `phase`
(below) tells them apart in one value.

A third scalar makes the *failed* state first-class instead of inferred:

- **Failed** — `failure_gen` (default `0`), the generation a reconcile last
  hard-failed at. `failure_gen = generation` ⟺ failed for the live spec. The
  drainer stamps it on a failed reconcile and clears it (→0) on a success that
  advances `synced_gen`; a spec bump increments `generation`, so a stale
  failure self-invalidates with no GC. (≈ Crossplane `Synced=False`.)

### `phase` — the single readiness signal

`phase` is a STORED GENERATED column, the **one value** every list row, count,
filter, and detail view keys off, so DB / API / UI cannot disagree. Seven
states, fixed precedence, derived purely from the scalars above — no join,
no JSON:

```sql
phase TEXT GENERATED ALWAYS AS (
    CASE
        WHEN deletion_requested_at IS NOT NULL          THEN 'Deleting'
        WHEN frozen_until = 'infinity'                  THEN 'Quarantined'
        WHEN failure_gen = generation                   THEN 'Failed'
        WHEN frozen_until IS NOT NULL                   THEN 'Orphaned'
        WHEN synced_gen >= generation AND NOT health_ok THEN 'Degraded'
        WHEN synced_gen >= generation                   THEN 'Ready'
        ELSE 'Reconciling'
    END
) STORED
```

`frozen_until` is one column for both set-aside states: a **finite** future instant = **Orphaned** (composer-dropped, in the grace window, the reaper's absolute teardown deadline), and **`'infinity'`** = **Quarantined** (an operator set a failed resource aside; never swept, since `'infinity'` is never `< now()`). Every scheduler/rollup gate skips a frozen row with the single predicate `frozen_until IS NULL`, and one partial index (`idx_resources_frozen_sweep`) backs both — keeping the reaper's sweep idle gate O(1) without taxing the hot write path with a second index.

Rows are listed in precedence order (top wins when more than one CASE arm holds):

| phase | meaning | K8s / Crossplane |
|---|---|---|
| `Deleting` | soft-delete in progress (`deletion_requested_at` set) | Terminating / deleting |
| `Quarantined` | operator set a failed/stuck resource aside (`frozen_until = 'infinity'`); frozen from all schedulers, excluded from rollup, never reaped | (no direct analog) |
| `Failed` | own reconcile hard-failed for the live spec (`failure_gen = generation`) | `Synced=False` |
| `Orphaned` | composer dropped this child; in the grace window before teardown (`frozen_until` finite, re-emit re-adopts) — see *Orphan-grace pruning* | (no direct analog) |
| `Degraded` | synced but observed unhealthy / children not ready | `Ready=False` |
| `Ready` | synced + healthy (`== is_ready`) | `Ready=True, Synced=True` |
| `Reconciling` | spec change in flight / never reconciled | `Synced=Unknown` |

`Failed` is served by the partial `idx_resources_failed (WHERE failure_gen =
generation)` — empty (zero maintenance) on a healthy fleet. List filters and
readiness counts are all `phase`-based; the `?phase=` query param replaces the
old `ready`/`not_ready`/`failed`/`deleting` flags. On the resources list page
`?phase=` is **repeatable** — multiple values union (`r.phase = ANY($phases)`),
so the UI's multi-select phase chips return the exact union, computed
server-side (never narrowed client-side over one fetched page).

The resources list is **token-paginated** (AWS-console style): the UI sends a
`limit` + keyset `cursor_at`/`cursor_id` and renders ONE page at a time with
Next/Prev (Prev via a client-side cursor stack). There is no `COUNT(*)` on this
path — at the 10M-row scale target a per-request total would be pure waste and
can lag the page under live writes, so the list shows "N resources · page K",
not "of M". Each page is exact and self-consistent; an empty `X-Next-Cursor-At`
means the last page. (This replaced an accumulating infinite-scroll list whose
periodic all-pages refetch could surface stale duplicates.)

**Rich, K8s/Crossplane-style conditions** (`Ready`/`Synced`/custom, with
`status` True/False/Unknown + reason + message + lastTransitionTime) live in a
narrow side table `resource_conditions`, written **only on transition** and
off the hot path — now **pure decoration** (the "why"); no filter, count, or
badge reads them for classification. The happy-path `Synced=True`/`Ready=True`
are synthesized from the scalars; a kind that reports nothing produces zero
condition rows and stays `health_ok=true` for free, keeping the 1M benchmark
on a write-free hot path. A stored `False` is cleared by the drainer the
instant the resource reconciles cleanly (recovery pass), so it never leaks or
shows stale in the detail panel.

> Readiness is one DB-derived `phase` (Ready / Reconciling / Degraded /
> Failed / Deleting / Orphaned / Quarantined) over the `synced_gen` /
> `health_ok` / `failure_gen` / `frozen_until` scalars, with honest
> conditions, composite-root failure roll-up, and terminal-vs-transient
> stop-retry.

---

## Kinds: the manifest (CRD) model

A kind is a `KindManifest` ([internal/model/manifest.go](../internal/model/manifest.go))
— a declarative document an operator **applies to the DB** (the `kind_manifest`
table), exactly the way Kubernetes applies a CRD. The manifest is pure DATA: it
carries JSON Schemas, the kind's declared **reactions**, and its lifecycle
policy — but **no Go code**. The core reads it from the DB and is otherwise
kind-blind.

```go
type KindManifest struct {
    Kind         model.Kind
    Description  string
    SpecSchema   json.RawMessage // JSON Schema (draft 2020-12); drives API validation
    StatusSchema json.RawMessage
    ConfigSchema json.RawMessage // shape of the kind's config document (nil = untyped/no config)
    Reactions    []ReactionDecl  // the (Trigger, Emits) reactions the core dispatches off
    FinalizerName string         // set → two-phase delete (deleteRequested teardown); empty → hard delete

    // Operational policy — seeded insert-if-absent into kind_config on apply,
    // then operator-editable live via /api/kinds/{kind}/versions/{version}/config.
    MaxInflight        int  // GLOBAL per-kind concurrency cap (0 = unlimited)
    TaskDeadlineSecs   int  // max runtime per task (0 = no deadline)
    ResyncIntervalSecs int  // >0 opts the kind into periodic drift re-observe
    ResyncRecomposes   bool // also re-run the composer on each resync re-pend
    OrphanGraceSecs    int  // grace window before a dropped child is torn down (0 = prune now)
}
```

Each `*_schema` field holds an inline **JSON Schema (draft 2020-12)** document
describing that axis's shape (`type`/`properties`/`required`/`additionalProperties`
…); `$schema` is optional and ignored. Nil = untyped (opts the axis out). They
differ in enforcement: `spec_schema` is validated on **every resource apply**
(the HTTP boundary AND the store gateway, so an ingestion-duty / reactor-chained
apply is checked identically — a violation is a 400 / `ErrInvalidSpec`);
`config_schema` is validated on every providerconfig apply for the kind;
`status_schema` is **advisory** (status is handler-produced, not user input) —
it only shapes the `/docs` surface. Each API pod's validator surface is
**rebuilt** from `kind_manifest` on a `kind_manifest_changed` NOTIFY plus a
failsafe re-read (the same shared refresher the caches below use), so a manifest
applied on ONE pod converges on EVERY pod — an added field is never spuriously
rejected by a pod that didn't handle the apply. Validation runs `huma.Validate` against the
stored schema bytes; each axis is also published as an OpenAPI component
(`#/components/schemas/<Kind>{Spec,Status,Config}`).

The schema bytes can be generated from a provider's Go structs with
`framework.Of[T]()` (a `struct{}{}` opts an axis out → untyped) so the JSON in a
manifest matches the Go types it validates — but the runtime stores the schema
bytes, never a `reflect.Type`, so the manifest stays pure DATA.

The six legacy "stages" collapsed to **one** concept: a *reaction* is a
`(Trigger, Emits)` pair. A `ReactionDecl` is:

```go
type ReactionDecl struct {
    Name      string      // stable handle; the worker keys its handler by (kind, Name)
    Trigger   Trigger     // the condition that fires it (see the closed set below)
    Emits     OutcomeMask // which parts of the returned Outcome the core APPLIES
    Verb      string      // ONLY for Trigger=operation: the subresource verb it handles
    Finalizer string      // ONLY for Trigger=deleteRequested: the finalizer string to strip
}
```

**`Trigger`** is a closed set (`framework.Trigger` constants — use the constant,
never a string literal):

| `Trigger` value | constant | fires when… |
|---|---|---|
| `specChange` | `TriggerSpecChange` | generation bumps (a spec edit / first apply) |
| `childrenSettled` | `TriggerChildrenSettled` | a composite root's whole subtree has settled (the rollup edge) |
| `deleteRequested` | `TriggerDeleteRequested` | a finalizer-bearing resource is being torn down |
| `operation` | `TriggerOperation` | a `resource_operations` row is enqueued for its `Verb` |
| `reactor` | `TriggerReactor` | a `reactor_bindings` subscription matches a watched kind's transition |
| `resync` | `TriggerResync` | a drift tick re-pends the resource (no generation bump) |

**`Emits`** is an `OutcomeMask` — a set of `OutcomeBit`s (`framework.OutcomeX`
constants), the parts of the returned `Outcome` the core APPLIES:
`children` / `edges` / `configs` / `status` / `conditions` / `finalizer` /
`operationOutput` / `sideEffect`.

`(Trigger, Emits)` together encode what each old stage did:

| Reaction role | `(Trigger, Emits)` |
|---|---|
| composer | `specChange` + `children` (+`edges`/`configs`/`status`) |
| worker | `specChange` + `status` (no `children`) |
| status rollup | `childrenSettled` + `status` |
| finalizer teardown | `deleteRequested` + `finalizer` |
| operator verb | `operation` + `operationOutput` (with `Verb`) |
| **reactor** | `reactor` + `sideEffect` **only** (NO transition — a binding supplies it) |

`ValidateManifest` (the SDK half of the DB `validate_kind_manifest()` CHECK —
both must agree) enforces the legality lattice at apply time:

- a `specChange` reaction that emits `children` must also emit `status`/`conditions`,
  and may **not** also emit `sideEffect`;
- **at most one** `specChange`-emits-`children` reaction (one composer), **at
  most one** `childrenSettled`-emits-`status` reaction (one rollup), and **at
  most one** `reactor` reaction per kind;
- `deleteRequested` requires a finalizer (on the reaction or the manifest);
- `operation` requires a `Verb`;
- a `reactor` reaction must emit `sideEffect` and **nothing else** (it acts on
  other kinds via a binding, not on itself).

### Work-kind CRD vs reactor-kind CRD

The single distinction is which reactions a kind declares. `KindManifest.IsReactor()`
returns true when a kind declares ONLY `reactor` reactions (≥1, and no
`specChange`/`childrenSettled`/`deleteRequested`/`operation`). A reactor owns no
resource in the graph — it isn't created directly; a binding subscribes it to
OTHER kinds' transitions. A reactor CRD carries **no** transition/kind/label;
those live on the binding.

A **work kind** — e.g. a leaf `vpc` that provisions and reports status:

```jsonc
{
  "kind": "vpc",
  "spec_schema":   { "type": "object", "properties": { "cidr": {"type":"string"} } },
  "status_schema": { "type": "object", "properties": { "vpc_id": {"type":"string"} } },
  "reactions": [
    { "name": "work", "trigger": "specChange", "emits": ["status"] }
  ],
  "max_inflight": 50            // operational policy (all optional)
}
```

A **reactor kind** — e.g. `statussink`. Note: exactly one `reactor` reaction,
`emits` is `["sideEffect"]`, and there is **no `transition` field anywhere** in
the CRD:

```jsonc
{
  "kind": "statussink",
  "config_schema": { "type": "object", "properties": {
    "endpoint": {"type":"string"}, "prefix": {"type":"string"} } },
  "reactions": [
    { "name": "react", "trigger": "reactor", "emits": ["sideEffect"] }
  ]
}
```

### Reactor bindings — the subscription (the ONLY place a transition appears)

A binding is a runtime-editable `reactor_bindings` row applied via
`POST /api/reactor-bindings` (or `conctl apply --type reactorbinding`, or the UI).
It is the SOLE wiring surface — there is no derived/manual split; every binding is
an explicit, editable subscription. Fields:

| field | type | required | meaning |
|---|---|---|---|
| `name` | string | yes | unique, operator-chosen, opaque handle (the identity for edit/delete). |
| `watch_kind` | string | yes | the resource **kind whose transition fires** this. Must be a known (declared) kind. |
| `transition` | enum | yes | which lifecycle edge — one of `created` / `synced` / `degraded` / `failed` / `deleted` (the closed set; `framework.Transition` / `AllTransitions`). |
| `reactor` | string | yes | the **reactor kind** to run. Must be a registered reactor kind, and **distinct** from `watch_kind`. |
| `label_match` | object | no | `{}` / omitted = match all; else the watched resource's labels must contain these (`labels @> label_match`). |
| `enabled` | bool | no (default `true`) | a disabled binding never fires; toggling is live. |

```jsonc
{
  "name": "classicbom-synced-to-statussink",
  "watch_kind": "classicbom",   // when a classicbom…
  "transition": "synced",       // …crosses 'synced'
  "reactor": "statussink",      // …run the statussink reactor
  "enabled": true
}
```

The reactor's `react` reaction then fires; the transition that fired arrives at
its handler as data (`ReactionRequest.Transition`, a `framework.Transition`), so
one reactor kind can serve several transitions via several bindings, and several
reactor kinds can subscribe to the same `(watch_kind, transition)`. See
*Reactors* below for the delivery mechanics.

The engine ships no built-in kinds — the core is provider-agnostic. The example
demos define these reference kinds (see [`examples/demos/`](../examples/demos/)):

| Kind | Demo | Reactions |
|---|---|---|
| `classicbom` | classic | compose + rollup — a hand-written **Go composer** expanding a BOM (functional domains × teams) into per-team accounts/VPCs/TGWs/routes, then rolling their status up into a summary |
| `account` | classic | work — a leaf (the account a team's resources live in) |
| `vpc` | classic | work + operate `enable_flow_logs` (an operator verb) |
| `tgw`, `route` | classic | work — networking leaves wired by value flows |
| `statussink` | classic | react (a `reactor` reaction) — reference **reactor**: uploads a resource's rolled-up status to an object store when a subscribed transition fires. The CRD registers it; a binding wires it. See *Reactors* |
| `celbom` | datadriven | compose — composition as **CEL rule data** in the providerconfig spec (a fixed Go fan-out + one-line `when` predicates) |
| `stdstarlark` | datadriven | compose — composition as a **Starlark program** shipped in the providerconfig bundle (logic-as-data, no redeploy) |
| `fakevpc`, `fakedb`, `fakeapp` | datadriven | work — a multi-edge value-flow DAG (an app gated on both a vpc + a db) |
| `faketerraform`, `fakek8sjob` | datadriven | work — "run a team's Terraform from S3" → "run its K8s Job on the built image" (a value-flow pipeline) |

(`noop`, a do-nothing leaf, is a test-only kind under [`test/internal/noop`](../test/internal/noop).)

**Adding a kind** is three steps, and the core never recompiles for the schema
or reactions:

1. **Write a provider package** implementing `converge.Provider`
   ([sdk-go/converge/provider.go](../sdk-go/converge/provider.go)) — `Kind() KindVersion`
   (PURE: the ONE (kind, version) served), `Work(ctx, req) (Outcome, error)` (run one
   task; the reaction **handler code**), `OnConfig(ProviderConfig)` (react to a
   runtime-editable config push — see *Runtime-editable config*), and `Ready() bool` (the
   provider dials its OWN external client on its own schedule and reports readiness — the
   SDK calls no setup step). It declares NO schema, NO reactions, NO policy. A binary that
   serves several `(kind, version)` pairs registers one provider per pair.
2. **Author its CRD manifest** — a `testfixtures/kind-<kind>.json` (the canonical
   fixture) declaring the schemas, reactions, and policy, or `PUT
   /api/kinds/{kind}/manifest` in prod. This is editable DATA: change the schema,
   add a reaction, or re-tune a policy knob without recompiling the core.
3. **Register it in a worker `main`** — list it in the `[]converge.Provider` passed to
   `converge.Serve(ctx, providers)`; the shipped `std*` providers are wired in
   [cmd/stdworker](../cmd/stdworker), the demo ones in
   [examples/demos/*/cmd/worker](../examples/demos).

A worker binary hosts only the providers its author registers, and each provider dials
its OWN downstream on its own schedule (there is no Setup step) — so a pod reads env /
builds creds only for its own kinds. A control pod carries NO provider code and dials
nothing, learning per-kind policy from `kind_config`/`kind_manifest`. The manifest ("CRD")
is applied to the DB out of band over the API — `conctl apply --type manifest` /
`PUT /api/kinds/{kind}/manifest`; a fresh cluster comes up empty and the broker
learns each kind live from `kind_manifest`. (In dev/test the launcher applies the
fixture CRDs client-side via conctl; the server does no boot bootstrap.)
The core (the broker) reads reactions/schemas/policy from the DB
([internal/runtime](../internal/runtime) `KindManifestCache`), selects the matching
reaction, and ships its NAME in the task; a remote worker is a dumb client
that looks up its handler by `(kind, reaction)` and holds no manifest.

The end-to-end runtime flow — how a CRD is applied, how a worker connects and
how its advertised kinds meet what's registered, and how a resource is reconciled
through broker → worker → DB (plus the mid-flight manifest fence and the
lifecycle-reactor path) — is covered by
[Kinds: the manifest (CRD) model](#kinds-the-manifest-crd-model) above and, for
the provider author's view, [implementing-a-provider.md](implementing-a-provider.md).

### Runtime-editable config

Worker behaviour that should change **without a redeploy** lives in the
`providerconfigs` table (Crossplane's `ProviderConfig`, adapted), not in env vars or
spec. A `providerconfig` is a small JSON document scoped to one consumer kind, in one
of two roles:

- **Default** (`is_default = true`, at most one per kind): the worker SDK PRIMES it at
  startup (a `GetProviderConfig` pull before the first task) so a provider can dial its
  BOOTSTRAP client, and keeps it **live** thereafter — the broker PUSHES the whole
  `{spec, data}` monolith on every edit (driven by a gated `providerconfig_changed` NOTIFY
  + a failsafe re-read on the control plane — the shared `NotifyRefresher`, the same
  LISTEN+failsafe every DB-view cache uses) and the SDK re-pulls on each reconnect. The
  push fires the provider's `OnConfig`, and WORK-TIME fields are read **per task** through
  that cached default, so an edit **live-reconfigures** every worker with no restart.
  **A missing or invalid default never crashes the pod:** a provider that needs the
  default to reach its bootstrap dependency simply reports `Ready() = false` until the
  config arrives and the dial succeeds — the pod starts and serves its healthy kinds while
  that one kind parks (advertised unready, so the broker sends it no work), then flips
  ready and serves live the moment the config lands.
- **Custom override** (`is_default = false`): a resource attaches its OWN providerconfig
  via `provider_config_ref`. At schedule time that config's `spec` is **cloned** into
  `work_queue.provider_config` (and its `data` into the bundle field), rides the task in
  `req.Env`, and the worker computes the EFFECTIVE config in its handler: `spec`
  field-merges over the default (override wins per key) while the bundle whole-replaces.
  Edits apply on the resource's next schedule.

Configs can be **created by a Composer** as well as by hand. A `ComposeResponse`
carries `Configs []ProviderConfigSpec` alongside its children and edges; the runtime
diffs them against the configs the root already owns and upserts only the changed/new
delta (and prunes the vanished) — the same model used for composed children. Such
configs are stamped with `owner_id` (the composing root), so they are
**garbage-collected with the root** (`ON DELETE CASCADE`) and a composer can find and
reconcile the ones it produced. Hand-authored (`POST /api/providerconfigs`) configs
leave `owner_id` NULL.

Config and `spec` are orthogonal: `spec` says *what* to reconcile, config says *how*
(buckets, broker URLs, …). The document's shape is the kind manifest's
`config_schema`, published at `GET /api/kinds/{kind}/schema` (`config_schema`)
and validated on write. Manage configs via `POST/GET/DELETE /api/providerconfigs`
(the list is paginated with `kind` / `name` / `default` filters), or browse them in the
UI's **Provider configs** page; a resource's detail panel links to its attached config.
Example: a kind that uploads to an object store takes its destination from config
(`s3_url`) — re-point the bucket via the API and every worker picks it up live; a
single resource can override it by attaching its OWN providerconfig (`is_default=false`,
via `provider_config_ref`), whose `s3_url` then merges over the default's for that
resource only. A provider may also split config into **two tiers** in one document: a
BOOTSTRAP field (e.g. a client endpoint the provider dials before it reports `Ready`, so
the kind advertises unready until the default config supplies it) and a WORK-TIME field
(edit it and every worker re-points with no restart).

---

## The reconcile pipeline

There is **one** task type for the main loop: `reconcile`. A single worker
invocation runs the kind's opted-in stages in order
([internal/runtime/reaction_engine.go](../internal/runtime/reaction_engine.go), `reactReconcile`):

1. **Composer** (if any, and `composed_gen < generation`) — produces desired
   children + dependency edges, applied in one transaction; bootstrap
   value-flows are substituted; the new children are scheduled.
2. **Worker** (if any) — reconciles the resource against the outside world and
   returns observed status.
3. **StatusRollup** (if any) — runs once every descendant is **settled**
   (`descendantsSettled`, an indexed `NOT EXISTS` probe): synced, or failed.
   A still-*progressing* descendant holds the rollup back — the worker emits
   its partial status with `advance_synced_gen=false` and the cascade re-fires
   the root when that descendant catches up. A *failed* descendant does NOT
   hold it back: the rollup runs and reports `Ready=False`/`ChildrenNotReady`,
   so the composite surfaces as `Degraded` instead of stalling in limbo.

The worker never mutates `resources` directly. It appends one row to
`work_outbox`; the **drainer** applies it.

The other task types are `delete` (run the Deleter per finalizer) and
`operate` (run a subresource verb). All three share the sharded `work_queue`.

**Per-task deadline.** A kind manifest carries `task_deadline_secs` as the
SEED (applied with the CRD); it is stored per kind in `kind_config.task_deadline_secs` and is
operator-editable live via `PUT /api/kinds/{kind}/versions/{version}/config` (no restart). The
claim reads it from `kind_config` and rides it onto each task. When set, the
dispatcher runs the task under a timer and races it: if it doesn't finish in
the window, its context is cancelled and the task is recorded as a **transient**
failure (attempts bumped, non-terminal) so the normal requeue path retries it.
Enforcement
is at the dispatcher, not the handler — a wedged handler that ignores
cancellation can't pin its worker slot past the deadline; its eventual
result is discarded (the orphan's outbox write runs on the cancelled
context and is dropped, so it can't resurrect the failure). This is the
fast, per-kind bound; the reaper's stale-heartbeat reclamation remains
the coarse backstop for genuinely dead workers.

The deadline propagates **all the way to a remote worker**: the broker's
`dispatchStage` ships the *remaining* budget as `task_deadline_ms` in the
`StageTask`, and the dumb worker bounds its own handler ctx with it (a
cooperative early-stop that frees its slot promptly; the broker's `runOne`
timer stays authoritative). Two edge guards: `secsToDur` clamps a
negative/overflowing `task_deadline_secs` to 0, and `dispatchStage` floors the
wire deadline at 1ms when a deadline exists (so a sub-ms / late-stage remaining
budget can't truncate to 0 = "unbounded" on the worker). `0` = no deadline at
every hop.

---

## Scheduling: reactive cascade + reaper backstop

Almost everything is driven by plpgsql in
[db/migrations/00001_schema.sql](../db/migrations/00001_schema.sql):

| Function / trigger | Role |
|---|---|
| `schedule_eligible(uuid[])` | The gate. Inserts a `reconcile` work_queue row for each candidate that needs reconciling (`generation > synced_gen`), is not deleting, has all **upstreams** caught up, and (for rollup roots) has all **descendants** caught up. ON-CONFLICT gen-gate makes it idempotent. |
| `cascade_on_ready_change` (AFTER STATEMENT) | **Reactive scheduling + demotion + lifecycle emit.** When the drainer's UPDATE advances any row's `synced_gen` to meet `generation`, this fires once and schedules its direct edge-dependents **and** the rollup-having root each upgraded descendant belongs to — so a composite's rollup fires the instant its last child syncs, no polling. It also **re-pends a root when a descendant's health flips false or it newly fails**, so composite demotion is reactive. And — gated by an `EXISTS`-on-bindings short-circuit so it costs nothing when unused — it writes a `lifecycle_outbox` row for each `synced` / `degraded` / `failed` transition, the durable edge *reactors* react to. |
| `reap_stale_lifecycle(...)` | Frees `lifecycle_outbox` claims whose reactor dispatcher heartbeat lapsed, re-arming the delivery (at-least-once). Rides the reaper tick; crash-recovery only — the dispatcher's poll is the primary path. |
| `drain_outbox_batch(max_rows, shards)` | The drainer's workhorse. Pops outbox rows; applies operate/delete/finalizer transitions; substitutes value flows into dependents; writes the coalesced `status`/`synced_gen`/`health_ok`/`failure_gen` update (with a no-op gate); upserts `resource_conditions` on transition and clears stale `False` rows on recovery. |
| `requeue_failed_and_pending(...)` | **Reaper backstop.** Time-driven catch-up for rows the cascade missed (crash recovery) past their retry window. Skips terminally-failed rows (`failure_terminal AND failure_gen = generation`) so they stop retrying, and lets a root run even when a descendant has failed (only still-*progressing* children block it). No longer the primary rollup path — the concurrent-completion straddle is now recovered in seconds by `drain_rollup_rechecks` (below), not this slow poll. |
| `drain_rollup_rechecks(max_rows, shards)` | **Straddle close-out.** When a composite root's final descendants settle across two *concurrent* drain txns, neither tx's MVCC snapshot sees "0 descendants lagging", so `cascade_on_ready_change` re-pends the root in *neither* and it would strand until the reaper poll (the old 3.5s↔12s rollup-latency variance). The cascade now ARMS such a root in a small `rollup_recheck` queue (+ `pg_notify('rollup_recheck')`); the drainer calls this each tick to re-run `schedule_eligible` for those roots in a **fresh transaction** whose snapshot DOES see all siblings settled → the gate clears → the root pends in ms. Deletes only roots that actually pended; no convoy (touches just the few straddled roots, ON-CONFLICT-idempotent); empty + cheap on the healthy path. |
| `requeue_for_resync(kind, ...)` | **Drift engine.** Re-pends *settled* resources of a resync-enabled kind (no generation bump) so a provider re-observes live health — this is how `health_ok` flips false out-of-band. Zero cost for kinds that don't opt in. |
| `reap_stale_work(...)` | Frees `work_queue` rows whose worker heartbeat lapsed (dead worker) so they can be re-claimed. |
| `delete_unclaimed_work(...)` | **Abandoned-work GC.** Hard-deletes `work_queue` rows that have sat **unclaimed** (`worker_id IS NULL`) past a long window (default 24h, `SWEEPER_UNCLAIMED_DELETE_AFTER`) — work no worker ever picked up because its kind lost its worker (provider removed / decommissioned). The *opposite* of `reap_stale_work` (which recovers a dead *claim*); this drops a never-claimed row that would otherwise sit in `idx_work_queue_pending` forever. Garbage collection, not abandonment: the resource stays lagging, so `requeue_failed_and_pending` re-enqueues a fresh row if a worker for the kind ever returns. Same zero-cost idle gate + `SKIP LOCKED` + shard-range pruning as the other reaper passes. |
| `sweep_expired_orphans(...)` | **Orphan-grace expiry.** Tears down composer-dropped children whose grace window elapsed without a re-emit (`frozen_until < now()`, which never matches the `'infinity'` quarantined rows): finalizer kinds → soft-delete (their Deleter runs), leaf kinds → hard delete. Reads the per-kind `finalizer_name` from `kind_config` (the reaper has no Go registry). Same zero-cost idle gate (`idx_resources_frozen_sweep`, empty in steady state) + `SKIP LOCKED` + shard pruning, and **throttled** to ~every 6th reaper tick (the deadline is absolute, so a coarse cadence keeps even the idle-gate probe off every hot-path tick at scale). See *Orphan-grace pruning*. |
| `sweep_deletable(...)` | **Reverse-dependency cascade delete engine.** Drives a marked teardown tree (see *Soft-delete + reverse-dependency cascade*) bottom-up: for each `deletion_requested_at` node that is now UNBLOCKED (no remaining owned child, no remaining dependent still deleting) it either **enqueues its `delete` teardown task** (finalizer present, none queued yet — so a dependency's teardown runs only after its dependents are gone) or **hard-deletes the row** (finalizers empty). Kicked once inline by `request_resource_deletion` and every reaper tick as the backstop; loops within a tick so a ready subtree collapses at once. Zero-cost idle gate (`idx_resources_deleting`, empty in steady state) + `SKIP LOCKED` + shard pruning. |
| `recount_inflight(lo, hi)` | **Concurrency-cap self-heal.** Recounts each capped kind's leased rows in the reaper's shard range and rewrites the authoritative `kind_inflight` partial for that range — see *Per-kind concurrency cap* below. No-op when no kind has a cap. |
| `notify_gated(channel)` | **The wake gate.** Fires a coalesced `pg_notify(channel)` capped to ~1 per 50ms window per channel, **globally** across all backends, without any caller blocking. Layer 1: unlocked `SELECT` of the channel's `wake_state` row — ~99.99% of callers return here, lock-free. Layer 2: a **non-blocking** `pg_try_advisory_xact_lock` picks the single per-window winner (losers return at once). Layer 3: the winner stamps `wake_state` and emits the one `pg_notify`. Called by `schedule_eligible` / `cascade_on_ready_change` / `requeue_for_resync` (`work_ready`) and by `AppendOutbox` per row (`outbox_ready`). This is what lets the per-row outbox wake survive 1M appends without storming `pg_notify`'s async-queue `AccessExclusiveLock`. |

Scheduling is reactive **in both directions**, and every wake is fired DB-side
through one gate, `notify_gated(channel)`:

- **work → drainer.** `AppendOutbox` ends with `notify_gated('outbox_ready')`,
  so a freshly-appended task result wakes the drainer. The drainer's bands
  LISTEN on `outbox_ready` and re-drain `work_outbox` immediately.
- **drainer → workers.** Drainer commit → AFTER-STATEMENT `cascade_on_ready_change`
  → `schedule_eligible`, which fires `notify_gated('work_ready')`. Each worker
  pod's single Dispatcher LISTENs on `work_ready` and re-sweeps `work_queue`.

`notify_gated` is the key to surviving 1M concurrent enqueues without a
notification storm: `pg_notify` itself takes an `AccessExclusiveLock` on the
shared async-notification queue, so firing it from thousands of backends would
serialize them. The gate caps **actual** `pg_notify` callers to ~1 per 50ms
window **per channel, globally**: a cheap unlocked `SELECT` against the
`wake_state` row short-circuits ~99.99% of calls (no lock taken), and only the
single per-window winner — chosen by a **non-blocking** `pg_try_advisory_xact_lock`
— emits. So even the per-row `outbox_ready` call on the 1M-append hot path adds
no contention. Each wake is a coalesced empty-payload latency hint, never a
source of truth: a missed notify just falls back to the failsafe poll. Polling
is now only a ~30s per-pod worker failsafe (drainer: 2s) for a dropped/missed
notify, backstopped by the reaper; the NOTIFY, not a poll cadence, carries
reconcile latency. There is still no inline INSERT-trigger scheduling.

---

## Reactors (durable sagas without a saga type)

A **reactor** runs a side effect when a resource crosses a lifecycle
**transition** — the generic seam for *"after X happens, do Y"*: after a composer root
rolls up, upload its status; after a resource fails, fire a compensator; after a
resource is created, enrich it. A "saga" is **not** a first-class object here —
it's the *emergent* chain of independent subscriptions, where one reactor's action
(an external call, or a `store.ApplySpec` that produces the next resource) fires
the next transition.

**Registration and subscription are separate concerns:**

- A **reactor CRD** (`kind_manifest` with a `reactor`-trigger reaction) only
  *registers* the reactor kind + its config schema. It names NO kind, transition,
  or label — a reactor worker just connects and waits for `STAGE_REACT` tasks,
  exactly like a work worker waits for `STAGE_WORK`.
- A **binding** is the *subscription*: a runtime-editable `reactor_bindings` row
  `(watch_kind, transition[, label predicate]) → reactor`. It is the SOLE place
  the "when X transitions, run reactor R" wiring lives. Adding a subscription is
  an `INSERT`; no new code, no restart. There is no manifest projection and no
  "derived vs manual" split — every binding is an explicit, editable subscription.

At delivery, the transition that fired reaches the reactor's handler as DATA
(`ReactionRequest.Transition`); the reaction NAME to run is resolved by the claim
from the reactor kind's own CRD, so the binding name is operator-chosen and opaque.

```
ApplySpec(composer-root) ──▶ composer → children reconcile → last child settles
                         │
        drain_outbox_batch advances the root's synced_gen >= generation
                         │  cascade_on_ready_change fires (the EXISTING edge)
                         ▼
   lifecycle_outbox{<root-kind>,'synced',gen} + DIRECT pg_notify('lifecycle_ready')
                         │   ← the durable "wait until rolled up": no slot held,
                         │     no poll — the wait is the ABSENCE of this row
                         ▼
   ReactorDispatcher (on the broker) claims it FOR UPDATE SKIP LOCKED in its
   shard range → joins reactor_bindings IN-DB for the reactor kind + resolves
   its reaction name from the reactor CRD → ships STAGE_REACT to a connected
   worker advertising the reactor kind → the worker runs the reactor's React,
   uploading the rolled-up status → broker acks (deletes the row)
```

How it reuses the engine rather than adding a parallel one:

| Concern | Mechanism |
|---|---|
| **The wait** | The existing `cascade_on_ready_change` `synced` edge — captured at the one line that already computes "newly-synced as a set". No worker slot, no goroutine, no polling loop is held while a saga waits; the wait is the absence of a `lifecycle_outbox` row. |
| **The outbox** | `lifecycle_outbox` — a LOGGED, RANGE-by-`shard_id` table (clone of `work_outbox`, but LOGGED so a committed transition survives a crash). A delivery's durable unit is **per-binding**: PK `(resource_id, transition, generation, binding_name, shard_id)`, so two subscriptions on the same transition get two independent rows. The **fan-out happens at emit** (the cascade INSERT joins `reactor_bindings` on `watch_kind` and writes one row per matching enabled binding, applying `label_match`), not at claim — so each binding's ack/retry never touches a sibling's row. Written only on real transitions with a matching binding, so a 1M-healthy steady state writes none. |
| **Zero cost when unused** | Every emit is behind `EXISTS (SELECT 1 FROM reactor_bindings WHERE enabled AND transition = …)` — with no binding it's one indexed `LIMIT 1` probe returning false, no join, no INSERT, no NOTIFY (identical to HEAD on the hot trigger). |
| **The wake** | `lifecycle_ready` is a **DIRECT** `pg_notify` (never `notify_gated`): it's per-statement / low-rate, and the gate's xact lock held to commit would lose wakes inside the seconds-long drain tx (the documented `work_ready` lesson). LOGGED + the dispatcher's own poll backstop mean a dropped notify costs one idle interval, not the reaper window. |
| **Delivery** | **At-least-once, per binding.** A reactor that errors is not acked; its claim heartbeat goes stale and `reap_stale_lifecycle` (short `LifecycleStaleAfter`, ~60s) re-arms ONLY that binding's row while the dispatcher heartbeats live claims so a slow reactor isn't reaped mid-run. Every `ReactRequest` carries a stable `DedupToken` (`<id>:<transition>:<generation>:<binding>` — binding-scoped so co-bound reactors don't collide) so idempotent sinks (an S3 PUT keyed on generation) overwrite rather than duplicate. |
| **Sharding** | The dispatcher runs on every `react` pod with **no leader**, claiming `shard_id BETWEEN lo AND hi` — a resource's transition lands on the same pod-range that drains its work (same `shard_of` hash). |

A **reactor** is coded like any worker: its handler is the same
`framework.ReactionHandler`, but its manifest declares a `reactor` reaction
(rather than `specChange`) so `KindManifest.IsReactor()` holds and the core
delivers it lifecycle transitions instead of `work_queue` tasks. The classic demo's
`statussink` ([examples/demos/classic/statussink/](../examples/demos/classic/statussink/))
is the reference: it dials its object store LAZILY from the kind's default
providerconfig and reports through `Ready` (unready until the endpoint arrives and the
dial succeeds), and its `Work` uploads `req.Resource.Status` to
`<endpoint>/<prefix>/<name>-<generation>.json`.

Wiring the motivating saga — *submit a composer root, wait until rolled up, upload
the status* — is three independent pieces: register the reactor CRD, give it a
destination config, and subscribe it to the transition:

```sh
# Applied with the conctl CLI (--server $API, or $CONVERGE_SERVER). The fixtures
# are raw API bodies, so --type names which object each one is.

# 1. Register the reactor's CRD (declares a `reactor` reaction + config schema;
#    NO kind/transition). Workers advertising statussink then get STAGE_REACT.
conctl apply --type manifest -f examples/demos/classic/testfixtures/statussink.kind.json

# 2. The reactor's destination (its DEFAULT providerconfig, kind = the reactor).
conctl apply --type providerconfig \
  -f examples/demos/classic/testfixtures/providerconfig-statussink.json

# 3. The subscription: when any classicbom syncs, run the statussink reactor.
#    This is the SOLE wiring — an editable reactor_bindings row.
conctl apply --type reactorbinding \
  -f examples/demos/classic/testfixtures/reactor-binding-classicbom-to-statussink.json
```

A reactor is dispatched over the SAME broker→Connect→worker path as every other
stage (`STAGE_REACT`): the broker drains `lifecycle_outbox` and ships the
reaction to a connected worker advertising the reactor kind, then acks the
delivery only after a successful Complete (at-least-once). There is no separate
react pod or in-process reactor tier — the core stays kind-blind.

This generalises: **fan-out/join** (two subscriptions on the same `(kind,'synced')`
deliver independently; a composite's `synced` *is* the join barrier),
**multi-step chains** (a reactor `ApplySpec`s the next resource), and
**compensate-on-failure** (a binding on `(kind,'failed')`).

Bindings are managed via `POST/GET/DELETE /api/reactor-bindings` or the UI's
**Reactor bindings** page (create/edit/delete, live). The dispatcher reads them
fresh on every claim, so a change takes effect with no restart.

---

## Sharding & partitioning

`work_queue`/`work_outbox` are UNLOGGED because the source of truth is
`resources.synced_gen` vs `generation` — on a Postgres crash the reaper
rebuilds the queue from `resources`.

Both are **RANGE-partitioned by `shard_id`** (a plain column set via the
`shard_of(resource_id)` function — a partition key can't be `GENERATED`)
so 200–500 concurrent workers don't contend on shared heap/index pages.
The claim, drain, heartbeat and reaper paths scope with `shard_id BETWEEN
$lo AND $hi`, **never `= ANY($array)`**: a bound range gets runtime
partition pruning (one partition locked per claim), whereas the array
form scans every partition and overflows the lock-manager fast-path.
Workers own contiguous shard ranges, so `[lo, hi]` is their exact set.

### Sharding: logical shards vs. physical partitions

There is **one** sharding key — `shard_id = hash(resource_id) % 256` — and
three layers read it at different granularities. They are not three
separate hashes; they are three views of the same 0–255 axis.

```
            256 LOGICAL SHARDS          shard_id ∈ [0, 255]
            (shard_of(resource_id))     the fixed addressing space
                      │
        ┌─────────────┴──────────────┐
        ▼                            ▼
  PHYSICAL layer                POD-OWNERSHIP layer
  16 table partitions           N LIVE pods of a role, each a
  16 shards each:               contiguous range assigned at RUNTIME
    p0  = shard 0..15             from cluster membership:
    p1  = shard 16..31            assign_member_shards(rank, N, 256)
    …                             e.g. 50 live pods → ~5 shards each
    p15 = shard 240..255          re-tiles when a pod joins/leaves
  (partition = shard_id / 16)
```

- **Logical shards (256, fixed).** The fine-grained label every row
  carries. Oversized on purpose so pod ownership can be re-divided at
  runtime without touching data. Changing it is a one-time rewrite of
  every `shard_id` — see [internal/shardutil](../internal/shardutil).
- **Physical partitions (16).** Just a *coarser bucketing* of those 256
  shards — 16 contiguous shards per partition. This is what splits the
  heap and indexes so concurrent writers don't share pages. It adds no
  new sharding dimension.
- **Pod ownership (N), assigned DYNAMICALLY.** A *different slicing* of the
  same 256 into contiguous ranges, one per LIVE pod. There is **no manual
  assignment**: each pod registers in `cluster_members`, and its `Resharder`
  asks the DB for its range — `assign_member_shards` ranks the pod among the
  live pods of its OWN role and range-partitions [0,256) by `(rank, count)`.
  A join/leave fires a `cluster_changed` `LISTEN/NOTIFY` that re-tiles every
  pod within ~one heartbeat (no leader, no lock — transient overlap during a
  reshard is safe under `FOR UPDATE SKIP LOCKED`). See *Dynamic sharding* below.

Because a pod's range (~5 shards at 50 pods) is **narrower** than a
partition's 16, a pod's `shard_id BETWEEN lo AND hi` claim prunes to **one
partition** (occasionally two when its range straddles a 16-boundary) —
never all 16. More workers → narrower pod ranges → cleaner pruning, which
is what lets worker count scale. The partition count (16) is coupled to
`NumShards` (256) only by the `i*16 .. (i+1)*16` tiling in the migration;
changing `NumShards` means re-tiling the partition bounds too.

---

## Per-kind concurrency cap (global parallelism limit)

A kind can be given a **global** ceiling on how many of its tasks run at once
across **all** worker pods — e.g. `vpc ≤ 100` so no more than 100 vpc jobs run
in parallel cluster-wide (to respect a cloud API limit). It is **one shared
pool of tokens that any worker on any shard draws from** — not a per-shard
slice — so a shard with a burst of work can use the whole pool and **no shard
is starved**. The in-app `WORKER_MAX_PARALLEL` is only per-pod and can't bound
the cross-pod sum, so the cap is enforced in the one place every pod's work is
serialized: the `work_queue` claim.

**Declaring a cap.** A kind's manifest (CRD) carries its default cap as
`max_inflight`. When the manifest is applied to `kind_manifest` (the fixture
bootstrap or `PUT /api/kinds/{kind}/manifest`), an AFTER trigger **seeds** that
cap + task-deadline + resync policy into the single `kind_config` table
(insert-if-absent, so an operator's live edits are never clobbered by a re-apply). The `work_queue` claim reads `max_inflight` from
`kind_config` **directly** (one PK probe in its budget CTE) — there is no
projected copy and no control-plane reconcile step. A row with `max_inflight = 0`
(or no row) is **uncapped**: the claim skips all cap logic (zero cost). Because
`kind_config` is the runtime-editable source, an operator **re-caps a kind LIVE**
by editing its row (`PUT /api/kinds/{kind}/versions/{version}/config` → `store.UpsertKindConfig`) —
the next claim sees the new ceiling with no restart; the declared value is the
one-time seed, not the runtime truth.

> **Caps vs resync — what's live.** A **cap** edit is always live (the claim
> reads `kind_config` directly). A **resync** edit (`resync_interval_secs` /
> `resync_recomposes`) is also live — the control plane re-reads `kind_config` on
> a coarse tick and pushes the new policy into the running Resyncer — with ONE
> exception: a deployment that booted with **no resync-enabled kind** has no
> Resyncer goroutine (it's `nil` to keep the no-resync path zero-cost at 1M
> scale), so enabling resync on a kind for the **first time** needs a control-pod
> restart. Re-tuning, disabling, or enabling resync once *any* kind already
> resyncs are all live.

**The counter — partials summed into one pool, self-healed by recount:**

The true in-flight count of a kind is just its `work_queue` rows with
`worker_id` set. Counting that globally on every claim would scan all 16
partitions (banned on the hot path), and no single reaper pod owns all 256
shards to count them. So the tally is stored as **partials** in `kind_inflight`:

- **One partial per shard-range**, keyed by `range_lo` (the low shard of the
  writer's range). The global in-flight = **`SUM(in_flight)` over a kind's
  partials** — and the claim's budget is `max_inflight − that SUM`, so every
  worker subtracts the **same global number**: one pool, no per-shard slicing.
- **The claim** (hot path) bumps its own pod's partial by the rows it actually
  stamped — a fast optimistic *hint* that holds the cap between recounts.
- **`recount_inflight`** (reaper tick, every ~5–10s) **owns its shard range**:
  it deletes every partial whose `range_lo` falls in the reaper's `[lo,hi]`
  (reclaiming the finer worker hints) and writes one authoritative partial
  with the true leased count for that range. This *recomputes truth from the
  queue* rather than trusting increment/decrement, so it absorbs every
  completion, dead-worker reap, and re-pointed row with **no counter
  bookkeeping anywhere else** — and it works even though workers slice the 256
  shards finely (~one range per worker pod) while reapers slice coarsely (~one
  per control pod). Drift is erased within one reaper interval.

In the `TestRealBOM1M` topology (3 control pods running the reaper, 200 worker
pods), the 3 reapers' ranges tile `[0,255]` disjointly, so their partials sum
to the exact global count. The cap is accurate to within one reaper interval
of one pod's claim burst — the right precision for a soft API-rate cap.

**Why it's fast (cost-checked):** the hot claim scan is unchanged — same
`idx_work_queue_pending`, same `shard_id BETWEEN lo AND hi` pruning, no
`ORDER BY`; the budget is a PK probe on `kind_config` + a `SUM` over the tiny
`kind_inflight` table (never `work_queue`), and an uncapped kind's cap CTEs
cost nothing. The recount is a range-pruned grouped index scan
(`idx_work_queue_leased`) over the reaper's own partition(s), off the hot path.
`kind_inflight` is UNLOGGED so it resets to 0 with `work_queue` on a crash (a
LOGGED counter would survive inflated against an empty queue and wedge the cap).

---

## Dependencies and value flows

A Composer emits dep edges between the children it creates. Two things ride on
an edge:

- **Ordering** — a dependent is not scheduled until every upstream is synced
  (`schedule_eligible`'s upstream gate).
- **Value flows** — `{"dep_field":"/vpc_id","src_field":"/vpc_id"}` copies a
  field from the upstream's *status* into the dependent's *spec*. Substitution
  happens at compose time (bootstrap) and on every upstream status change (the
  drainer's substitute pass), so a route gets its VPC's `vpc_id` without the
  provider polling.

---

## Soft-delete + reverse-dependency cascade (K8s-style finalizers, kro-style ordering)

`DELETE /api/resources/{kind}/{name}` calls `request_resource_deletion`, which does NOT
remove the row. It deletes the **whole teardown tree** under/downstream of the
target, in dependency order, in three cooperating pieces:

**1. Mark the tree (`cascade_mark_for_deletion`).** A cycle-safe recursive CTE walks
from the target across BOTH edge kinds — everything it **owns** (`owner_id`
composition subtree, transitively) AND everything that **depends on** it
(`resource_deps.dependency_id → dependent_id`, transitively) — and stamps
`deletion_requested_at` + seeds each node's own `finalizers[]` from its kind's
`FinalizerName` (read from `kind_config`). So deleting a VPC also marks its subnets,
their routes, and anything else downstream. `is_ready` flips false immediately;
`phase = Deleting` (highest precedence). It enqueues **no** teardown task and removes
**no** row — that is the sweep's job (below), kept in separate statements because a
data-modifying CTE would see the pre-mark snapshot and treat every node as an
unblocked leaf.

**2. Ordered teardown + gated removal (`sweep_deletable`).** The level-triggered
engine — kicked once inline by `request_resource_deletion` (so leaves start
immediately) and then every reaper tick as a backstop. A node is **unblocked** once
it has no remaining owned child and no remaining dependent still being deleted. For
each unblocked marked node the sweep does one of two things:
- **finalizer present, no delete task yet → enqueue its `delete` task**, so its
  teardown runs *now* and only now — a dependency's finalizer (e.g. the cloud
  VPC-delete) never runs while a dependent (a subnet) still exists;
- **`finalizers` empty (teardown done, or the kind has none) → hard-delete the row.**

The Deleter runs on a worker; on success the drainer strips its finalizer string.
Removing a node unblocks the layer above it, which the next sweep pass advances — so
the tree tears down strictly **dependents-before-dependencies, the composition root
last**. The sweep loops within a tick so a ready subtree collapses at once; it's
gated by a zero-cost idle probe (`idx_resources_deleting`, empty in steady state) +
`SKIP LOCKED` + shard pruning. `owner_id ON DELETE CASCADE` remains as a safety net
but, because the gate removes a parent only after its children are already gone, it
only ever fires on an already-childless leaf — it never hard-deletes a
finalizer-bearing row out from under its Deleter.

**3. Strict wait.** A node whose own teardown never finishes keeps its finalizer,
stays `Deleting`, and **blocks everything above it indefinitely** (there is no
dead-letter unblock) — so a stuck child is surfaced rather than silently orphaning
its parent. A failed `delete` task is re-pended by the drain so its teardown retries.

If a kind has **no** finalizer, its nodes are still marked and are hard-deleted by
the gate as soon as their own children/dependents are gone (no teardown to run).

The **composer prune** honors the same protocol: when a recompose stops producing a
child, a child of a kind **with** a Deleter goes through `request_resource_deletion`
(its teardown tree tears down in order) rather than being hard-deleted out from under
it; a child with no Deleter is hard-deleted directly.

> **See it live:** the [classic demo](../examples/demos/classic/README.md) gives
> every kind a finalizer with a simulated teardown paced by `FAKE_DELETE_DELAY`
> (default 5s) — `conctl delete resource <bom>` visibly collapses the tree
> reverse-dependency order in the UI. Note that while the BOM still *desires* a
> child, resync/recompose re-creates one deleted out from under it (self-healing);
> delete the root or lower the child from the BOM spec to keep a subtree torn down.

### Orphan-grace pruning

A composer that drops a child **by mistake** (a transient bug, a half-parsed
spec) and re-emits it the next cycle would, under the immediate prune above,
tear the child down — destroying real cloud infra for a finalizer kind, or the
row for a leaf kind — only to recreate it moments later. **Orphan-grace** is the
opt-in safety buffer: a kind with `orphan_grace_secs > 0` (carried in the kind
manifest, seeded to `kind_config`, operator-tunable per kind via the kinds API)
does **not** tear a dropped child down immediately. Instead the composer stamps
`frozen_until = now() + grace` (a **finite** future instant) and leaves the row
fully in the DAG (`phase = Orphaned`). One column carries both set-aside states —
a finite `frozen_until` is "orphaned"; `'infinity'` is "quarantined" (below) — so
the stored `phase`, the reaper sweep, and the rollup/scheduling-gate exclusions
all key off the single `frozen_until` column (`frozen_until IS NULL` = live).
Then:

- **Re-emit within the window → re-adoption.** The next compose that produces
  the child again clears `frozen_until` (even when the re-emitted spec or only
  the labels changed — path-independently, no generation churn), cancelling the
  pending teardown. Nothing was destroyed. (A re-emit only clears a *finite*
  mark — it never lifts an operator's `'infinity'` quarantine.)
- **Window elapses without a re-emit → the reaper's `sweep_expired_orphans`
  escalates** it into the real delete: finalizer kinds get the soft-delete
  protocol (their Deleter runs), leaf kinds are hard-deleted. The sweep reads
  the per-kind `finalizer_name` from `kind_config` (a control pod has no Go
  registry), and rides the same zero-cost idle gate + `SKIP LOCKED` + shard
  pruning as the other reaper passes.

Orphaned children are **ignored** while in grace: excluded from a composite's
rollup (they don't count toward unready / don't flip it Degraded) and from the
scheduling descendant-gate (they don't block their root). `orphan_grace_secs = 0`
(the default) preserves the immediate-prune behavior above. Grace applies to
**all** kinds — protecting a mistakenly-dropped real-infra (finalizer) child is
the primary motivation.

### Quarantine (operator set-aside)

**Quarantine** is the operator escape hatch for "one child keeps failing and is
blocking the BOM rollup — set it aside so the rest can proceed," *without*
deleting it. `POST /api/resources/{kind}/{name}/quarantine` stamps the same `frozen_until`
column with the **`'infinity'`** sentinel (`phase = Quarantined`), which:

- **Freezes the row** from every scheduler (`schedule_eligible`, the cascade
  demotion, `requeue_failed_and_pending`, the interval-drift `requeue_for_resync`)
  and both value-flow substitute passes — it stops retrying and can't be
  resurrected by a dependency's status change — via the single `frozen_until IS
  NULL` gate it shares with orphaned rows. It also drops any pending `reconcile`
  task (but not a `delete` task — quarantine must not interfere with teardown).
- **Unblocks the root's rollup**: a quarantined child is excluded from the
  descendant gate + the unready count, exactly like an orphaned one — so one bad
  child no longer degrades the composite.
- Is **never deleted** by the reaper: `'infinity'` is never `< now()`, so the
  `sweep_expired_orphans` gate skips it with no special-case predicate.

Quarantine is a **hard freeze** — only an explicit `POST .../unquarantine`
(clears `frozen_until` to NULL, re-arms via `schedule_eligible`) lifts it. A spec
edit (Apply) or a Resync while quarantined persists the new spec / bumps
generation but does **not** un-freeze or reschedule — the row reconciles to its
latest spec only once released. A composer re-emit does **not** re-adopt a
quarantined child either (re-adoption clears only a *finite* `frozen_until`). This
distinguishes quarantine from orphan-grace: orphaned = the system dropped it and
will tear it down on a timer; quarantined = a human set it aside indefinitely.

---

## Topology: single binary, a set of duties

A pod is not a *role*; it is a **set of duties**. One binary
([cmd/converge/main.go](../cmd/converge/main.go)) resolves the `ROLE` env
var (`all` | `control` | `broker`) into a duty list via
`engine.DutiesFromConfig` and runs them through one `engine.Engine` — the binary
never branches on role into distinct node types. The two duties are the **control**
bundle (reaper + drainer + resyncer + spec GC + cluster-member GC) and the
**broker** (owns a `work_queue` shard tile, claims tasks, and fans each reaction
out over Connect to the workers via the WorkerService). The pod runs the schema
migrations once at boot in `cmd/converge` — before any duty starts, so every
schema read sees a migrated DB — and it is role-independent (control, broker,
and `all` all migrate). It is session-advisory-locked, so of the concurrently
starting pods exactly one applies and the rest block then no-op; no Deployment
must start first:

- `all` (default) — `control` + `broker` for every registered kind + HTTP.
- `control` — the `control` bundle + HTTP API. No `broker`.
- `broker` — `broker` for every registered kind. No `control`, no HTTP.

`converge` is ONLY ever a control plane and/or a broker — **never** a worker. All
provider handler code lives in a separate DB-free **worker binary**; the workers
connect IN to a broker and pull work (there is no `ROLE=worker`).
A worker either compiles Go providers against the SDK, OR is the shipped default
**stdworker** ([cmd/stdworker](../cmd/stdworker/README.md)) that runs an
external program per task over a stdin/stdout JSON protocol — the no-Go path to a
new kind. Either way the broker↔worker contract is identical.
A broker learns which kinds to claim LIVE from `kind_manifest` (the
`KindManifestCache`), so it links no provider package and needs no compiled-in kind
list. A pod with no broker duty — a `control` pod — dials nothing and reads the
per-kind operational settings it needs (resync policy) from the single
`kind_config` DB table, which is what makes a control pod cheap. This is what
lets you run **3 `control` pods + N `broker` pods**: the control pods own the
sweepers and API, the broker pool claims every kind's tasks and dispatches them
to the connected workers.

Pods are sharded by `shard_id` (hash % 256), assigned **dynamically** from live
cluster membership (see *Dynamic sharding* below) — so control pods drain/​reap
disjoint shard sets and broker pods claim disjoint slices, and the split
re-tiles itself as pods come and go. An optional `DATABASE_READ_URL` points UI
reads at a streaming replica.

**Kubernetes probes** — every pod answers `/livez`, `/healthz` (liveness, DB-free
so a Postgres blip can't trigger a restart) and `/readyz` (readiness — pings the
primary pool, so a pod that can't reach Postgres is pulled from endpoints without
being killed). Control/`all` pods serve them on the main API mux (`LISTEN_ADDR`,
default `:8080`); `broker` pods, which run no API server, serve them from a tiny
probe-only listener on `HEALTH_ADDR` (default `:8081`, plain HTTP).

**Listen address & TLS** — `LISTEN_ADDR` takes a bare `host:port` (`:8080`,
`0.0.0.0:8080` — plain HTTP, the historical form) or a URL whose scheme selects
the protocol: `http://host:port` (plain) or `https://host:port` (TLS). For
`https://`, set `TLS_CERT_FILE` and `TLS_KEY_FILE` to the PEM server cert and
key. The scheme is authoritative and validated at boot — `https://` without the
cert/key pair, or `http://`/bare *with* it, fails fast rather than silently
serving the wrong protocol.

The cert/key (and the client-CA below) are **hot-reloaded**: an auto-injected
rotation (cert-manager, a SPIFFE sidecar, a mounted k8s secret) is picked up
in-flight with no restart and no dropped connections — on **both** sides of the
handshake: the control/broker listeners' server keypair and the worker's client
keypair (+ the CAs each pins) share one reloader. Each re-reads the files every
`TLS_RELOAD_INTERVAL` (default `3m`), compares them by content, and
atomically swaps the in-memory keypair only when it actually changes; a
half-written file mid-rotation is logged and the last-good material is kept
until the write completes. Polling (rather than `inotify`) is deliberate — a k8s
secret mount rotates via an atomic symlink swap of its `..data` directory, which
filesystem watches on the leaf file miss.

**Mutual TLS** — set `TLS_CLIENT_CA_FILE` (alongside `https://`) to a PEM CA
bundle to require client certificates: the server then **requires and verifies**
that every client presents a cert signed by a CA in the bundle
(`RequireAndVerifyClientCert`) and rejects the handshake otherwise. Empty (the
default) → one-way TLS, clients unauthenticated. The CA bundle is hot-reloaded
on the same interval, so rotating the client CA also needs no restart. A
`TLS_CLIENT_CA_FILE` without `https://` fails fast at boot (mTLS would otherwise
silently not apply).

**Database auth (AWS IAM / Aurora)** — set `DB_IAM_AUTH=true` to authenticate to
RDS / Aurora Postgres with short-lived **IAM auth tokens** instead of a static
password. The DSN (`DATABASE_URL`, and `DATABASE_READ_URL` if set) still supplies
host/port/user/dbname and **must** use `sslmode=require` (or stronger) — Aurora
IAM auth requires TLS — but its password is ignored. Every new physical
connection is opened with a fresh token minted from the pod's AWS credentials
(the default chain: env vars, shared config, **IRSA** web identity, the
EC2/ECS/EKS role, IMDS). The token rides on pgx's `BeforeConnect` hook, so the
30-minute connection recycle (`MaxConnLifetime`, jittered) and any health-check
reconnect re-sign with whatever credentials are current — a credential rotation
(typically every ~15m) is picked up **in-flight with no restart**. The same hook
covers `converge migrate up`, so migrations authenticate to Aurora with IAM
exactly like the running pods. The region is resolved from the AWS default chain
(`AWS_REGION`); a missing region or credentials fails the pod fast at boot rather
than on first query. Default off → the DSN password is used as-is, so local/dev
and the test suite are unaffected.

**Logging** — structured `log/slog`. `LOG_FORMAT=json` emits one JSON object per
line (for k8s log shippers); the default `text` stays human-readable. `LOG_LEVEL`
(`debug`/`info`/`warn`/`error`) sets the threshold.

**Components**
([internal/engine](../internal/engine), [internal/runtime](../internal/runtime)): an
`engine.Engine` runs a list of duties. The **control** bundle groups Reaper +
Drainer + optional Resyncer + SpecGC + ClusterMemberGC (and the HTTP API runs
alongside it); the **dispatch** duty is one pod-scoped `runtime.Dispatcher` that
round-robins all `(kind, task_type)` pairs, with one union heartbeat and a
`work_ready` LISTEN (idle backoff PollMin→~30s failsafe). The control sweepers
share the `pollLoop` helper (adaptive busy/idle backoff). The per-pair
concurrency ceiling is a single pod-level default on the broker dispatcher
(100, from `broker.NewDispatch`), uniform across every kind.

### Running-fleet registry (the cluster view)

Every process registers itself in the `cluster_members` table as a **cluster
member** — one running instance of the binary, identified by its `role`
(`control`/`broker`/`all`), not a machine/node — via a
per-process `ClusterMemberReporter`
([internal/runtime/clustermember.go](../internal/runtime/clustermember.go))
constructed once in [cmd/converge/main.go](../cmd/converge/main.go). Each
beat (default 10s, `MEMBER_HEARTBEAT_EVERY`) is one cheap PK UPSERT carrying the
member's identity (role, kinds, owned shard span, booted runtime config, version,
host/pid, `started_at`) plus a live in-flight task count read from the
dispatcher. This beat is the ONE member-liveness cadence: the routing liveness
window (both shard tiling and mesh peer eviction) is `3×` it (30s), so a crashed
member drops out of routing within one window. A `role=all` process writes exactly ONE row (the reporter lives in
`main.go`, not in the engine halves). `last_heartbeat` is stamped with the DB
clock (`now()`), so liveness is free of cross-pod clock skew, and it is entirely
off the 1M hot path (a dozens-of-rows LOGGED table, never touched per task).

Liveness is **K8s-style soft status with delayed GC**:

- The API derives `Ready` vs `NotReady` from heartbeat age (≈3 missed beats) at
  read time — there is no stored status column, and the row is KEPT while
  `NotReady` so a just-crashed member stays visible (greyed) in the UI. This is a
  UI/history signal; ROUTING liveness (reshard + mesh) is decided separately by the
  `3×`-beat window the TopologyWatcher applies (below). Like the resource `phase`,
  the UI renders the server's `status` verbatim and never re-derives it.
- **Deregister on clean shutdown.** As the LAST shutdown step (after the HTTP
  server, worker, and control plane have stopped), a member deletes its OWN
  `cluster_members` row so it disappears from the cluster view immediately. The
  delete runs on a fresh, deadline-bounded context (5s) — a shutting-down member
  must never hang waiting on the DB if the network is wedged, so on
  error/timeout it just exits and lets the GC reclaim the row.
- `gc_stale_cluster_members()` — driven by the `ControlPlane`'s
  `ClusterMemberGC` sweeper
  ([internal/runtime/clustermembergc.go](../internal/runtime/clustermembergc.go)),
  NOT `pg_cron` (that extension isn't enabled) — is the FALLBACK: it hard-deletes
  rows past a longer TTL (~5m), reclaiming members that crashed (no deregister
  ran) or whose deregister timed out. This GC is UI/history cleanup ONLY (when the
  greyed row disappears) — NOT a routing signal; routing evicts a crashed member on
  the much shorter `3×`-beat liveness window. Cadence invariant: beat 10s ≤
  NotReady/3 ≤ TTL 5m.

The UI surfaces this at **`/cluster`** (`kubectl get nodes`-style table: name,
role, status, kinds, shards, in-flight, version, uptime, last heartbeat; expand a
row for the full runtime config), served from `GET /api/cluster-members`.

`BuildVersion` (the cluster view's VERSION column, also `GET /api/version`)
defaults to `"dev"`. `just build` / `just docker` stamp it automatically from
`git describe --tags --always --dirty` — the current tag plus commits-since and
the short commit (e.g. `v1.2.3-5-gabc1234`), the bare short commit when no tag is
reachable, and a `-dirty` suffix for an uncommitted tree. Override with
`just version=… build` (`version` is a `justfile` variable, so the assignment
precedes the recipe name).

### Dynamic sharding (membership-driven, no leader)

The same `cluster_members` registry is what assigns shards. There is **no manual
`POD_INDEX`/`POD_COUNT`** and no StatefulSet ordinal: a pod's contiguous shard
range is computed at runtime from who else is live, so you scale a role by
editing `replicas` and the cluster re-tiles itself. This is what lets every role
run as a plain **Deployment** instead of a StatefulSet.

- **Assignment is a pure DB function.** `assign_member_shards(member_id, total,
  liveness_secs)` ranks a member among the LIVE members of its **own role**
  (ordered by `member_id`, heartbeat newer than the liveness window) and
  range-partitions `[0,256)` by `(rank, count)` — the *exact* integer tiling
  `shardutil.ShardsForPod` used to do statically, but with the divisor being the
  live member COUNT discovered at query time. It returns one `int4range`, so the
  contiguous-range invariant (one `shard_id BETWEEN lo AND hi` → one partition)
  always holds. Roles tile `[0,256)` independently.
- **Reactivity is `LISTEN/NOTIFY`, via ONE topology reactor.** An `AFTER INSERT OR
  DELETE ON cluster_members` trigger fires `notify_gated('cluster_changed')` (the
  same coalescing notifier as `work_ready`/`outbox_ready` — heartbeat UPSERTs do NOT
  fire it, only joins/leaves). Each pod runs a single **`TopologyWatcher`**
  ([internal/runtime/topologywatcher.go](../internal/runtime/topologywatcher.go)) that
  `LISTEN`s on `cluster_changed` once (with a failsafe poll behind it) and, on each
  wake, runs its registered reactors: the **`Resharder`**
  ([internal/runtime/resharder.go](../internal/runtime/resharder.go), re-ask
  `assign_member_shards`) and — on a broker — the **mesh** (dial/drop peer routes,
  [internal/broker/relay.go](../internal/broker/relay.go)). Both read the LIVE
  membership through the SAME liveness window (`3×` the beat), so shard tiling and
  mesh routing never disagree on who is alive. On a JOIN (INSERT) or graceful LEAVE
  (DELETE) the whole role re-tiles AND the mesh re-connects within ~one NOTIFY window.
  A HARD CRASH fires no NOTIFY (the row lingers, only its heartbeat lapses); the
  watcher's failsafe poll re-reads the liveness-filtered membership, so the crashed
  pod drops out of BOTH the shard tiling AND the mesh peer set within one liveness
  window (≈30s) — the mesh stops dialing its dead address then, not at the GC TTL.
- **No leader, no lock.** A reshard swaps the process's shard set
  ([internal/runtime/shardset.go](../internal/runtime/shardset.go) — an
  `atomic.Pointer` every driver reads lock-free) so the dispatcher, drainer,
  reaper, resyncer and spec-GC all pick up the new range on their next tick.
  Because all assignments are rank-based, *every* boundary shifts when the
  member count changes, so two pods briefly overlap mid-reshard — which is safe:
  `work_queue` claims are `FOR UPDATE SKIP LOCKED` and every task is idempotent,
  so at worst a task runs twice and reconciles. The dispatcher heartbeats the
  shard span of its **in-flight tasks** (not its current ownership), so work
  already claimed is never reaped out from under it when its range shrinks.

---

### Graceful shutdown & in-flight drain

On a rollout / node move k8s sends SIGTERM, waits `terminationGracePeriodSeconds`
(60s on all three Deployments), then SIGKILLs. Each long-running driver —
the SDK worker runner (`sdk-go/converge`), the work `Dispatcher`, the `ReactorDispatcher` — has a
single `Run(ctx)` plus a **`DrainGrace`** field, and on `ctx` cancel **stops
pulling new work but lets in-flight work finish** for up to `DrainGrace` (bounded
by each task's own `TaskDeadline`) before abandoning it. The host just cancels
one signal ctx — the drain is the component's own behavior.

- **Worker** (`WORKER_DRAIN_TIMEOUT`, 50s): running handlers finish to their
  deadline instead of being cut mid-execution.
- **Broker** (`SHUTDOWN_DRAIN`, 45s): the parked `dispatchStage` goroutines keep
  waiting for their workers' `Complete`s so fanned-out results LAND — the
  symmetric half of the worker's drain (*worker waits for handlers; broker waits
  for its workers*). Ordering is load-bearing: `eng.Stop` drains **before** the
  broker Connect server is closed (the `Complete` path must stay open during the
  drain). A straggler past the budget is released (lease NULLed) for fast
  re-claim; the reaper is the crash backstop.
- **Control plane** runs no handlers — its sweepers are idempotent/resumable, so
  it needs no per-task drain, only the 60s grace + graceful API `Shutdown`.

All drain budgets sit below the 60s pod grace so the process exits on its own;
`preStop: sleep 5` on each pod closes the Service-endpoint-removal race. A result a
worker computes while the stream is down (broker gone) is BUFFERED and redelivered on
the worker's next connect — to whatever broker it reconnects to, which fences the write
on the echoed `claim_epoch` — so a computed result is not re-executed across a rollout.
Only when the redelivery buffer is full does a result get discarded + counted, and the
task then re-runs (at-least-once; the `claim_epoch` fence prevents double-apply).
**Caveat:** a handler still RUNNING when its broker reboots (compute not yet finished)
is re-executed on re-dispatch, so a *minutes*-long handler (e.g. a long terraform apply)
re-runs unless its work outlived the reboot — rely on handler idempotency, or raise grace
+ `SHUTDOWN_DRAIN` toward the task length so it drains.
