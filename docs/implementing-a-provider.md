# Implementing a provider (a new kind)

This is the end-to-end guide for a new developer teaching Converge a new **kind**. A kind
is two decoupled halves you build in order:

1. **Author the kind manifest** — the CRD: the spec/status/config **JSON Schemas** (the
   contract the server validates against and publishes at `/docs`), the declared
   **reactions**, and operational policy. Pure DATA, no code.
2. **Generate types** from that manifest (optional but recommended) — schema-first, so your
   handler's Go structs / TS interfaces can't drift from what the core validates.
3. **Write the provider** — the handler code, in the language of your choice (the **Go** or
   **TypeScript** SDK), or with **no code at all** via the generic `stdio` worker.
4. **Run a worker** — the process that hosts your provider and pulls work.
5. **Apply a provider config** (optional) — operator-owned runtime config for the kind.
6. **Create resources** — apply instances of your kind and watch them reconcile.

For the big-picture design see [architecture.md](architecture.md); for the quick start see
the [README](../README.md); for the versioning model see
[Versioning](#versioning-what-every-provider-author-must-know) below.

---

## Mental model: a kind is two decoupled halves

A kind is defined by two things that are deployed independently:

1. A **manifest** (the CRD) — pure **DATA** an operator applies to the cluster:
   the spec/status/config JSON Schemas, the declared **reactions**, and
   operational policy (caps, deadlines, finalizer). No Go code. Live-editable.
2. A **provider** — **HANDLER CODE ONLY**: a `converge.Provider` whose `Work` runs a
   task (dispatching on the reaction). It declares no schema, no reactions, no policy.

The core is **kind-blind**: it routes work purely off the manifest's
`(Trigger, Emits)` reaction masks and the `(kind, kind_version)` a task carries —
it never special-cases a kind name. That decoupling is what lets you retune or
reshape a kind by editing DATA (no core recompile) and run the handler in a dumb
worker that holds no manifest and no DB handle.

**The manifest comes first because the schema is the contract.** The spec and provider-config
shapes are declared as JSON Schema in the manifest; the server validates every resource /
config apply against them and publishes them in the OpenAPI doc at `/docs`. Your handler is
written *against* that contract — so you author (and can generate types from) the manifest
before writing a line of handler code.

**Every kind is versioned.** A kind has whole web-API-style versions — `vpc/v1`,
`vpc/v2` — carried in a separate `kind_version` integer (1–32767), never baked
into the kind string. `kind_version` is **REQUIRED and explicit everywhere** —
there is no implicit v1 default. A worker advertises `(kind, kind_version)` and the
broker routes it only that exact version's tasks; a v1 task never reaches a v2
worker. You set the version on the manifest, in your provider's `Kind()`, and on
every child/config a composer emits. See [Versioning](#versioning-what-every-provider-author-must-know) below.

### Three ways to write the handler

Step 3 (the handler) is the only language-specific part; steps 1–2 and 4–6 are identical
whichever you pick:

| Path | Import | When |
|---|---|---|
| **Go SDK** | [`sdk-go/converge`](../sdk-go/converge) | a Go provider (the classic in-tree demos) |
| **TypeScript SDK** | [`converge-worker-sdk`](../sdk-ts) | a provider in TS/Node (see [examples/demos/typescript](../examples/demos/typescript/README.md)) |
| **stdio (no code)** | — | back the kind with *any external program* over a stdin/stdout JSON protocol; no SDK, no compile (see [the stdio path](#the-no-code-path-stdworker)) |

Both SDKs expose the SAME `Provider` contract (`Kind`/`Work`/`OnConfig`/`Ready`, camelCase in
TS) and the same reaction/outcome shape; the stdio path speaks that same contract over JSON.

### Worked examples to read alongside this guide

The repo ships six runnable demos under [`examples/demos/`](../examples/demos/) — each
`cd examples/demos/<demo> && just demo` brings up a cluster + worker and converges a graph.
What each one shows:

| Demo | What to see |
|---|---|
| [classic](../examples/demos/classic/README.md) | The full guide in Go — a **leaf worker** ([account.go](../examples/demos/classic/account/account.go)), a **composer** with children + value flows + rollup ([classicbom.go](../examples/demos/classic/classicbom/classicbom.go)), a **reactor** ("after X, do Y") ([statussink.go](../examples/demos/classic/statussink/statussink.go)), a **kind with two versions in one binary** ([networking.go](../examples/demos/classic/networking/networking.go)), and the **CRD manifests** ([testfixtures/kind-\*.json](../examples/demos/classic/testfixtures/)) |
| [datadriven](../examples/demos/datadriven/README.md) | The same DAG as **composition-as-data** (no Go, no redeploy): `celbom` (CEL `when` rules), `stdcel` (a kro-style CEL resource graph), and `stdstarlark` (a Starlark program) |
| [stdio](../examples/demos/stdio/README.md) | The generic `stdio` provider bridging **any external program** (any language) into a kind over the stdio protocol — task JSON on stdin → outcome JSON on stdout |
| [stdshell](../examples/demos/stdshell/README.md) | The `stdshell` provider running a script carried **inline in the resource spec** and recording its exit result + output |
| [stdterraform](../examples/demos/stdterraform/README.md) | The `stdterraform` provider running a real `tofu apply` on a module, rolling outputs into status, and `tofu destroy` on delete (needs Docker) |
| [typescript](../examples/demos/typescript/README.md) | A **composer + leaves written in TypeScript** on the `converge-worker-sdk` SDK — config-gated readiness + schema-first generated types |

> **Building with an AI coding agent? Point it at the demos.** A worker is a small,
> well-shaped thing (implement one interface, return an `Outcome`, call `Serve`), which makes
> it an ideal task to hand to an AI agent. Tell your agent to read the closest demo above and
> implement your kind by analogy — for example: *"Look at `examples/demos/classic` — the
> `account` leaf worker, the `classicbom` composer with its children/value-flows/rollup, and
> the `testfixtures/kind-*.json` manifests. Write a new kind `database` (spec `{engine,
> size_gb}`, status `{endpoint}`) and a composer `appstack` that fans an app into a `database`
> + a `bucket` and flows the database `endpoint` into the app's spec — same structure, with the
> manifest, generated types, and a unit test for `Work`."* Every piece it needs exists in the
> demos as a copyable pattern. See [Why code, not YAML](why-code-not-yaml.md) for why this is
> the easy path.

---

## Step 1 — Author the kind manifest (the CRD)

Everything about a kind that isn't handler code lives in its manifest — a
[`KindManifest`](../internal/model/manifest.go) JSON document. It is the **contract**:
the spec/status/config JSON Schemas the server validates against, the reactions the core
routes off, and operational policy. Author a `kind-<kind>.json`:

```jsonc
{
  "kind": "account",
  "kind_version": 1,                    // REQUIRED, explicit, 1–32767 — no v1 default
  "description": "account/v1 — a cloud account owned by a single team.",
  "spec_schema":   { "type": "object", "properties": { "team_name": {"type":"string"} }, "required": ["team_name"] },
  "status_schema": { "type": "object", "properties": { "account_id": {"type":"string"} } },
  "config_schema": { "type": "object", "properties": { "endpoint":  {"type":"string"} } },
  "reactions": [
    { "name": "work", "trigger": "specChange", "emits": ["status"] }
  ],
  "max_inflight": 100                   // operational policy (optional)
}
```

and apply it out of band, exactly like a Kubernetes CRD (the server does no boot bootstrap):

```sh
conctl apply --type manifest -f account.kind.json
```

(or `PUT /api/kinds/{kind}/manifest` in prod, or the UI's Kinds page). A fresh cluster comes
up empty; the broker learns each kind live from the `kind_manifest` table, and a worker
advertising the kind is matched automatically.

> **`kind_version` is required on the manifest too.** A missing or `0` version is
> rejected `422` at the API (by the server-side manifest validator, which mirrors
> the DB `validate_kind_manifest()` CHECK) — never treated as v1.

### The schemas ARE the contract — JSON-Schema-driven, server-validated, published at `/docs`

`spec_schema`, `status_schema`, and `config_schema` each hold a **JSON Schema** (draft
2020-12) describing the shape of a JSON document. Embed each inline as a JSON object (not a
URL, not a file `$ref`); `$schema` is not required and is ignored. Each field is optional —
omit one to opt that axis OUT of typing.

The server **validates against these on every write** and **publishes them at `/docs`** — so
a resource spec or a provider config that doesn't match its schema is rejected at the gateway,
not discovered later in a handler:

| field | describes | validated? | when | on failure |
|---|---|---|---|---|
| `spec_schema` | a **resource's** `spec` | **yes** | every resource apply (the API **and** the store gateway, so an SQS/reactor-chained apply is checked too) | HTTP **400** |
| `config_schema` | a **provider config's** `spec` | **yes** | every providerconfig apply for the kind | HTTP **400** (empty config validates as `{}`, so `required` still fails) |
| `status_schema` | the `status` a **handler writes** | **no** | — | advisory only — shapes the `/docs` surface; status is produced by your handler, not user input |

Both the **resource spec** and the **provider config** are thus JSON-Schema-driven inputs the
server owns the validation of; only `status` is handler-authored (so its schema is advisory).
Each schema is published in the OpenAPI doc at `/docs` as `#/components/schemas/<Kind>Spec` /
`Status` / `Config` — the live, browsable contract for anyone applying resources or configs.

The demos' `*.kind.json` are the reference patterns. Because the schema is the source of
truth, don't hand-maintain a matching struct — generate one (step 2).

### Reactions

Each reaction is one `ReactionDecl` (the manifest's `reactions[]` entries):

```go
type ReactionDecl struct {
    Name      string      // stable handle; the handler is keyed by (kind, version, Name)
    Trigger   Trigger     // WHAT fires it (closed set below)
    Emits     OutcomeMask // WHICH parts of the returned Outcome the core APPLIES
    Verb      string      // only Trigger=operation: the subresource verb it handles
    Finalizer string      // only Trigger=deleteRequested: the finalizer string to strip
}
```

**`trigger`** — the complete closed set (the JSON string; the Go constant is `model.TriggerX`):

| `trigger` (JSON) | Go constant | fires when… |
|---|---|---|
| `specChange` | `TriggerSpecChange` | generation bumps (spec edit / first apply / version flip) |
| `childrenSettled` | `TriggerChildrenSettled` | a composite root's whole subtree settled (rollup edge) |
| `deleteRequested` | `TriggerDeleteRequested` | a finalizer-bearing resource is torn down |
| `operation` | `TriggerOperation` | a subresource verb is invoked (needs `Verb`) |
| `reactor` | `TriggerReactor` | a **reactor binding** matches a watched kind's transition |
| `resync` | `TriggerResync` | a periodic drift tick re-pends the resource (no gen bump) |

**`emits`** — the outcome parts the core APPLIES, as JSON strings: `children`, `edges`,
`configs`, `status`, `conditions`, `finalizer`, `operationOutput`, `sideEffect`. (In the
manifest you write these strings; the control-plane constants are `model.OutcomeX`.)

Common `(trigger, emits)` recipes:

| To build a… | Declare |
|---|---|
| **worker** (reconcile a leaf) | `specChange` + `status` |
| **composer** (fan a resource into children) | `specChange` + `children` (+`edges`/`configs`/`status`) |
| **status rollup** (aggregate children) | `childrenSettled` + `status` |
| **finalizer teardown** (on delete) | `deleteRequested` + `finalizer` |
| **operator verb** (a subresource action) | `operation` + `operationOutput` (with `Verb`) |
| **reactor** ("after X, do Y") | `reactor` + `sideEffect` **only** |

Legality is enforced at apply time by both `ValidateManifest` and the DB
`validate_kind_manifest()` CHECK: at most one composer, one rollup, and one `reactor`
reaction per kind; a `specChange`-emits-`children` reaction must also emit `status`
(and may not emit `sideEffect`); `operation` needs a `Verb`; `deleteRequested` needs
a finalizer; a `reactor` reaction emits `sideEffect` and nothing else.

### Work kind vs reactor kind

The only difference is which reactions a kind declares. A **work kind** owns a
resource in the graph and is applied directly. A **reactor kind** owns no resource
and is never applied as one — a *binding* runs it when some OTHER kind transitions.
A reactor CRD declares exactly one `reactor` reaction, `emits: ["sideEffect"]`, and
**no transition anywhere** (the CRD only *registers* the reactor):

```jsonc
{
  "kind": "statussink",
  "kind_version": 1,
  "config_schema": { "type": "object", "properties": {
    "endpoint": {"type":"string"}, "prefix": {"type":"string"} } },
  "reactions": [ { "name": "react", "trigger": "reactor", "emits": ["sideEffect"] } ]
}
```

You wire it with a **reactor binding** — a runtime-editable subscription applied
separately (`conctl apply --type reactorbinding -f …`, `POST /api/reactor-bindings`,
or the UI). This is the ONLY place a transition appears:

```jsonc
{
  "name": "classicbom-synced-to-statussink",
  "watch_kind": "classicbom",   // when a classicbom…
  "transition": "synced",       // …crosses 'synced' (created|synced|degraded|failed|deleted)…
  "reactor": "statussink"       // …run the statussink reactor.
}
```

A binding may optionally scope by `watch_kind_version` (which version of the watched
kind fires it) and pin `reactor_version` (which version of the reactor runs). The
fired transition reaches the reactor's handler as `ReactionRequest.Transition`, so
one reactor can serve several transitions and several reactors can subscribe to one
event.

### Publishing a changed manifest (the schema-stability gate)

Re-publishing a manifest for an **existing** `(kind, kind_version)` is gated:

- **Identical** schema → idempotent no-op (safe to re-apply on every deploy).
- **Additive / backward-compatible** change (a new optional field, a widened bound,
  a dropped requirement) → **auto-allowed**. Every live resource of that version
  stays valid, so any worker of that version stays interchangeable. No flag needed.
- **Breaking** change (a new required field, a removed/retyped/narrowed field) →
  **rejected `409`, always.** There is **no override** — you must ship it as a
  **new `kind_version`** so live resources keep validating against the schema they
  were applied under. The linter is sound (it may over-report, never under-report),
  so a rare false-positive is resolved by bumping the version, not by forcing an
  in-place mutation.

Applying `vpc/v2` (a new version) has no prior manifest for that version → always
allowed. This is the whole point of versioning: a breaking change is a new version,
not a silent reshape of the old one.

---

## Step 2 — Generate types from the manifest (schema-first)

Because the manifest's JSON Schema is the source of truth, generate your handler's types
FROM it rather than hand-keeping a parallel struct that can drift. The `just gen-types`
recipe derives **Go structs and/or TypeScript interfaces** — for each schema block present
it emits `<Kind>Spec`, `<Kind>Status`, and `<Kind>Config`:

```sh
# Go structs → ./gen (package gen); POSITIONAL args: manifests-glob, go-out, go-package
just gen-types 'internal/providers/myprov/*.kind.json' gen gen

# TS interfaces → ./src/gen (empty go-out skips Go); needs json-schema-to-typescript
# on PATH — see the TypeScript demo's `npm run gen`
just gen-types 'examples/demos/typescript/testfixtures/*.kind.json' '' gen examples/demos/typescript/src/gen
```

Then read/write those generated types in your handler (step 3): a schema edit + re-run keeps
Go and TS in lockstep. (The generators are the pinned `github.com/atombender/go-jsonschema`
for Go and `json-schema-to-typescript` for TS; `cmd/gen-types` drives both. Generated dirs
are regenerated, not committed.) This step is optional — you can hand-write the structs — but
generation is what guarantees the handler and the server-validated schema never disagree.

---

## Step 3 — Write the provider (handler code)

The handler implements ONE contract — `Provider` — one instance per `(kind, version)`. The
SDK calls only its four methods, NEVER a setup/bring-up step. In Go
([`converge.Provider`](../sdk-go/converge/provider.go)):

```go
type Provider interface {
    // The ONE (kind, version) this provider serves. PURE — no env, no dial.
    Kind() converge.KindVersion

    // Run ONE task for that pair. The SDK dispatches a claimed task here; return an
    // Outcome (wrap the error with converge.Terminal to stop retries). If the kind
    // declares several reactions, switch on req.Reaction.
    Work(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error)

    // The kind DEFAULT providerconfig — the whole {Spec, Data} monolith. Fired once at
    // startup with the primed default, then on every operator edit. Store it and read it
    // per task; a no-config provider makes this a no-op. (The per-resource override rides
    // the task; you merge them in Work.)
    OnConfig(cfg converge.ProviderConfig)

    // "Can I do work right now?" The SDK POLLS this; false sheds this pair's work at the
    // broker (RS-), true resumes it. A provider still dialing its downstream returns false
    // until connected. This is the whole bring-up contract — no setup step.
    Ready() bool
}
```

ONE `Provider` serves ONE `(kind, version)`, so `Work`/`OnConfig`/`Ready` take no
kind/version argument (the pair is fixed by `Kind`). A pure leaf is tiny — read the spec
(using your generated `<Kind>Spec` type), reconcile, return status:

```go
const Kind converge.Kind = "account"

type Provider struct{ /* your own client(s), dialed in a constructor */ }

func (Provider) Kind() converge.KindVersion {
    return converge.KindVersion{Kind: Kind, Version: 1} // explicit version, >= 1
}
func (Provider) Work(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
    var spec AccountSpec // the generated type from step 2
    if err := json.Unmarshal(req.Resource.Spec, &spec); err != nil {
        // a malformed spec can't be fixed by a retry → terminal
        return converge.Outcome{}, converge.Terminal(fmt.Errorf("decode account spec: %w", err))
    }
    // …reconcile against the real world; honor ctx (deadline + shutdown)…
    status, err := json.Marshal(AccountStatus{AccountID: "acc-" + spec.TeamName})
    if err != nil {
        return converge.Outcome{}, err // transient — retried
    }
    return converge.Outcome{Status: status}, nil
}
func (Provider) OnConfig(converge.ProviderConfig) {} // no config
func (Provider) Ready() bool                { return true } // no downstream to dial
```

`Work` is **pure decision logic** — it never touches Postgres. It gets a `ReactionRequest`,
returns an `Outcome`, and the **core** applies exactly the parts the reaction's `emits` mask
permits. See [the reaction contract](#input--handler--output-the-reaction-contract) for the
full input/output shape.

A kind with several reactions switches on `req.Reaction` inside `Work` (e.g. a composer
routes `"compose"` → build children, `"rollup"` → aggregate, `"teardown"` → finalize).
Different versions of a kind are DIFFERENT providers (each with its own `Work`), not a branch
inside one — see [Serving multiple versions](#serving-two-versions-of-a-kind).

> **TypeScript** authors implement the same contract on `converge-worker-sdk` (`kind()` /
> `work(req)` / `onConfig(cfg)` / `ready()`), reading the generated TS interfaces. See the
> [SDK README](../sdk-ts/README.md) and the [TypeScript demo](../examples/demos/typescript/README.md).
> Prefer **no code**? Skip to [the stdio path](#the-no-code-path-stdworker).

### Consuming a provider config

**Providerconfig is a monolith** of `{Spec (json), Data ([]byte)}` — `OnConfig` receives both
together, once at startup with the primed default and again on every operator edit (base64 is
only how the public/REST API ferries `Data`; over the worker wire and in `OnConfig` it is raw
bytes).

**`OnConfig` delivers the DEFAULT; the merge happens in `Work`.** A resource may carry its OWN
providerconfig override, which rides on the task in `req.Env` — NOT through `OnConfig`. The
EFFECTIVE config a task runs on is `default ⊕ override`, resolved per task with two helpers:
`converge.EffectiveConfig[T](def.Spec, req.Env.ProviderConfig)` deep-merges the `spec`
(override wins per key) and decodes into your `T` (your generated `<Kind>Config`);
`converge.EffectiveBundle(def.Data, req.Env.ProviderBundle)` whole-replaces the opaque bundle
(a bundle can't be deep-merged). Store the default from `OnConfig` in a guarded field and read
it per task. See the SDK's
[Consuming a providerconfig](../sdk-go/converge/README.md#consuming-a-providerconfig) for a
complete example.

### Advertising per-kind readiness (shedding a degraded kind)

A worker process hosts many providers over ONE stream to the broker. If one provider's
downstream breaks — its Kafka is unreachable, its cloud API is throttling — you don't want the
broker to keep pushing THAT kind's work into a worker that will only fail it, and you don't
want to drop the whole stream (the other kinds are fine). `Provider.Ready` is how a worker
sheds a single kind:

- The SDK **polls** your `Ready` (default every 2s). A `true→false` edge sends the broker an
  `Interest` **RS-** for that `(kind, version)`; `false→true` sends **RS+**.
- On RS-, the broker stops **claiming** that kind's work off Postgres (when no ready worker
  for it exists anywhere in the fleet) and stops **pushing** it to your worker — its resources
  park, queued, for a healthy worker. The kind's healthy siblings on the same stream are
  unaffected.
- A task already **running** when you go unready finishes normally; a task that arrives in the
  tiny race window fails **transiently** (never terminal — a health outage must not poison-pill
  a resource) and re-dispatches to a healthy worker.
- A provider that always returns `Ready → true` is never gated (the default). If `Ready` is
  already false at connect, the RS- rides the subscribe, so a kind that boots degraded gets
  zero tasks.

`Ready` is *advisory* — it never fences correctness (the broker owns the lease); it only steers
where work lands. See the readiness demo:
`cd examples/demos/datadriven && just demo-unhealthy` launches the fleet with `fakedb`
reporting unready, so its resources park while the rest of the DAG reconciles.

### Serving several kinds or versions from one binary {#serving-two-versions-of-a-kind}

ONE `Provider` serves ONE `(kind, version)`. To host several from one binary, list one
provider per pair in the slice passed to `converge.Serve`, and the SDK fans out. A provider
that needs to serve several pairs (e.g. because they share a backend, or the set is
env-driven) exposes a constructor returning `[]converge.Provider`; the host splices it into
the slice.

**Two versions of one kind are two providers**, each with its own `Work` — a v1 spec and
a v2 spec have different shapes, so v2 code handles v2 tasks and a v1 worker never sees a
v2 task. The classic demo's networking provider builds one single-pair provider per pair
over a shared backend:

```go
// examples/demos/classic/networking/networking.go — trimmed
func New() ([]converge.Provider, error) {
    be := backend{ /* delays, fault rate, read from env */ }
    return []converge.Provider{
        provider{kv: converge.KindVersion{Kind: KindVPC, Version: 1}, be: be, work: vpcWorker{…}},
        provider{kv: converge.KindVersion{Kind: KindVPC, Version: 2}, be: be, work: vpcV2Worker{…}}, // v2 requires `region`
        provider{kv: converge.KindVersion{Kind: KindTGW, Version: 1}, be: be, work: tgwWorker{…}},
        provider{kv: converge.KindVersion{Kind: KindRoute, Version: 1}, be: be, work: routeWorker{…}},
    }, nil
}

// each provider fixes its pair via Kind() and routes its own reactions in Work:
func (p provider) Kind() converge.KindVersion { return p.kv }
func (p provider) Work(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
    switch req.Reaction {
    case "teardown":                 return p.be.teardown(ctx, req)
    case "operate:enable_flow_logs": return p.enableFlowLogs(ctx, req)
    }
    return p.work.React(ctx, req) // this pair's reconcile handler (vpc/v1 ≠ vpc/v2)
}
```

The host splices the constructor into the slice:
`nets, _ := networking.New(); providers = append(providers, nets...)` then one
`converge.Serve(ctx, providers)`. The broker routes each `vpc` resource to the worker for
the exact version it pins. A resource pinned to a version with no connected (ready) worker
**parks** (stays unclaimed) rather than run on the wrong version — a safe, visible stall.

---

## Step 4 — Run a worker

A worker is a **dumb Connect client** built on the `sdk-go/converge` SDK. It holds no DB
handle and no manifest — it dials the broker, advertises its `(kind, kind_version)` pairs,
pulls work, runs your `Work`, and reports the outcome. The wire contract is a small,
language-agnostic [protobuf](../proto/converge/worker/v1/worker.proto) — the PUBLIC
`WorkerService` every language SDK wraps (the broker↔broker mesh is a separate,
internal proto a worker never sees); the SDK hides all of it — you never touch
connectrpc, an HTTP transport, or a reconnect loop.

> The SDK has its own self-contained guide with runnable examples and the full
> configuration reference: **[sdk-go/converge/README.md](../sdk-go/converge/README.md)** (also on
> [pkg.go.dev](https://pkg.go.dev/github.com/salesforce/converge/sdk-go/converge)). This step
> is the quick version; that README is the reference.

```go
package main

import (
    "context"
    "log"
    "os/signal"
    "syscall"

    "github.com/salesforce/converge/sdk-go/converge"

    "example.com/myprov" // your provider package
)

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    // Serve reads config from the environment, then dials, serves, reconnects, and drains;
    // it blocks until ctx ends. A malformed env var (a bad duration/count) is a loud error.
    if err := converge.Serve(ctx, []converge.Provider{myprov.Provider{}}); err != nil {
        log.Fatal(err)
    }
}
```

`converge.Serve(ctx, providers)` reads config from the environment and OWNS the whole
broker relationship: dialing (h2c, or mTLS with cert hot-reload), reconnect-with-backoff,
config prime/refresh, the readiness poller, and graceful drain — plus the `WORKER_KINDS`
filter, optional k8s probes, and logging. You list one `converge.Provider` per pair in the
slice; the SDK fans work out to the matching provider. The CALLER owns only the signal ctx
+ the exit decision (Serve touches no signals and never `os.Exit`s, so it embeds in a
bigger app too). The one thing the SDK does NOT manage is your OWN external clients (a
Kafka producer, a DB pool) — you open those in `main` and `defer client.Close()` them,
exactly as without converge.

The config comes from the environment:

| env var | default | meaning |
|---|---|---|
| `BROKER_ADDR` | — (**required**) | the broker's Connect address |
| `WORKER_MAX_PARALLEL` | `16` | max concurrent tasks this pod runs (per pair) |
| `WORKER_DRAIN_TIMEOUT` | `50s` | graceful drain window on SIGTERM |
| `WORKER_KINDS` | (all) | comma-separated filter of kinds to serve |
| `TLS_CERT_FILE` / `TLS_KEY_FILE` / `BROKER_CA_FILE` | — | mTLS to the broker |
| `TLS_RELOAD_INTERVAL` | — | cert reload poll interval (hot-reload on rotation) |
| `HEALTH_ADDR` | — | optional `:port` for k8s liveness/readiness probes |
| `LOG_LEVEL` / `LOG_FORMAT` | `info` / `text` | logging |

```sh
BROKER_ADDR=broker:9090 WORKER_MAX_PARALLEL=64 ./myworker
```

The in-tree binaries are [cmd/converge](../cmd/converge) (the all-in-one server /
broker / worker with the bundled providers, running each provider IN-PROCESS) and
[cmd/stdworker](../cmd/stdworker) (the generic external-program providers). The
[classic demo worker](../examples/demos/classic/cmd/worker) and the
[datadriven demo worker](../examples/demos/datadriven/cmd/worker) (which also shows a
readiness knob) both build their `main` on `converge.Serve`.

The SAME `converge.Provider` runs unchanged on a dumb worker (its `Work` over the
WorkStream) AND in the in-process all-in-one `cmd/converge` engine — no second contract,
no rebuild.

---

## Step 5 — Apply a provider config (optional)

A **provider config** is an operator-owned document your handler reads at runtime
(e.g. an endpoint, a bucket, credentials). Its shape is the kind's `config_schema` (step 1),
so the server validates every config apply against it — exactly like a resource spec. It is
versioned on the same axis as the kind — one config lives at a specific `(kind, kind_version)`,
validated against that version's `config_schema`:

```sh
conctl apply --type providerconfig -f providerconfig-account.json
```

There is one **default** per `(kind, kind_version)` (applied to every resource of
that version that doesn't override it) plus any number of named **custom** overrides
a resource attaches via `provider_config_ref`. A resource may only attach a config of
its own `(kind, kind_version)`. Your handler reads the effective config (default ⊕
override) from `req.Env` live per task — re-pointing a bucket reconfigures every
worker with no restart (see [Consuming a provider config](#consuming-a-provider-config)).

---

## Step 6 — Create resources

```sh
conctl apply --type resource -f resource-account.json
# or POST /api/resources with a {kind, kind_version, name, labels, spec} body
```

The resource's `spec` is validated against the kind's `spec_schema` (step 1) at the gateway
(`400` on mismatch). The manifest must name its `kind_version` (`422` if missing/0). Applying
an existing `(kind, name)` at a **different** version is a uuid-stable **flip** — the spec is
rewritten and the row's version bumps in place, value-flows survive, no infra is torn down.

Useful `conctl` commands for a new dev:

```sh
conctl apply  --type {resource|manifest|providerconfig|reactorbinding} -f FILE
conctl get    resource <kind>/<name>
conctl get    manifest <kind> --kind-version <N>   # --kind-version is REQUIRED here
conctl get    providerconfig <name>
conctl list   {resource|manifest|providerconfig|reactorbinding|cluster}
```

---

## Input → handler → output: the reaction contract

The core fills only the `ReactionRequest` fields a given reaction needs (driven by
its `Trigger`), calls your `Work`, and applies exactly the `Outcome` parts the
reaction's `emits` mask permits. All DB reads/writes happen in the core around the
handler, so every fencing and gating invariant holds regardless of where the handler
runs.

```go
type ReactionRequest struct {
    Reaction    string   // the ReactionDecl.Name — which reaction this is (switch on it in Work)
    KindVersion int      // the resource's version — switch on it to serve several versions of one kind
    Trigger     Trigger  // why it fired

    Resource    Resource        // the subject resource, with its current Spec/Status
    Status      json.RawMessage // accumulated status so far (nil for the first handler)
    Observed    []Resource      // COMPOSER: the children it already owns, with statuses
    Descendants []Resource      // ROLLUP: the settled subtree to aggregate
    Operation   *Operation      // OPERATE: the verb + input
    Transition  Transition      // REACTOR: the lifecycle edge (a TransitionX constant: created|synced|degraded|failed|deleted)
    Generation  int64           // the generation that fired (pin reads/keys to it)
    DedupToken  string          // REACTOR: stable id for non-idempotent sinks
    Env         *Env            // logger + this resource's per-resource config override (merge with the default in Work)
}

type Outcome struct {
    Status     json.RawMessage      // work/compose/rollup status (last-writer-wins)
    Conditions []Condition          // health/custom axes (accumulate)
    Children   []ChildSpec          // COMPOSER: desired children
    Edges      []DepEdge            // COMPOSER: dependency edges (+ value flows)
    Configs    []ProviderConfigSpec // COMPOSER: provider configs it owns

    OperationOutput json.RawMessage // OPERATE: verb output
    SideEffectDone  bool            // REACTOR: informational
}
```

- A **worker** reads `req.Resource.Spec` (+ `Env` for config), reconciles the outside
  world, returns observed `Status`.
- A **composer** reads `req.Resource.Spec` + `req.Observed` and returns desired
  `Children` + `Edges`.
- A **rollup** reads `req.Descendants` and returns an aggregated `Status`.

**Always honor `ctx`** — the per-task deadline (`TaskDeadlineSecs`) and pod shutdown
both arrive as cancellation. A handler that ignores it has its result discarded
anyway, so cooperating is strictly better. Handlers must be **idempotent**: work is
at-least-once and a task may run twice (a reshard overlap, a retry).

---

## Passing values between resources (composition + value flows)

A composer returns **children** and **edges**; two things ride each edge:

- **Ordering** — a dependent is not scheduled until every upstream is synced.
- **Value flow** — a field is copied from an upstream's *status* into the
  dependent's *spec*, so a downstream resource gets an id it couldn't know at compose
  time — with no provider polling.

```go
out.Children = append(out.Children,
    // every emitted child MUST carry its explicit KindVersion (no v1 default;
    // a 0 fails at ApplyComposeResult, never silently v1)
    converge.ChildSpec{Kind: KindVPC,   KindVersion: 1, Name: team, Spec: VPCSpec{CIDR: "10.0.0.0/16"}},
    converge.ChildSpec{Kind: KindRoute, KindVersion: 1, Name: team, Spec: RouteSpec{}},
)
out.Edges = append(out.Edges,
    converge.DepEdge{
        From: routeRef, To: vpcRef,
        Values: []converge.ValueFlow{
            {DependentField: "/vpc_id", SourceField: "/vpc_id"}, // route.spec.vpc_id ← vpc.status.vpc_id
        },
    },
)
```

Any `ProviderConfigSpec` a composer emits likewise carries a required `KindVersion`.
`SourceField`/`DependentField` are JSON Pointers. Substitution happens DB-side — at
compose time and on every upstream status change — so the route worker just reads
`spec.vpc_id`; it never waits on or polls the VPC.

### Status rollup

A composite's **rollup** reaction (`childrenSettled` + `status`) runs once every
descendant has settled (synced, or failed). A still-progressing child holds it back;
a failed child does not — the rollup runs and reports `Ready=False`, so the composite
surfaces as **Degraded** rather than stalling. The cascade re-fires the root the
instant its last child syncs — no polling.

---

## Failing correctly: terminal vs. transient

A handler signals failure by returning an `error`:

- A **plain error is TRANSIENT** — the task is retried (attempts bumped, backoff),
  and the resource flaps `Reconciling → Failed → Reconciling → Ready` as it recovers.
  Use it for anything that might succeed on retry (a throttled API, a not-yet-ready
  dependency).
- Wrap with **`converge.Terminal(err)`** to STOP retrying — the resource pins at
  `Failed` for the current generation until its spec changes. Use it for un-retryable
  errors (a malformed spec, a permanent permission denial). A spec bump bumps
  `generation`, which self-invalidates the terminal failure with no GC.

```go
if err := json.Unmarshal(req.Resource.Spec, &spec); err != nil {
    return converge.Outcome{}, converge.Terminal(fmt.Errorf("bad account spec: %w", err))
}
```

A coarse backstop sits above the handler: a task past its `TaskDeadlineSecs` is
cancelled and recorded transient; a dead worker's claim is reclaimed by the reaper's
stale-heartbeat sweep.

---

## Versioning: what every provider author must know

`kind_version` is a first-class, **required** field. The rules that affect your code:

- **It is explicit everywhere, never defaulted.** Set it on the manifest, in your
  provider's `Kind()`, on every `ChildSpec` and `ProviderConfigSpec` a composer emits,
  and on every resource/config you apply. A missing or `0` value is a hard failure
  (`422` at the API, a startup error in the SDK, a rejected compose in the store) —
  never silently treated as v1.
- **Routing is `(kind, kind_version)` exact equality.** A worker advertising `vpc/v2`
  only ever gets `vpc/v2` tasks. Serve multiple versions by registering one provider per
  version — each fixing its pair via `Kind()`, with its own `Work` (see
  [step 3](#serving-two-versions-of-a-kind)).
- **A breaking change is a new version, never an in-place edit.** The publish gate
  auto-allows additive changes but rejects breaking ones `409` with no override —
  bump the version instead. Live resources keep reconciling on their old version and
  migrate over via a re-apply (a uuid-stable flip).
- **Range:** `1`–`32767` (the `smallint` ceiling of the `resources`/`work_queue`
  columns the version rides on). The API bounds every version input at `32767`.

---

## The no-code path (stdworker) {#the-no-code-path-stdworker}

To back a kind with an external program instead of an SDK, use the generic **stdio**
kinds served by [cmd/stdworker](../cmd/stdworker/README.md) — no SDK import, no compile.
Steps 1, 2, 5, 6 are unchanged; only step 3 (the handler) differs — it's your program:

1. Apply the CRD as in [step 1](#step-1--author-the-kind-manifest-the-crd) (the shipped
   `stdio` / `stdio-composer` CRDs, or your own).
2. Apply a providerconfig naming your program (`command` + `args`, or the
   `STDIO_COMMAND` env var).
3. Run `stdworker` with `BROKER_ADDR` set (and `STDIO_KINDS` /
   `STDIO_COMPOSER_KINDS` to pick which kinds it serves).

The worker sends the task as one JSON `Request` on the program's stdin (reaction,
spec, config, bundle, observed) and reads one JSON `Response` on stdout (status,
conditions — and for a composer, children + edges). **A compose program must set
`kind_version` on every child it returns** — exactly like the Go `ChildSpec.KindVersion`
(a `0` is rejected, never v1). Exit codes: `0` = success, `2` = terminal (no retry),
any other non-zero = transient (retried). See
[examples/demos/stdio](../examples/demos/stdio/README.md) for the canonical handler.

---

## Testing a kind

Handlers are pure, so unit-test them by calling the provider's `Work` with a hand-built
`ReactionRequest` and asserting the `Outcome` — no DB, no broker needed. For the full
reconcile path
(compose → children → value flows → rollup → status), the integration suite drives a
real Postgres via testcontainers; see the demo providers under
[examples/demos/](../examples/demos/) and their `*_test.go` for reference patterns,
including fault injection to exercise transient retry + heal.
