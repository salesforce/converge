# TypeScript worker demo

## Quickstart

`cd examples/demos/typescript && just demo`, then open the UI at http://localhost:8080 and
watch it converge. `Ctrl+C` tears it down. The rest of this README explains what you're
looking at.

A Converge **worker written in TypeScript** — the same role the Go demo workers play, in a
different language. It reconciles a small resource graph against the same broker, over the
same public `worker.proto`. **A worker's language is invisible to the control plane:** the
broker owns the queue, the lease, and all DB I/O and speaks proto to whatever pulls work;
this worker wraps that proto with [`converge-worker-sdk`](../../../sdk-ts) exactly as the
Go workers wrap it with [`sdk-go/converge`](../../../sdk-go/converge).

## What it composes

One TypeScript worker hosts **three providers** — a composer and two leaves — and reconciles
the whole graph a Go composer would:

```
tsproject/demo (composer root)
  ├─ account/demo-platform   (leaf: team_name → id from the config's pattern)
  ├─ account/demo-payments   (leaf)
  └─ bucket/demo-bucket       (leaf: bkt-<account_id>)
       edge: bucket.account_id ← account/demo-platform.status.account_id  (value flow)
rollup: tsproject/demo goes Ready only when every child is Ready.
```

This exercises the **full engine flow** from TypeScript: `compose` (fan a spec into children +
dependency edges + value flows), leaf `work`, the engine flowing an upstream's status into a
downstream's spec along the edge, and `rollup` (aggregate the settled subtree into the root's
`Ready` condition). It's the classic-demo composition at small scale — same engine, the
composer written in TS instead of Go.

### Config-gated readiness

The `account` leaf also demonstrates the **providerconfig + readiness** contract. It can't mint
an id without a name pattern, so its config is its bring-up dependency:

- `onConfig(cfg)` stores the kind's **default** providerconfig (the SDK primes it at startup and
  re-fires on every edit).
- `ready()` is **false** while the config is empty → the SDK advertises **RS-** and the broker
  **parks** the account kind's work; once the config carries an `id_pattern`, `ready()` flips
  **true** → **RS+** → the parked work drains. (Only the account kind is gated; the composer and
  bucket keep flowing.)
- `work()` resolves the **effective** config (kind default ⊕ any per-resource override, via
  `effectiveConfig`) and formats the id from the pattern (`acc-{team_name}-prod`).

`just demo` stages this to make it visible: it applies the account resources first (they park),
waits 10s, then applies the config (they drain). In production the config is normally in the DB
before the worker starts, so the provider is ready from task one — the pause only makes the flip
observable.

### Schema-first types

The worker doesn't hand-write its spec/status/config interfaces — it **generates** them from
the kind manifests. `npm run gen` runs the repo's `cmd/gen-types` over `testfixtures/*.kind.json`
and emits `<Kind>Spec` / `<Kind>Status` / `<Kind>Config` into `src/gen/`, which `worker.ts`
`import type`s. The manifest's JSON Schema is the single source of truth (it's what the core
validates against), so the types can't drift from it — and the same recipe generates Go structs
for a Go provider, so one schema types both languages. `just demo` runs `npm run gen` before the
build; `src/gen/` is committed (so the demo typechecks on a clean clone) and re-emitted by
`npm run gen` whenever a manifest changes — re-run it and commit the result after editing a CRD.

## The point

Converge treats a worker as an opaque Connect client of the broker. There is no Go on the
worker side of the wire — only the generated proto stubs and an SDK that wraps them. So a
team can author providers *and composition* in whatever language they run their services in,
connect to the cluster, and pull work. This demo proves it end to end: a Node process, built
from TypeScript, composes and reconciles a real resource graph in the same cluster a Go
worker would.

A provider is the whole author experience — implement `Provider` and call `serve()`. A leaf
serves one reaction; a composer switches on `req.reaction`:

```ts
class ProjectProvider implements Provider {
  kind() { return { kind: "tsproject", version: 1 }; }
  async work(req: ReactionRequest): Promise<Outcome> {
    if (req.reaction === "compose") {          // specChange → fan out children + edges
      const { teams, project } = JSON.parse(dec.decode(req.resource.spec));
      const children = teams.map((t) => ({ kind: "account", kindVersion: 1, name: `${project}-${t}`, /* … */ }));
      // … plus a bucket child + a value-flow edge: bucket.account_id ← account.account_id
      return { children, edges, status: /* … */ };
    }
    if (req.reaction === "rollup") {           // childrenSettled → aggregate readiness
      const ready = req.descendants.filter((d) => d.isReady).length;
      return { conditions: [{ type: "Ready", status: ready === req.descendants.length ? "True" : "False" }] };
    }
    throw terminal(`unknown reaction ${req.reaction}`);
  }
  onConfig() {}
  ready() { return true; }
}
await serve([new ProjectProvider(), new AccountProvider(), new BucketProvider()], controller.signal);
```

No proto, no Connect, no reconnect loop — the SDK reads `BROKER_ADDR` / `TLS_*` / `WORKER_*` /
`HEALTH_ADDR` from the environment and owns the broker relationship. One `serve([...])` fans a claimed
task to the provider whose `kind()` matches.

## Prerequisites

The root Go toolchain (for the control plane, broker, and `conctl`) **plus Node ≥ 18 + npm**
(for the SDK + this worker). The SDK is consumed as a local `file:` dependency, so no npm
registry is involved.

## Run

```sh
just demo          # 3 control + 3 brokers + 3 TypeScript workers, apply the CRDs + the tsproject root
```

`just demo` builds the converge binary + UI + `conctl` (root build), then the TypeScript SDK
and this worker (`npm install && npm run build` in each), brings up the local cluster with
`WORKER_BIN` pointed at this demo's `ts-worker.sh` shim (which execs `node dist/worker.js`),
and applies the CRDs (tsproject + account + bucket) plus a `tsproject` root (and one
hand-applied standalone `account` leaf alongside it). It then waits 10s — during which the
account children sit parked (RS-, no config yet) — and applies the account providerconfig,
opening the readiness gate.

Watch it converge with the [`conctl`](../../../cmd/conctl/README.md) CLI — `just demo` builds
it at `bin/conctl` (run it from the repo root as below, or `just install` to put `conctl` on
your `PATH`):

```sh
bin/conctl list resources                                        # tsproject/demo + its composed children → Ready
bin/conctl get resource account/demo-platform -o json | jq .status  # {"account_id":"acc-platform-prod"}  (from the config pattern)
bin/conctl get resource tsproject/demo -o json | jq .status      # {"children_ready":3,"phase":"ready"}
bin/conctl get resource bucket/demo-bucket -o json | jq .status  # {"bucket":"bkt-acc-platform-prod","account_id":"acc-platform-prod"}
```

The account's id shows the config pattern (`acc-{team_name}-prod`); the bucket's `account_id`
proves the value flow — the composer left it absent, and the engine filled it from the
account's status before the bucket ran.

`Ctrl+C` tears down the cluster + Postgres. `just kill` cleans up a wedged run.

## Files

- `src/worker.ts` — the three `Provider`s (`tsproject` composer + `account` & `bucket`
  leaves) + the `serve()` call (the entire worker). It imports its spec/status/config types
  from `src/gen/` (below).
- `src/gen/*.ts` — **generated** `<Kind>Spec` / `Status` / `Config` interfaces, derived from
  the manifests by `npm run gen` (schema-first; committed so the demo typechecks on a clean
  clone, re-emitted by `just demo`). The worker `import type`s these so its shapes can't drift
  from what the core validates.
- `ts-worker.sh` — the `WORKER_BIN` shim the launcher runs (`node dist/worker.js`, inheriting
  the launcher's env).
- `testfixtures/tsproject.kind.json` — the composer CRD (`compose` on `specChange`, `rollup`
  on `childrenSettled`).
- `testfixtures/account.kind.json` / `bucket.kind.json` — the leaf CRDs the composer emits
  (`account.kind.json` also declares the `config_schema` the account's readiness gates on).
- `testfixtures/providerconfig-account.json` — the account kind's default providerconfig
  (the `id_pattern`); applied after the worker connects, it flips the account provider ready.
- `testfixtures/resource-tsproject.json` — the composition root the worker composes.
- `testfixtures/resource-account.json` — a standalone account leaf, applied by hand alongside
  the composition.

## See also

- [`converge-worker-sdk`](../../../sdk-ts) — the TypeScript worker SDK.
- [`../classic`](../classic) / [`../datadriven`](../datadriven) — the Go demos (composer,
  child kinds, reactor) on the same engine.
- [`docs/spec/sdk-spec.md`](../../../docs/spec/sdk-spec.md) — the capability + naming spec
  every worker SDK satisfies.
