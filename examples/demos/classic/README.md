# Classic composition demo

## Quickstart

`cd examples/demos/classic && just demo`, then open the UI at http://localhost:8080 and watch
it converge. `Ctrl+C` tears it down. The rest of this README explains what you're looking at.

A hand-written **Go composer** that fans a BOM of teams into a real resource graph
— the "compiled composer" counterpart to [`../datadriven`](../datadriven/) (which
expresses the same shape as *data*). Same engine; the difference is only *where the
composition logic lives*.

**The benefit of a Go composer: express any business logic in Go — total
flexibility.** The composer is just a function `(spec) → children + edges`, so you
have the full language at your disposal: arbitrary control flow, real types, calls
out to any library or service, table-driven fan-out, whatever your composition
needs. Nothing about the engine constrains how you compute the child DAG. (And
because converge treats composition as a plug-in, it doesn't *have* to be Go — see
[`../datadriven`](../datadriven/) for the same DAG expressed as CEL rules or a
Starlark program. Composition can be implemented in virtually anything.)

## The business problem

**Provision every team with its standard set of cloud resources, and keep them
converged as teams come and go.** A platform/landing-zone team owns an organization
of many functional domains (business units), each with many service teams. Every
service team needs a baseline footprint — its own AWS **account**, a **VPC** in that
account, attachment to the domain's shared **transit gateway** via a **route** — and
those pieces have ordering and data dependencies (the VPC needs the account's id, the
route needs the VPC's and TGW's ids). Doing this by hand across hundreds of teams is
slow, drifts, and breaks when a step transiently fails.

This demo models that as a single declarative **BOM** (bill of materials: the list of
domains × teams) that the platform team owns. The composer fans the BOM into the
full per-team resource graph and wires the value flows; the engine provisions in
dependency order, rolls the subtree's health up to the root, and self-heals on
failure. **Add a team → its account/VPC/route appear; remove a team → its resources
are torn down** (`resource-classicbom-updated.json` shows exactly this recompose).
The real value: the platform team edits one BOM, and the desired state for the whole
org is reconciled — onboarding, offboarding, and drift correction become automatic.

Swap the stub leaves for real cloud providers and `account`/`vpc`/`tgw`/`route`
become real AWS Organizations accounts, VPCs, transit gateways, and routes — the
composition and convergence logic is unchanged.

## What this demo shows

1. **Composition** — one `classicbom` root (a BOM of functional domains × service
   teams) fans out into a per-FD / per-team resource graph (account + vpc + tgw +
   route), entirely from a Go composer.
2. **Value flows** — the composer emits `DepEdge`s carrying `ValueFlow`s: a VPC's
   `account_id` is flowed from its account's status; a route's `vpc_id` / `tgw_id`
   are flowed from the VPC and TGW. The engine gates each consumer until its
   producer is Ready, then flows the value into its spec.
3. **Rollup** — a `rollup` reaction aggregates child readiness back onto the root,
   so the `classicbom` root's status summarizes the whole subtree.
4. **Reactor** — a `reactor_bindings` subscription `(classicbom, synced) → statussink`
   fires `statussink` once on the root's `synced` transition (the durable-saga
   spine), uploading the rolled-up status to an object store.
5. **Retry & heal** (via `just inject-faults`) — transient leaf failures retry with
   backoff and the run still fully converges.

| Provider | What it shows |
|---|---|
| [`classicbom`](classicbom/) | the composer: a `ClassicBOMSpec` (functional domains × service teams) → per-FD/per-team children, plus the `rollup` reaction. |
| [`account`](account/) | a leaf: the AWS account a team's resources live in. |
| [`networking`](networking/) | leaves `vpc` / `route` / `tgw` — wired by **value flows** (`route.vpc_id` ← `vpc.status`, `vpc.account_id` ← `account.status`, …). |
| [`statussink`](statussink/) | a **reactor**: a subscription `(classicbom, synced) → statussink` uploads the root's rolled-up status to an object store. |
| [`fault`](fault/) | the demo's chaos knob (transient fault injector + simulated teardown delay) the leaf workers consult — enabled by the optional `fault_rate`/`delay` args to `just demo` (see [Run the demo](#run-the-demo)). |

These are **example providers** — they import only `sdk/*`, never `internal/*`,
exactly what an external team ships. They're also the realistic composite the core
engine's integration + 1M-scale tests exercise (crash recovery, rollup latency,
reactors, the `TestRealBOM*` scale gates).

## Run the demo

First time only, from the repo root: `just setup` (verify the toolchain) then
`just gen` (regenerate all generated code — sqlc, proto stubs, goldens, client).
Then, from this directory:

```bash
just demo                 # 3 control + 3 brokers + 3 workers
just demo 3 0.25 5s 15s    # + 25% transient faults, 5s work delay, 15s teardown delay
just inject-faults 0.3 2s  # chaos preset: rate=0.3, delay=2s
```

The optional args are `demo [num_workers] [fault_rate] [delay] [delete_delay]` —
the chaos knobs (default off) make the engine's behavior visible: `fault_rate`
(`FAKE_FAULT_RATE`, 0..1) makes each account/networking reconcile fail TRANSIENTLY
and heal on a retry; `delay` (`FAKE_WORK_DELAY`) slows each reconcile so you can
watch it; `delete_delay` (`FAKE_DELETE_DELAY`, default 5s) paces teardown so the
cascade delete is watchable. The composer is never faulted, so the DAG always
builds; the run still fully converges.

Builds the converge binary + this demo's worker, brings up the cluster, applies
the CRDs (via conctl, once the API is up), applies the reactor subscription
(`reactor-binding-classicbom-to-statussink.json`), applies the statussink config,
and posts the BOM. Watch it converge with the [`conctl`](../../../cmd/conctl/README.md)
CLI — `just demo` builds it at `bin/conctl` (run `./bin/conctl …` from the repo root, or
`just install` to put `conctl` on your `PATH`):

```bash
conctl list resources          # the composition roots + child counts

# With faults on, watch the phase tally — Failed rises as faults hit, drains to 0
# as the engine retries and heals (--all includes the leaf children):
watch -n3 "conctl list resources --all --limit 100000 -o json \
  | jq -r '.resources[].phase' | sort | uniq -c"
```

Then apply [`testfixtures/resource-classicbom-updated.json`](testfixtures/resource-classicbom-updated.json)
(one team removed, one added) to watch a **recompose** prune + add children and the
rollup re-settle. `Ctrl+C` tears down the cluster + Postgres.

### Cascade delete — watch a teardown collapse the tree

Every kind in this demo now carries a **finalizer** (`classicbom`, `account`, `vpc`,
`tgw`, `route`), and each simulates its external cleanup with a delay
(`FAKE_DELETE_DELAY`, **default 5s**) so you can watch the teardown happen. Deleting a
resource marks its **whole teardown tree** — everything it owns and everything that
depends on it — and tears it down **reverse-dependency order**: a dependent's
finalizer runs (and its row is removed) before its dependency's, and a composition
root goes **last**, only after its subtree is gone.

Try it right in the **UI** (http://localhost:8080) and watch the graph collapse:

- **Delete an `account` in the UI** — click the account node (or find it in the
  resource list), open its panel, and hit **Delete**. In this demo a team's VPC
  depends on its account and its routes depend on the VPC, so deleting the account
  marks the account **and** that VPC **and** those routes, then tears them down
  bottom-up (**routes → vpc → account**) — you can watch each node flip to `Deleting`
  and disappear in reverse-dependency order.
- **Delete the `classicbom` root in the UI** — the ENTIRE tree tears down, reverse-
  dependency order, the root removed **last** (after all its children are gone).

The same via the CLI — a resource is addressed by `<kind>/<name>` (run
`conctl list resources` to see the kinds and names):

```bash
conctl delete resource account/<name>        # deletes the account + everything downstream
conctl delete resource classicbom/<name>      # deletes the whole tree, root last
```

**But note the resync interaction.** The BOM root has `resync_recomposes` on a
`resync_interval_secs` interval: while the BOM still exists and still *desires* a
child, the composer **re-creates** anything you delete out from under it — the delete
is honored, then the next recompose (or resync tick) re-adds the still-desired
resource, so it reappears. So to actually keep a subtree torn down you either:

- **delete the BOM root** (nothing desires the children any more → they stay gone), or
- **lower the child from the BOM spec** first (apply an updated BOM that no longer
  emits it — the recompose prunes it through the same finalizer teardown), or
- expect it to **come back on the next resync** if it's still in the BOM — which is
  the correct, self-healing behavior for a declared, still-desired resource.

A resource whose teardown can never finish keeps its parent **blocked** in `Deleting`
(strict ordering — the parent waits), which surfaces the stuck child rather than
silently orphaning it.

### Versioning — evolve a kind safely (`vpc/v1` → `vpc/v2`)

This demo ships **two versions of `vpc`** so you can experience the whole web-API
versioning feature as an operator: whole versions only (`v1`, `v2` — no minor/micro,
like `/api/v1` → `/api/v2`), the **kind string stays bare `vpc`** (the version is a
separate `kind_version`), routing is strict `(kind, kind_version)` equality, and the
ONE migration event is a **kind-version bump** (the author rewrites the spec). The
one worker binary serves BOTH versions (`networking.Provider` returns
`VPCRuntime{KindVersion:1}` + `VPCv2Runtime{KindVersion:2}`); the core routes each
resource to the version it pins.

- **`vpc/v1`** — `{account_id, cidr}` plus an OPTIONAL `tags`.
- **`vpc/v2`** — a BREAKING bump: adds a **required** `region`; the v2 worker echoes
  it into `status.region` so you can SEE that the v2 worker (not v1) ran it.

Both CRDs (`vpc.kind.json`, `vpc-v2.kind.json`) are auto-applied at `just demo`.
The `conctl apply` body is a raw manifest/resource — the `kind_version` field in the
JSON is what selects the version. Run these in order; each is one observable operation:

```bash
# 1) NON-BREAKING v1 evolution — the publish LINTER AUTO-ALLOWS it (no version bump,
#    no flag). vpc-v1-additive.json adds an OPTIONAL `owner` field to v1 → HTTP 200.
conctl apply --type manifest -f testfixtures/versioning/vpc-v1-additive.json
#    → accepted: additive schema changes stay on the same kind_version.

# 2) BREAKING change attempted on v1 — the linter REJECTS it 409. vpc-v1-breaking.json
#    adds a REQUIRED `region` to v1 (would invalidate every existing v1 spec).
conctl apply --type manifest -f testfixtures/versioning/vpc-v1-breaking.json
#    → 409 Conflict: "BREAKING schema change … ship it as a NEW kind_version". THIS
#      FAILURE IS THE POINT — the system refuses to silently redefine v1. There is NO
#      override: a breaking change MUST become a new kind_version (see step 3), so
#      live v1 resources keep validating against the schema they were applied under.

# 3) The RIGHT way to make that breaking change: it already shipped as vpc/v2
#    (vpc-v2.kind.json, applied at boot). Confirm both versions exist (kind_version
#    is REQUIRED on the manifest GET — name the exact version):
conctl get manifest vpc --kind-version 1     # the v1 CRD
conctl get manifest vpc --kind-version 2     # the v2 CRD

# 4) Apply a v1 resource USING the new optional field (still v1, no migration):
conctl apply --type resource -f testfixtures/versioning/resource-vpc-v1-tagged.json
conctl get resource vpc/sample-vpc-tagged      # status.vpc_id; version shows v1

# 5) Apply a NEW v2 resource — routes to the v2 worker, which echoes the region:
conctl apply --type resource -f testfixtures/resource-vpc-v2.json
conctl get resource vpc/sample-vpc-v2          # status.{vpc_id, region:"us-west-2"}
#    → the "region" in status proves the v2 worker ran it (v1's status has no region).

# 6) FLIP a live resource v1 → v2 (uuid-stable): re-apply the SAME name
#    (sample-vpc-tagged) at kind_version 2 with the v2 spec. The row's kind_version +
#    spec flip IN PLACE on the same identity — no delete/recreate — and it re-routes
#    to the v2 worker. (The new v2 spec MUST be complete — region required — or the
#    flip is rejected and the resource stays on v1.)
conctl apply --type resource -f testfixtures/versioning/resource-vpc-flip-to-v2.json
conctl get resource vpc/sample-vpc-tagged      # now status has region → it's on v2
```

**Strict routing to see for yourself:** the v2 resources are only ever executed by
the v2 code path, and a resource pinned to a kind_version with no connected worker
would **park** (stay unclaimed, `worker_id` NULL) rather than run on the wrong
version — the safe, visible stall. In this demo the one worker serves both versions,
so nothing parks; kill the worker's v2 registration (or apply a `vpc/v3` resource
with no v3 worker) and the task waits instead of mis-routing.

**Per-version operational config (caps/deadlines):** claim caps/deadlines are
per-`(kind, kind_version)`. Edit v2's independently of v1's via the `?kind_version=`
query on the config endpoint (REQUIRED — no implicit v1 default):

```bash
curl -fsS -X PUT "$API/api/v1/kinds/vpc/config?kind_version=2" \
  -H 'content-type: application/json' \
  -d '{"max_inflight":50,"task_deadline_seconds":30}'   # tunes vpc/v2 only; v1 untouched
```

#### Provider configs are per-`(kind, kind_version)` too

A **provider config** is an operator-owned document the worker reads at runtime
(here: the flow-logs bucket). It is versioned on the **same axis as the kind** — one
lives at a specific `(kind, kind_version)`, validated against THAT version's
`config_schema`. `vpc/v2`'s config schema adds a `default_region` that `vpc/v1`'s
does not, so a v1 config and a v2 config are genuinely different documents. There are
two flavors:

- a **default** per `(kind, kind_version)` (`is_default: true`) — exactly one, applied
  to every resource of that version that doesn't attach its own;
- **custom** overrides — any number, each a named document a resource attaches by
  name via `provider_config_ref`.

The config `name` is **globally unique**: one name = one `(kind, kind_version)`. A v2
config is a *distinct name*, never "the same config at v2". Run these in order:

```bash
# 1) Apply the per-version DEFAULTS. v1's default has only flow_logs_bucket; v2's also
#    carries default_region — each validates against its OWN version's config_schema.
conctl apply --type providerconfig -f testfixtures/versioning/providerconfig-vpc-v1-default.json
conctl apply --type providerconfig -f testfixtures/versioning/providerconfig-vpc-v2-default.json
#    → two independent defaults coexist; setting v2's default does NOT touch v1's.

# 2) A SECOND default on the same (kind, kind_version) is rejected — one default per
#    version. (Re-applying the SAME name updates in place; a DIFFERENT name with
#    is_default:true on vpc/v1 → 409 "a default provider_config already exists for vpc/v1".)

# 3) Apply CUSTOM overrides, one per version (distinct global names):
conctl apply --type providerconfig -f testfixtures/versioning/providerconfig-vpc-v1-custom.json  # vpc-v1-payments
conctl apply --type providerconfig -f testfixtures/versioning/providerconfig-vpc-v2-custom.json  # vpc-v2-payments

# 4) Attach a MATCHING custom config to a v1 resource — the (kind, kind_version) lines up → OK:
conctl apply --type resource -f testfixtures/versioning/resource-vpc-v1-with-custom.json
#    → sample-vpc-v1-payments reconciles using vpc-v1-payments' bucket.

# 5) THE GUARD: attach a v2 config to a v1 resource → REJECTED at apply time.
#    resource-vpc-v1-config-mismatch.json is vpc/v1 but references vpc-v2-payments (vpc/v2).
conctl apply --type resource -f testfixtures/versioning/resource-vpc-v1-config-mismatch.json
#    → 422: "provider_config \"vpc-v2-payments\" is for vpc/v2, not vpc/v1".
#    The reverse (a v2 resource → a v1 config) is rejected identically. A resource can
#    only attach a config on its OWN (kind, kind_version); the worker never sees the wrong shape.
```

The store-level guard (mismatch rejected in BOTH directions, plus
one-default-per-version and per-version coexistence) is proven by
`TestVersioning/ProviderConfig_PerKindVersion`; the kind/version routing and the
over-the-wire (real Connect v1/v2/v3 workers) coverage lives in
`test/versioning_test.go` and `test/versioning_e2e_test.go`.

### CRDs applied (via conctl, once the API is up)

The demo applies its CRDs client-side with `conctl apply --type manifest` from
**both** the repo-root `testfixtures/` (shared core kinds) **and** this demo's
`testfixtures/` (an OS path-list); the server does no boot bootstrap. The kinds
this demo defines (all in [`testfixtures/`](testfixtures/)):

Every work/composer kind below also declares a `teardown` (`deleteRequested`)
reaction + a `finalizer`, so a delete tears each down through its own (simulated)
cleanup rather than vanishing — see *Cascade delete* above.

| CRD | Kind | Role |
|---|---|---|
| `classicbom.kind.json` | `classicbom` | composer root (BOM spec; `compose` + `rollup` + `teardown` reactions; `resync_recomposes`) |
| `account.kind.json` | `account` | leaf — produces `status.account_id`; `teardown` finalizer |
| `vpc.kind.json` | `vpc` (v1) | leaf — produces `status.vpc_id` (+ the `enable_flow_logs` operator verb); `teardown` finalizer. Carries an OPTIONAL `tags` field (a non-breaking v1 evolution). |
| `vpc-v2.kind.json` | `vpc` (v2) | the SAME bare kind at **kind_version 2** — adds a REQUIRED `region`; the v2 worker echoes it into `status.region`. See *Versioning* below. |
| `tgw.kind.json` | `tgw` | leaf — produces `status.tgw_id`; `teardown` finalizer |
| `route.kind.json` | `route` | leaf — consumes `vpc_id` + `tgw_id`; `teardown` finalizer |
| `statussink.kind.json` | `statussink` | reactor kind — registers the `react` reaction only (no transition; a binding subscribes it) |

### Resources generated (from the BOM)

The shipped [`testfixtures/resource-classicbom.json`](testfixtures/resource-classicbom.json)
is a large BOM (58 functional domains, ~1,176 service teams → ~5,400 resources). The
composer emits, per BOM element:

- **per functional domain** (when the FD has networking): one `tgw` + one tgw-host
  `account`, with an edge `tgw → account`.
- **per service team**: one `account`, one `vpc` (edge `vpc → account`, flows
  `account_id`), and one `route` (edges `route → vpc` + `route → tgw`, flows
  `vpc_id` + `tgw_id`).

The root `classicbom` rolls up all child readiness into its status and goes Ready
when the subtree is Ready.

## Apply resources manually (sample fixtures)

Each leaf kind ships a standalone sample under [`testfixtures/`](testfixtures/) so you
can create one **by hand** — no composer, no BOM. Because there's no composer to flow
upstream values, a manual leaf supplies every field directly (the API validates each
against the kind's `spec_schema`, so these are guaranteed to apply):

| Sample | Kind | Reaches Ready by… |
|---|---|---|
| [`resource-account.json`](testfixtures/resource-account.json) | `account` | writing `status.account_id` from `team_name` |
| [`resource-vpc.json`](testfixtures/resource-vpc.json) | `vpc` | `account_id` (exactly 12 chars) + `cidr` → `status.vpc_id` |
| [`resource-tgw.json`](testfixtures/resource-tgw.json) | `tgw` | `name` → `status.tgw_id` |
| [`resource-route.json`](testfixtures/resource-route.json) | `route` | `vpc_id` + `tgw_id` supplied directly → `status.route_id` |

With a cluster up (`just demo` or `just inject-faults`):

```bash
conctl apply --type resource -f testfixtures/resource-vpc.json
# repeat for resource-account.json / resource-tgw.json / resource-route.json
```

These create top-level resources (named `sample-*`, so they never collide with the
BOM's composed children) that reconcile to Ready independently of the BOM.

## How it fits together

- **CRD bootstrap**: see [CRDs applied](#crds-applied-via-conctl-once-the-api-is-up) —
  the path-list loads the core `testfixtures/` **and** this demo's.
- **The worker** ([cmd/worker](cmd/worker/main.go)) is ~15 lines — it lists its
  providers and calls `sdk-go/converge.Serve`.
- **Value flows**: `networking` is the reference for composer-emitted `DepEdge` +
  `ValueFlow` — `vpc` produces an id in its status, `route`/`tgw` consume it; the
  engine gates the consumer until the producer is Ready and flows the value in. (The
  data-driven demo's `fakeapp → fakevpc + fakedb` is a minimal echo of this.)
- **Reactor**: the `statussink` CRD only registers a `reactor` reaction; the
  editable subscription `reactor-binding-classicbom-to-statussink.json` wires
  `(classicbom, synced) → statussink`. When a BOM finishes syncing, the
  ReactorDispatcher runs statussink's `react` reaction once (at-least-once, deduped).
