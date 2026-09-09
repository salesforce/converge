# Data-driven composition demo

## Quickstart

`cd examples/demos/datadriven && just demo`, then open the UI at http://localhost:8080 and
watch it converge. `Ctrl+C` tears it down. The rest of this README explains what you're
looking at.

Three ways to express composition as **data** (not compiled Go), plus a
**multi-edge dependency + value-flow** simulation — all driven by a generic converge
cluster. The three are DIFFERENT tools for different use cases — a declarative rule, a
declarative graph, and a full program — not three flavours of the same thing:

- **CEL rules** (`celbom`) — a per-rule `when` **selector** over a fixed fan-out. The
  simplest, safest option: non-Turing-complete, one-line predicates, structured
  queryable JSON. Reach for it when every policy is "select teams by a predicate, stamp
  a child from a template."
- **kro-style CEL resource graph** (`stdcel`, a shipped built-in) — a whole **graph**
  of children declared as data, fields wired with CEL (`${vpc.status.id}`), deps +
  value flows **inferred** from the references. Reach for it to declare an arbitrary
  DAG without writing a program — kro's `ResourceGraphDefinition` model.
- **Starlark program** (`stdstarlark`, a shipped built-in) — a full, sandboxed
  **program** (`compose(spec, config)`, a multi-file `.star` bundle with loops,
  branches, helpers). Reach for it when the composition needs genuine computation a
  rule or a static graph can't express — the escape hatch.

> **celbom vs stdcel** — both are CEL, so it's worth being explicit: in **celbom** CEL
> is a **selector** (a `when` predicate deciding which teams get a fixed template, with
> deps `depends_on`-declared), while in **stdcel** CEL is the **wiring** of an arbitrary
> graph (expressions connect every field; deps + flows are *inferred* from the
> `${dep.status.x}` references — no `when`, no `depends_on`). celbom = "select teams,
> stamp a template"; stdcel = "declare a graph, CEL connects the fields."

The two expression languages:

- **CEL** ([cel-go](https://github.com/google/cel-go)) is Google's Common
  Expression Language — a small, **non-Turing-complete** expression language (it
  provably terminates, can't loop or recurse). `celbom` uses it for a one-line `when`
  selector; `stdcel` uses it to wire every field of a graph.
- **Starlark** ([go.starlark.net](https://github.com/google/starlark-go)) is
  Google's Python-like configuration/scripting language with a deterministic,
  sandboxed (no I/O) interpreter — a **full program** with loops/branches, the most
  expressive of the three (and the reason `stdstarlark` exists alongside the two CEL
  composers: some logic needs a program, not a predicate or a static graph).

The point is that converge treats composition as a plug-in: it can be implemented in
virtually anything — compiled Go for full programmatic control, declarative CEL rules
for safe selector+template config, a [kro](https://kro.run)-style CEL resource graph
for a whole DAG as data, a Starlark program for sandboxed logic-as-data, or your own
engine. Same core, same typed `Outcome`, same value-flows / rollup / fence either way —
you pick the expressiveness/safety trade-off that fits the policy.

## The business problem

**Provision every team with its standard resources — but let the *provisioning
policy itself* be owned and changed by non-engineers, without a code deploy.** It's
the same landing-zone problem as the [classic demo](../classic/) (a BOM of functional
domains × service teams, each needing a baseline footprint provisioned in dependency
order), but here the twist is *who controls the rules and how fast they can change*.

In a real platform org the composition policy isn't static: a governance/security
team decides *which* teams get *what* (this domain also needs a database; that domain
is exempt; a new compliance rule applies to everyone). With a compiled Go composer,
every such change is a code edit, review, build, and redeploy of the provisioning
engine. This demo instead keeps the policy as **data** the engine reads live:

- a security engineer edits a **CEL rule** (`when team.fd == "fd-03"`), a **CEL graph**
  field, or a **Starlark program** (`fd_selector`, the `compose()` logic), then
  re-applies it —
- the change is pushed to running workers and the next compose/resync recomposes the
  whole org to match, **with no rebuild or redeploy of converge.**

So the business value is the same provisioned graph as classic (per team: an app and
its VPC + database, wired by value flows), but the *rules that generate it* live in
auditable, hot-swappable config a policy owner controls — turning "change what every
team gets" from an engineering release into a config edit.

## What this demo shows

1. **Composition is data** — three composers (`celbom`, `stdcel`, `stdstarlark`) turn
   inputs into child DAGs, but express the composition logic as *data* (CEL rules in
   the config spec, a kro-style CEL resource graph in the config spec, or a Starlark
   program in the config bundle) rather than compiled Go. Edit the data +
   re-apply → recompose, **no redeploy**. They are DIFFERENT tools (rule / graph /
   program), each with its own expressiveness/safety trade-off — see the intro.
2. **Two declaratively-applied DAGs** — each BOM composer emits two graphs per team,
   selected by data (a celbom rule / a stdstarlark policy's `dag` field):
   - **app** — `fakeapp` depends on BOTH `fakevpc` (flows `vpc_id`) and `fakedb`
     (flows `db_endpoint`) — multi-edge value flow.
   - **pipeline** — `fakek8sjob` depends on `faketerraform`: terraform "runs" the
     team's `*.tar` bundle from S3 and produces an `image_tag`; the engine flows it
     into the job's `built_image`, so the Job runs the image the apply built.
3. **Literal inputs in the policy** — the team-specific inputs each kind needs
   (faketerraform's `tar_url`, fakek8sjob's `image`) are literals the rule/policy
   bakes in, so "run this team's Terraform / this image" is a config edit.
4. **Composer labels** — every composed child carries `{composed_by,
   functional_domain, team, policy}` labels, so the composers' outputs are
   queryable + distinguishable.

The three composers — **distinct tools**, not interchangeable:

| Composer | Style | Where the logic lives | Reach for it when |
|---|---|---|---|
| [`celbom`](celbom/) (demo-local) | **CEL rules** — rule data + a one-line `when` selector per rule (fixed Go fan-out) | providerconfig **spec** (structured, queryable JSON) | every policy is "select teams by a predicate, stamp a child from a template" — the simplest/safest option; non-Turing-complete |
| [`stdcel`](../../../internal/providers/stdcel) (shipped built-in) | **kro-style CEL resource graph** — a whole graph of children, fields wired with CEL (`${vpc.status.vpc_id}`), deps + flows **inferred** | providerconfig **spec** | you want to declare an arbitrary DAG as data without a program — kro's `ResourceGraphDefinition` model |
| [`stdstarlark`](../../../internal/providers/stdstarlark) (shipped built-in) | **Starlark program** — a full `compose(spec, config)` (a multi-file `.star` bundle: `compose.star` + `load()`-ed `policies/*.star` + `emit.star`), loops/branches/helpers | providerconfig **bundle** (`data`, a `.star` zip) | the composition needs genuine computation a rule or static graph can't express — the escape hatch |

`celbom` and `stdstarlark` are NOT redundant: a CEL **rule** is a bounded selector (safe, auditable, can't loop), while a Starlark **program** is arbitrary logic (maximally flexible, but opaque code). This demo runs celbom + stdstarlark side by side over the same BOM precisely to show that trade-off — pick the least powerful tool that expresses your policy. The leaf DAGs both fan out into:

| Leaf DAG | What it shows |
|---|---|
| [`fakeapp`](fakeapp/) → [`fakevpc`](fakevpc/) + [`fakedb`](fakedb/) | the **app** DAG: a multi-edge value flow — `fakevpc` produces `vpc_id`, `fakedb` produces `db_endpoint`; two `DepEdge`+`ValueFlow`s gate `fakeapp` until BOTH are Ready and flow each value in. A Ready `fakeapp` proves both edges + flows worked. |
| [`fakek8sjob`](fakek8sjob/) → [`faketerraform`](faketerraform/) | the **pipeline** DAG: `faketerraform` fakes "run a team's Terraform from S3" (`spec.tar_url` → `status.image_tag`); `fakek8sjob` fakes "run a team's K8s Job" (`spec.image`), but depends on the apply and the engine flows its `image_tag` into `built_image` (built wins). So the Job runs the image the apply built. |

`celbom` is a **demo-local** example provider (imports only `sdk/*` + `cel-go`); `stdcel` + `stdstarlark` are **shipped built-ins** (`internal/providers/*`, hosted by `stdworker`) reused here. All import only `sdk/*` — the generic converge core (control + broker) carries no provider code.

## Run the demo

First time only, from the repo root: `just setup` (verify the toolchain) then
`just gen` (regenerate all generated code — sqlc, proto stubs, goldens, client).
Then, from this directory:

```bash
just demo                 # 3 control + 3 brokers + 3 workers
just demo 6                # override the worker count
```

That builds the converge binary + this demo's worker, brings up the cluster,
applies the CRDs (this demo's kinds, via conctl once the API is up), applies the
celbom rule set + the stdcel graphs (default + custom) + the stdstarlark `.star`
program bundle, and posts the two BOMs (celbom + stdstarlark) + both stdcel instances.
Watch it converge in the UI
(http://localhost:8080) or with the [`conctl`](../../../cmd/conctl/README.md) CLI —
`just demo` builds it at `bin/conctl` (run `./bin/conctl …` from the repo root, or
`just install` to put `conctl` on your `PATH`):

```bash
conctl list resources          # the composition roots + child counts
```

Expect the celbom + **stdstarlark** roots Ready, each with **125 children** (per
matching team BOTH DAGs Ready): the **app** DAG (`fakeapp` wired to its `fakevpc` +
`fakedb`) and the **pipeline** DAG (`fakek8sjob` wired to its `faketerraform`). The
pipeline proof: each job's `status.ran_image` is the image the apply built
(`registry.internal/<team>:built`), **not** the literal `nginx:1.27` — the
`image_tag → built_image` flow delivered. `stdstarlark` proves the SHIPPED generic
Starlark built-in runs a full multi-file `.star` program to do the BOM fan-out (the
program's escape-hatch power), while `celbom` does the same DAG from bounded CEL rules
(the safe/declarative option). Both `stdcel` roots are also Ready: `kro-happy-newton`
(DEFAULT graph) with its 3 `kro-…` app-DAG children, and `kro-eager-lovelace` (CUSTOM
graph) with 5 children forming the SAME app + pipeline DAG — proving the generic graph
wires the identical value flows on both config paths. `Ctrl+C` tears down the cluster +
Postgres.

> This demo has no fault-injection recipe — for "watch the engine retry & heal"
> chaos, see the [classic demo](../classic/)'s `just inject-faults`.

### CRDs applied (via conctl, once the API is up)

The launcher applies this demo's [`testfixtures/`](testfixtures/) kind manifests
client-side with `conctl` once the control API is up; the set is
**self-contained** — every kind the composers emit lives here:

| CRD | Kind | Role |
|---|---|---|
| `celbom.kind.json` | `celbom` | composer root — CEL-rule data in the config **spec** (demo-local) |
| `stdcel.kind.json` | `stdcel` | composer root — kro-style CEL **resource graph** in the config **spec** (shipped built-in) |
| `stdstarlark.kind.json` | `stdstarlark` | composer root — generic Starlark program (multi-file `.star` zip) in the config **bundle** (shipped built-in) |
| `fakeapp.kind.json` | `fakeapp` | app DAG leaf — CONSUMES `vpc_id` + `db_endpoint` |
| `fakevpc.kind.json` | `fakevpc` | app DAG leaf — PRODUCES `status.vpc_id` from `account_id` |
| `fakedb.kind.json` | `fakedb` | app DAG leaf — PRODUCES `status.db_endpoint` from `account_id` |
| `faketerraform.kind.json` | `faketerraform` | pipeline DAG leaf — runs a team's `*.tar` from S3 (`tar_url` → `status.image_tag`) |
| `fakek8sjob.kind.json` | `fakek8sjob` | pipeline DAG leaf — runs a team's K8s Job (`image`); CONSUMES the apply's `image_tag` |

### Resources generated (from the BOMs)

Both BOMs ([`resource-celbom.json`](testfixtures/resource-celbom.json),
[`resource-stdstarlark.json`](testfixtures/resource-stdstarlark.json)) use the shared
`deployment_instance` shape (5 functional domains × 5 service teams = 25 teams each).
Per matching team a composer emits TWO DAGs:

```
app DAG (celbom rule "stack" / stdstarlark policy dag="app"):
  fakeapp  ──depends on──▶  fakevpc   (flows status.vpc_id      → spec.vpc_id)
          └─depends on──▶  fakedb    (flows status.db_endpoint  → spec.db_endpoint)

pipeline DAG (celbom rule "pipeline" / stdstarlark policy dag="pipeline"):
  fakek8sjob ──depends on──▶  faketerraform  (flows status.image_tag → spec.built_image)
    (faketerraform tar_url + fakek8sjob image are LITERALS the rule/policy bakes in)
```

The two BOM composers (`celbom`, `stdstarlark`) each produce
25 × 5 = **125 children** per team-batch — 25 each of fakeapp / fakevpc / fakedb /
fakek8sjob / faketerraform — + 25 × 3 = 75 edges. Child names are prefixed `cel-`
(celbom) / `sstar-` (stdstarlark) so the composers never collide (`uniq_resource_meta`
is global). Both emit the **same DAG shape** — one via CEL rules, one via a Starlark
program — the point being you can pick either expressiveness level for the same result.

**`stdstarlark`** ([`resource-stdstarlark.json`](testfixtures/resource-stdstarlark.json))
takes the BOM spec and fans it out into the 125-child DAG via the shipped **generic**
`stdstarlark` built-in's `compose(spec, config)` contract, from a **multi-file `.star`
program** ([`testfixtures/stdstarlark/`](testfixtures/stdstarlark/): `compose.star` +
`policies/stack.star` + `policies/pipeline.star` + `emit.star`, all zipped into the
providerconfig **bundle**). `compose.star` `load()`s the policy files (each a
`policy()`) and the emit module — so `stdstarlark` **assembles and uses all the `.star`
files** in the bundle; the "gather policies + fan out" convention lives in the `.star`
program (the provider stays generic). Adding a policy is a new `.star` file + a
`load()` line in `compose.star`, re-zip + re-upload — no redeploy. This is the
**escape-hatch** path: a full program, for logic a CEL rule (celbom) can't express.

The **`stdcel`** instances are different in shape: not a BOM to fan out, but **one
team's inputs** (`fd`, `team`, `account`, `image`) each, from which the provider
**infers** the DAG order + value flows out of the graph's `${…}` references. The demo
ships TWO — one on each config-delivery path:

- **`kro-happy-newton`** ([`resource-stdcel-default.json`](testfixtures/resource-stdcel-default.json))
  carries no `provider_config_ref`, so it composes from the kind **DEFAULT** graph
  ([`providerconfig-stdcel-default.json`](testfixtures/providerconfig-stdcel-default.json),
  `is_default:true`) — the leaner **app DAG** (vpc + db + app): **3 children + 2 flows**.
- **`kro-eager-lovelace`** ([`resource-stdcel.json`](testfixtures/resource-stdcel.json))
  attaches a **CUSTOM** graph by name (`provider_config_ref: "stdcel-graph"`,
  [`providerconfig-stdcel.json`](testfixtures/providerconfig-stdcel.json),
  `is_default:false`) — the richer **app + pipeline DAG** (adds `job → tf`, a
  cross-field `/image_tag → /built_image` flow): **5 children + 3 flows**, the per-team
  unit celbom/stdstarlark stamp 25 of.

So the demo exercises **both** config paths: celbom/stdstarlark + `kro-happy-newton`
use the shared DEFAULT (edit once, recompose all — the kro RGD model), while
`kro-eager-lovelace` uses a per-instance CUSTOM override. To provision N teams you
apply N `stdcel` instances with different `${schema.spec.*}` inputs, whereas
celbom/stdstarlark fan one BOM into all teams.

## Apply resources manually (sample fixtures)

Each leaf kind ships a standalone sample under [`testfixtures/`](testfixtures/) so you
can create one **by hand** — no composer, no BOM. Because there's no composer to flow
upstream values, a manual leaf supplies the fields directly (each validated against
the kind's `spec_schema`, so they're guaranteed to apply):

| Sample | Kind | Reaches Ready by… |
|---|---|---|
| [`resource-fakevpc.json`](testfixtures/resource-fakevpc.json) | `fakevpc` | `account_id` → `status.vpc_id = "vpc-<account_id>"` |
| [`resource-fakedb.json`](testfixtures/resource-fakedb.json) | `fakedb` | `account_id` → `status.db_endpoint = "db-<account_id>.internal:5432"` |
| [`resource-fakeapp.json`](testfixtures/resource-fakeapp.json) | `fakeapp` | `vpc_id` + `db_endpoint` supplied directly → `status.wired_to` + `status.db` |
| [`resource-faketerraform.json`](testfixtures/resource-faketerraform.json) | `faketerraform` | `tar_url` (`s3://…/*.tar`) → `status.apply_id` + `status.image_tag` |
| [`resource-fakek8sjob.json`](testfixtures/resource-fakek8sjob.json) | `fakek8sjob` | `image` supplied directly → `status.job` + `status.ran_image` |

With a cluster up (`just demo`):

```bash
conctl apply --type resource -f testfixtures/resource-faketerraform.json
# repeat for resource-{fakevpc,fakedb,fakeapp,fakek8sjob}.json
```

These create top-level resources (named `sample-*`, so they never collide with the
composed `cel-` / `sstar-` / `kro-` children) that reconcile to Ready independently of
the BOMs.

## How it fits together

- **CRD bootstrap**: see [CRDs applied](#crds-applied-via-conctl-once-the-api-is-up) —
  `KIND_FIXTURES_DIR` is a path-list; this demo's dir is self-contained.
- **The worker** ([cmd/worker](cmd/worker/main.go)) is ~15 lines — it lists its
  providers and calls `sdk-go/converge.Serve`. The same shape an external team writes.
- **`apply-bundle`** zips the `.star` files (`testfixtures/stdstarlark/`, recursively —
  `compose.star` + `policies/*.star` + `emit.star`) and uploads the zip as the
  `stdstarlark` kind's default providerconfig bundle — run it again after editing a
  `.star` to change composition with no redeploy.
