# stdworker

The **default converge worker**: a shipped binary that hosts converge's built-in,
general-purpose **std\*** providers. It holds **no database handle** — it dials one
broker, pulls work, runs the reaction, and reports back; the broker does all the
DB/fencing.

By default it advertises **all** of its providers' kinds — `stdio`, `stdio-composer`,
`stdshell`, `stdterraform`, `stdcel`, `stdstarlark` (the `stdio` names are renameable, see
below). But *advertising* a kind is not *running* it: a hosted kind only claims work once
you **apply that kind's CRD + providerconfig** over the API — a worker that boots first
just waits. Scope a fleet down to a subset with `WORKER_KINDS` (e.g.
`WORKER_KINDS=stdterraform` advertises only that one). So the three layers are: the binary
hosts every std\* kind, `WORKER_KINDS` narrows *which* it advertises, and the applied CRD
is what makes an advertised kind actually do work.

It hosts five providers:

| Provider | Kind(s) | What it does | Kind naming |
|---|---|---|---|
| [`stdio`](../../internal/providers/stdio) | `stdio` (leaf), `stdio-composer` (composer) | Runs an **external program** (any language, no converge Go SDK): task JSON on stdin → outcome JSON on stdout — the *converge stdio protocol*. A composer program fans out a child graph. | `STDIO_KINDS` / `STDIO_COMPOSER_KINDS` **override** the defaults (one fleet, many named kinds). |
| [`stdshell`](../../internal/providers/stdshell) | `stdshell` (leaf) | Runs a script carried **inline** in `spec.script`, under a configurable interpreter (bash by default). Captures the exit result + output as status. | **Fixed** kind `stdshell` (no env var). |
| [`stdterraform`](../../internal/providers/stdterraform) | `stdterraform` (leaf) | Runs a real `terraform`/`tofu` **apply** on a user's module (from **S3**, **GCS**, **Azure Blob**, **HTTPS**, or a **git** repo); the module's outputs become status. Multi-cloud state backend (s3/azurerm/gcs, native locking). On delete it runs **destroy** (teardown). | **Fixed** kind `stdterraform` (no env var). |
| [`stdcel`](../../internal/providers/stdcel) | `stdcel` (composer) | The **generic, kro-style composer**: a **resource graph definition as data** (the providerconfig spec) — child resources whose fields are **CEL** expressions over the instance's spec (`${schema.spec.x}`) and over each other (`${vpc.status.id}`). Infers the dependency order + value flows from the references. One provider, **any** graph, no code. | **Fixed** kind `stdcel` (no env var). |
| [`stdstarlark`](../../internal/providers/stdstarlark) | `stdstarlark` (composer) | The **generic Starlark composer**: an operator-supplied **Starlark program** (a `.star` zip in the providerconfig **bundle**, `compose(spec, config)`) fans a resource's spec out into a child DAG. Sandboxed (no I/O), config-parameterized, `load()`-able modules. One provider, **any** program, no code. | **Fixed** kind `stdstarlark` (no env var). |

Every provider imports only `sdk-go/*` (never `internal/*`), so each is also the
**reference template** for an out-of-tree provider an external team ships. See each
package's doc comment for its protocol/spec, and
[`examples/demos/`](../../examples/demos) for runnable end-to-end demos
(`stdio`, `stdshell`, `stdterraform`, and the `stdcel`-driven
[`datadriven`](../../examples/demos/datadriven) composition demo).

## Build

```bash
just build          # from the repo root → produces bin/stdworker
# or:
go build -o bin/stdworker ./cmd/stdworker
```

## Run

The worker always needs a **broker to connect to** (`BROKER_ADDR`). What else it
needs depends on which kinds you drive:

```bash
# stdio: a program to run (per-kind providerconfig `command`, or this fallback)
BROKER_ADDR=http://localhost:9090 STDIO_COMMAND=/path/to/handler.sh bin/stdworker

# stdshell / stdterraform / stdcel / stdstarlark: nothing extra on the worker — the
# script is in the spec, stdterraform's state backend is the providerconfig, stdcel's
# graph is the providerconfig spec, and stdstarlark's program is the providerconfig bundle.
BROKER_ADDR=http://localhost:9090 bin/stdworker
```

It stays running until `SIGTERM`/`SIGINT`, draining in-flight tasks first. To run a
**narrow** fleet (only some of the hosted kinds), scope it with `WORKER_KINDS` (e.g.
`WORKER_KINDS=stdterraform` runs only that provider's kind).

## Make a kind do work — apply its CRD

A booted worker claims nothing until the kind it hosts is **taught to the control
plane**. Teaching a kind is applying its **CRD** (the `KindManifest` — schemas +
reactions + policy) to the running control plane; the worker + control plane are
decoupled (k8s-CRD model — the manifest is applied out of band from the handler code),
so the order doesn't matter: apply the CRD before or after the worker boots. Per kind,
in order:

1. **Apply the CRD** (once per `(kind, kind_version)`). Every std\* provider ships its
   reference CRD next to the provider code as `<kind>.kind.json`
   (e.g. [`internal/providers/stdshell/stdshell.kind.json`](../../internal/providers/stdshell/stdshell.kind.json),
   [`internal/providers/stdio/stdio.kind.json`](../../internal/providers/stdio/stdio.kind.json)).
2. **Apply the providerconfig** — the kind's live-editable default config (`stdio`'s
   `command`, `stdterraform`'s state backend, `stdcel`'s graph, `stdstarlark`'s `.star`
   bundle), created the same way as any object over the API.
3. **Apply resources** of that kind — they queue in `work_queue` until the worker claims
   them.

Three ways to apply a CRD, all hitting the same `PUT /api/kinds/{kind}/manifest` endpoint:

```bash
# a) conctl (the kubectl-style CLI) — --type crd | manifest | kind all work:
conctl apply --type crd -f internal/providers/stdshell/stdshell.kind.json --server http://localhost:8080

# b) raw REST — the CRD JSON is the request body verbatim:
curl -sf -X PUT http://localhost:8080/api/kinds/stdshell/manifest \
  -H 'Content-Type: application/json' \
  --data-binary @internal/providers/stdshell/stdshell.kind.json
```

**c) the UI:** open the web console (served by a `control` pod), go to a kind's page, and
click **Apply CRD** — it PUTs the same manifest.

## The providers

### `stdio` — run an external program (any language)

Per task the worker runs your program **once**, writes exactly one JSON **Request**
object to its stdin, and reads exactly one JSON **Response** object from its stdout. It
serves a leaf (`stdio`, `specChange → status`) and a composer (`stdio-composer`,
`specChange → children + status`) — the same program handles both, told which via
`request.reaction`. This is the *converge stdio protocol* (the authoritative Go shapes are
`Request`/`Response` in
[`internal/providers/stdio/types.go`](../../internal/providers/stdio/types.go)).

**Input — the Request on stdin** (one JSON object; a field is omitted when empty):

| Field | Type | Meaning |
|---|---|---|
| `reaction` | string | `"work"` (leaf) or `"compose"` (composer) — which reaction fired. |
| `trigger` | string | why it fired, e.g. `"specChange"` / `"resync"` — informational. |
| `kind` | string | the resource's kind. |
| `name` | string | the resource's name (stable across generations). |
| `generation` | integer | the spec generation — key idempotent side effects on it. |
| `spec` | JSON | the resource's spec, **verbatim opaque JSON** (the program owns its schema). |
| `config` | JSON | the **effective providerconfig `settings`** blob, verbatim opaque JSON (see below). |
| `bundle` | string | the **effective bundle** bytes, **base64-encoded** (an opaque artifact, e.g. a zip). Omitted when there is no bundle. |
| `observed` | array | **compose only** — the currently-owned children, each `{kind,name,ready,status}`, so a composer can diff desired-vs-observed. |

**Output — the Response on stdout** (one JSON object; fill only what your reaction may
emit — the core drops parts the manifest's `emits` mask forbids; **empty stdout is a valid
success** = empty outcome):

| Field | Type | For | Meaning |
|---|---|---|---|
| `status` | JSON | work + compose | the resource's new status document, **stored verbatim**. |
| `conditions` | array | work + compose | health/custom axes: `{type, status, reason?, message?}`, `status` ∈ `"True"｜"False"｜"Unknown"`. |
| `children` | array | **compose only** | desired children: `{kind, kind_version, name, spec, labels?}` — `kind_version` is **required, ≥ 1** (no implicit v1). |
| `edges` | array | **compose only** | dependency edges: `{from:{kind,name}, to:{kind,name}, values?}` (`from` depends on `to`). |
| `edges[].values` | array | **compose only** | value flows: `{dependent_field, source_field}` — RFC-6901 JSON Pointers (`"/account_id"`); fills the dependent's spec field from the upstream's status field once it's Ready. |

**Exit code = failure class:**

```
exit 0     success — parse stdout as the Response
exit 2     TERMINAL — a bad spec/config a retry can't fix; the engine stops retrying
exit other TRANSIENT — retried with backoff
stderr     captured into logs / the failure message (any exit)
```

**How `config` / `bundle` are resolved (the effective providerconfig).** The kind's
`Config` (`command` / `args` / `env` / `timeout_seconds` / `settings`) splits in two: the
worker **consumes** `command`/`args`/`env`/`timeout_seconds` to *launch* the process (the
program never sees them as data), and **forwards only the opaque `settings` blob** as
`request.config`. Both `request.config` and `request.bundle` are the **effective** values —
the kind's default providerconfig **merged with the resource's per-resource override**
(`settings` deep-merges, override wins per key; the bundle whole-replaces). The merge runs
**per task**, just before launch, so an operator's config edit lands on the next reconcile
with no worker restart.

The default kind names `stdio` / `stdio-composer` are **overridable** via
`STDIO_KINDS` / `STDIO_COMPOSER_KINDS`, so one fleet can back many distinct named
external-program kinds (e.g. `backups`, `dns`). Reference handler:
[`examples/demos/stdio/testfixtures/handler.sh`](../../examples/demos/stdio/testfixtures/handler.sh)
(bash + `jq`).

### `stdshell` — run an inline script

`spec.script` holds the script text; the worker writes it to a temp file and runs
`<interpreter> [args…] <script>` under the task's context. `interpreter`, `args`,
`env`, and `timeout_seconds` are per-resource (with kind-wide defaults in the
providerconfig). It captures tail-bounded stdout/stderr into status and emits a
`Ready` condition. Exit codes: `0` = ok, `2` = terminal, any other non-zero =
transient; a timeout/cancel is transient. The script runs in its own process group,
so a timeout kills the whole tree (no orphaned child pins the worker slot). Serves the
**fixed** kind `stdshell`.

```json
{ "kind": "stdshell", "name": "nightly-cleanup",
  "spec": { "script": "#!/usr/bin/env bash\nset -euo pipefail\naws s3 rm s3://tmp --recursive\n",
            "timeout_seconds": 300 } }
```

Fixtures: [`examples/demos/stdshell/testfixtures/`](../../examples/demos/stdshell/testfixtures);
spec/config in [`internal/providers/stdshell/types.go`](../../internal/providers/stdshell/types.go).

### `stdterraform` — apply/destroy a user's Terraform module

`spec.source` points at a module; the worker fetches it, injects a state backend
keyed to the resource's UUID, and runs a real apply (outputs → status). On delete it
runs destroy (teardown) and strips the finalizer **only after** the real infra is
gone. The module ships no backend block — the platform owns state placement + locking
(the Terraform Enterprise model). Serves the **fixed** kind `stdterraform`. Supported
module sources:

- `s3://bucket/key.tar.gz` — a `*.tar.gz` from AWS S3 (`aws` CLI, AWS chain).
- `gs://bucket/key.tar.gz` — a `*.tar.gz` from GCS (`gcloud storage`, active gcloud
  credential / Workload Identity).
- `azblob://<account>/<container>/key.tar.gz` — a `*.tar.gz` from Azure Blob (`az`,
  `--auth-mode login` / Entra ID).
- `https://host/path/mod.tar.gz` — a `*.tar.gz` fetched over HTTPS (`curl`).
- `git::https://host/repo//subdir?ref=REF` — a git repo cloned at `REF`; the optional
  `//subdir` selects a module dir within it.

Each object-store scheme uses that cloud's **ambient identity** — no credentials in the
spec. The module source and the state backend are independent (e.g. a module from GCS
with state in S3).

The **Terraform/OpenTofu version is auto-resolved per module** via **tfenv**
(`binary=terraform`, HashiCorp Terraform / BUSL-1.1) or **tofuenv** (`binary=tofu`,
OpenTofu / MPL-2.0): `spec.tf_version` pins an explicit token (`1.7.5`, `latest`,
`latest:^1.7`, `min-required`, `latest-allowed`); else a `.terraform-version` /
`.opentofu-version` shipped in the bundle; else **`latest-allowed`** — the newest
version the module's own `required_version` permits. The resolved version is
auto-installed (flock-guarded, cached in the image), so the image pins no fixed CLI.

The **state backend is multi-cloud**, set once in the providerconfig `backend` field
with each cloud's native locking auto-on: `s3` (`use_lockfile` — a `.tflock` object,
no DynamoDB, needs terraform/tofu ≥ 1.10), `azurerm` (blob lease), `gcs` (atomic lock
object) — each with its own block (`s3`/`azurerm`/`gcs`) plus `state_prefix` and
`binary` (`terraform` or `tofu`). For any other backend, put a raw `backend.tf` in the
providerconfig `data` bundle (with the `__CONVERGE_STATE_KEY__` placeholder). **Credentials**
are never in the config — each backend reads its cloud's standard chain the worker
inherits (`AWS_*`/IRSA, `ARM_*`/workload identity, `GOOGLE_*`/GKE Workload Identity).

```json
{ "kind": "stdterraform", "name": "hello-bucket",
  "spec": { "source": "git::https://git.host/infra//modules/bucket?ref=v1.2.0",
            "vars": { "name": "converge-hello" } } }
```

Fixtures: [`examples/demos/stdterraform/testfixtures/`](../../examples/demos/stdterraform/testfixtures);
spec/config in [`internal/providers/stdterraform/types.go`](../../internal/providers/stdterraform/types.go).

### `stdcel` — a kro-style resource graph as data

The **generic composer**: you declare a **graph of child resources as data** and wire
any field between them with **CEL**, no per-kind Go — the [kro](https://kro.run)
`ResourceGraphDefinition` model on the converge engine. The graph lives in the
providerconfig **spec**; the `stdcel` resource's OWN spec is the instance inputs.

- The **instance** inputs are read as `${schema.spec.x}` (kro's top-level schema).
- `${other.spec.x}` **inlines** another resource's resolved spec at compose time
  (a wiring for ordering, no runtime edge — the value is baked in).
- `${other.status.x}` becomes a **dependency edge + value flow**: the engine schedules
  `other` first and flows its `status.x` into this child once it is Ready.

**Dependencies and value flows are INFERRED, not declared** — there's no `depends_on`
or `flow` list (contrast the demo's `celbom`, which declares both). The provider reads
the CEL references and derives the graph:

- a resource that references `${dep.…}` **depends on** `dep`; the provider
  topologically sorts the graph, rejecting **cycles / self-references / unknown ids**
  (a terminal error naming the members).
- a `${dep.status.SOURCE}` **whole-value** spec leaf emits a **`ValueFlow`** whose
  **source field** is `SOURCE` (the status path in the expression) and whose
  **dependent field** is *where the leaf sits* in the child spec — so
  `built_image: "${tf.status.image_tag}"` flows `/image_tag → /built_image` (the two
  names may differ). At compose time the leaf resolves to `null`; the engine fills it
  from `dep`'s status before the child runs.

A whole-value `"${expr}"` keeps the CEL result's native type (int/bool/list/map); an
interpolation `"a-${expr}-b"` is a string (and can't be a flow — only whole-value
status leaves flow). Editing the graph + re-applying recomposes with **no worker
redeploy**. Serves the **fixed** kind `stdcel`; its child kinds must have their own
applied CRDs.

```json
{ "resources": [
    { "id": "vpc", "template": { "kind": "fakevpc", "name": "${schema.spec.name}-vpc",
        "spec": { "account_id": "${schema.spec.account}" } } },
    { "id": "app", "template": { "kind": "fakeapp", "name": "${schema.spec.name}-app",
        "spec": { "vpc_id": "${vpc.status.vpc_id}" } } } ] }
```

This is the generalisation of the `datadriven` demo's `celbom` (whose CEL is only a
one-line `when` selector over a fixed BOM fan-out). Runnable end-to-end:
[`examples/demos/datadriven`](../../examples/demos/datadriven). CRD + spec/config in
[`internal/providers/stdcel`](../../internal/providers/stdcel).

### `stdstarlark` — a Starlark composition program as data

The **generic Starlark composer**: you ship a **Starlark program** — a zip of `.star`
files in the providerconfig **bundle** (`data`), with `compose.star` at the root
defining a `compose(spec, config)` function — and it fans a resource's spec out into a
child DAG, no per-kind Go. It's the program-as-data sibling of `stdcel` (rules-as-data),
and the `datadriven` demo uses it as one of its three composers.

- `compose(spec, config)` gets the resource's **own spec** (instance inputs) and the
  **effective providerconfig spec** (`default ⊕ override`) as parameterization **data**
  — so behaviour changes with a config edit, no bundle re-upload.
- It returns `struct(children=[…], edges=[…], configs=[…])`; the host marshals it into
  the typed `Outcome` the core diffs (recompose / prune / fence / value-flows). An edge
  is `struct(dependent=struct(kind,name), dependency=struct(kind,name), values=[…])`;
  a value flow is `struct(dependent="/field", source="/field")`.
- Other `.star` files in the bundle are **loadable modules** via `load("name.star",
  "sym")` (resolved in-memory — no filesystem/network access).
- **Sandboxed & deterministic**: a Starlark thread gets no I/O beyond the host's pure
  builtins (`struct`, glob `match`), and the zip is unpacked in memory, so composition
  is a pure function of `(spec, config, bundle)`. An absent bundle is TRANSIENT (retry
  until it propagates); a malformed bundle / bad program is TERMINAL.
- **Size caps** (zip-bomb defence): the bundle unpacks to at most **4 MiB total** and
  **512 KiB per `.star` file** — a real composition program is a few KB; a bundle over the
  cap fails TERMINAL.

```python
# compose.star (zipped into the providerconfig bundle)
def compose(spec, config):
    children = [struct(kind = "fakevpc", name = spec.name + "-vpc",
                       spec = {"account_id": spec.account})]
    app = struct(kind = "fakeapp", name = spec.name + "-app", spec = {})
    edges = [struct(dependent = struct(kind = "fakeapp", name = app.name),
                    dependency = struct(kind = "fakevpc", name = spec.name + "-vpc"),
                    values = [struct(dependent = "/vpc_id", source = "/vpc_id")])]
    return struct(children = children + [app], edges = edges)
```

Upload the program with the zip-the-`.star`-files step (see the
datadriven demo's `just apply-bundle`). CRD + the Go↔Starlark contract in
[`internal/providers/stdstarlark`](../../internal/providers/stdstarlark).

## Naming your own stdio kinds

The `stdio` provider's default kind names (`stdio` / `stdio-composer`) are overridable
so one fleet can back many use cases — give each its OWN kind (distinct in listings,
with its own CRD, providerconfig, schema, and concurrency cap):

```bash
BROKER_ADDR=http://localhost:9090 \
STDIO_KINDS=backups,dns  STDIO_COMPOSER_KINDS=app-bom \
  bin/stdworker
```

For EACH name apply a matching CRD (a `work` reaction for a leaf, a `compose` reaction
for a composer) and a providerconfig; the demo CRDs are templates — copy one and change
the `kind`. A name listed as both a leaf and a composer is rejected at startup.
(`stdshell` and `stdterraform` are **fixed** kinds — there is no env var to rename them.)

## Configuration (environment)

| Var | Default | Purpose |
|---|---|---|
| `BROKER_ADDR` | — (**required**) | The broker Connect URL to dial, e.g. `http://broker:9090` (or `https://…` for TLS). |
| `STDIO_KINDS` | `stdio` | Comma-separated **stdio leaf** kind names (each gets a `work` reaction). |
| `STDIO_COMPOSER_KINDS` | `stdio-composer` | Comma-separated **stdio composer** kind names (each gets a `compose` reaction). Setting *either* this or `STDIO_KINDS` replaces the stdio defaults for that role. |
| `STDIO_COMMAND` | — | Program the `stdio` provider runs when a kind's providerconfig sets no `command`. |
| `WORKER_KINDS` | all | Comma-separated subset of ALL the resolved kinds (across providers) to actually run — e.g. `stdterraform`. |
| `WORKER_MAX_PARALLEL` | `16` | Max concurrent in-flight tasks on this worker (advertised as its credit). |
| `WORKER_DRAIN_TIMEOUT` | `50s` | On shutdown, how long to let in-flight tasks finish before force-stop. |
| `HEALTH_ADDR` | — | If set (e.g. `:8081`), serve a health endpoint for liveness/readiness probes. |
| `LOG_LEVEL` / `LOG_FORMAT` | `info` / `text` | Logging verbosity and format (`text` or `json`). |
| `TLS_CERT_FILE`, `TLS_KEY_FILE` | — | Client keypair for **mTLS** to the broker (both required to enable it). |
| `BROKER_CA_FILE` | — | Pin the broker's CA (with the mTLS keypair above). |

> A provider's **runtime configuration** (region, endpoints, the stdio program's own
> settings, stdterraform's state backend, …) is NOT a worker env var — it comes from the
> kind's **providerconfig**, applied over the API, so an operator changes it live with
> no restart. Set only the launch knobs above on the worker itself. (The one exception
> is `STDIO_COMMAND`, a convenience fallback for the stdio program's path.)

## Container

The shipped image for this worker is the repo-root
[`Dockerfile.stdworker`](../../Dockerfile.stdworker): a Debian-slim base that bundles
**everything** the three providers shell out to — `tfenv` + `tofuenv` (which install
the Terraform/OpenTofu version each module needs on demand — none is baked in) + `aws` + `gcloud`
+ `az` + `git` + `curl` + `bash` + `jq` — so ONE image can run any of the std\* kinds at
any TF/OpenTofu version. Build it from the repo root with `just docker-stdworker` (the
[stdterraform demo](../../examples/demos/stdterraform) builds and runs this same image).
Cloud credentials come from each provider's ambient chain (`AWS_*`/IRSA, `ARM_*`/workload
identity, `GOOGLE_*`/GKE Workload Identity) — none baked into the image.

(The repo's root [`Dockerfile`](../../Dockerfile) is a SEPARATE, lean image carrying only
the DB-less `converge` control/broker binary + `conctl` — **not** the worker. Roll your
own slimmer worker image if you run only `stdio` + `stdshell` and want to drop the
Terraform CLIs; `distroless/static` won't work — a shell is required for the stdshell
interpreter and the stdio reference handler.)
