# stdio demo

## Quickstart

`cd examples/demos/stdio && just demo`, then open the UI at http://localhost:8080 and watch
it converge. `Ctrl+C` tears it down. The rest of this README explains what you're looking at.

Make **any external program** a converge resource — in any language, with no
converge Go SDK. The shipped [`stdio`](../../../internal/providers/stdio) provider (run by
[`bin/stdworker`](../../../cmd/stdworker)) runs a program per task, pipes the
task in as one **JSON Request on stdin**, and reads the outcome as one **JSON
Response on stdout** — the *converge stdio protocol*. So adding a new kind of work
is a *config edit + a script*, not a Go change. The classic case is *"run some
arbitrary bash and record the result"*, but the program can be anything: it just
has to read one JSON object and print one.

This demo ships **no Go of its own** — the stdworker is a first-class binary. It
provides only the reference protocol handler ([handler.sh](testfixtures/handler.sh))
and the fixtures. The provider interprets **none** of the resource's spec, config,
or bundle — they are opaque bytes forwarded to the program, which owns their schema.
Converge still reconciles the resource like any other kind: retry on transient
failure, roll a composer's children up into its status, delete children with root.

## What this demo shows

- **A worker that hosts an arbitrary program.** The Go worker is the runner: it
  resolves the command (from the providerconfig or the `STDIO_COMMAND` env var),
  marshals the task to the child's stdin, and maps its stdout back to an `Outcome`,
  doing all the DB/fencing on the program's behalf. The reference program here is a
  bash [`handler.sh`](testfixtures/handler.sh) using `jq` — **no Go per use case**.
- **Two roles from one provider.** The same code + protocol serves two kinds:
  - **`stdio`** — a **leaf** worker (`specChange → status`): runs `spec.run` as a
    shell command and records its exit code + output. "Run arbitrary bash."
  - **`stdio-composer`** — a **composer** (`specChange → children + status`): fans
    out one child per entry in `spec.children` — each at `spec.child_kind` /
    `spec.child_kind_version` (a composer pins the exact child `kind_version`) and
    named `<spec.name_prefix>-<entry>` — i.e. the celbom/stdstarlark capability from
    a plain script.
- **Failure semantics via exit code.** `exit 0` = success (parse stdout); `exit 2`
  = **terminal** (a bad spec/config a retry can't fix — the engine stops retrying);
  any other non-zero = **transient** (retried with backoff). `stderr` is captured
  into the failure message. Empty stdout is a valid empty outcome (a pure
  side-effect program).
- **Live config + cancellation.** The command/settings are read **live** per task
  from the providerconfig (an operator edit lands with no restart), and the child
  runs under the task's context — a drain, lease loss, or the config
  `timeout_seconds` **kills the process**, so no run outlives its claim.

This demo uses the built-in default kind names (`stdio` / `stdio-composer`). A real
deployment gives each use case its OWN named kind (e.g. `backups`, `dns`) via the
worker's `STDIO_KINDS` / `STDIO_COMPOSER_KINDS` env — see
[cmd/stdworker](../../../cmd/stdworker/README.md#naming-your-own-stdio-kinds).

| Kind | Role | What the program returns |
|---|---|---|
| [`stdio`](../../../internal/providers/stdio/stdio.kind.json) | leaf (`work`) | `response.status` (+ optional `conditions`) |
| [`stdio-composer`](../../../internal/providers/stdio/stdio-composer.kind.json) | composer (`compose`) | `response.children` + `response.edges` (+ `status`) |

## The protocol

One-shot subprocess, one JSON object each way (see
[internal/providers/stdio/types.go](../../../internal/providers/stdio/types.go)):

```
stdin  ← {"reaction":"work","trigger":"specChange","kind":"stdio","name":"hello",
          "generation":1,"spec":{…},"config":{…},"bundle":"<base64>"}
stdout → {"status":{…},"conditions":[…],"children":[…],"edges":[…]}
exit    0 = ok  |  2 = terminal (no retry)  |  other = transient (retry)
stderr  = captured for logs / the failure message
```

`spec` and `config` are passed through **verbatim** (the program owns their
shape); a `compose` request also carries the currently-owned children in
`observed` so a program can diff desired-vs-observed. A `work` reaction's response
`children`/`edges` are ignored — only `stdio-composer` shapes a graph.

## Prerequisites

The repo root's `just setup`, plus **`bash` + `jq`** on PATH (the reference
handler uses them). No Docker — the workers run on the host.

## Run the demo

First time only, from the repo root: `just setup` then `just gen`. Then, from this
directory:

```bash
just demo                 # 3 control + 3 brokers + 3 stdworkers (host)
```

`just demo`:

1. builds the converge binary + the shipped `bin/stdworker` (via the root build);
2. brings up the cluster on the host via `deploy/dev-local.sh`, exporting
   `STDIO_COMMAND=testfixtures/handler.sh` so the stdworker runs the reference
   handler (the providerconfigs set no `command`, falling back to it);
3. applies both CRDs (`stdio` + `stdio-composer`) **from the provider package**
   (`internal/providers/stdio/`), their providerconfigs, and two resources — a bash
   leaf and a BOM that fans out three leaves.

Watch it converge in the UI (http://localhost:8080) or with the
[`conctl`](../../../cmd/conctl/README.md) CLI — `just demo` builds it at `bin/conctl` (run it
from the repo root as below, or `just install` to put `conctl` on your `PATH`):

```bash
bin/conctl get resource stdio/hello-agent   # status = { run, exit_code, output, ok } (addressed by kind/name)
bin/conctl list resources                    # hello-bom + its 3 composed stdio children → Ready
```

`Ctrl+C` tears down the cluster and Postgres.

## Register the kind / add a providerconfig / create a resource

`just demo` does this for you; the same three steps by hand (from the repo root)
show the split between the **shipped provider's CRD** and the **demo's** config +
workload. Register the kind, tell converge where/how to run it, then create work:

```bash
# 1. register the kind — the CRD ships WITH the stdio provider:
conctl apply --type manifest -f internal/providers/stdio/stdio.kind.json
conctl apply --type manifest -f internal/providers/stdio/stdio-composer.kind.json

# 2. add the providerconfig — where/how to run (demo defaults: command from
#    STDIO_COMMAND, timeout_seconds):
conctl apply --type providerconfig -f examples/demos/stdio/testfixtures/providerconfig-stdio.json
conctl apply --type providerconfig -f examples/demos/stdio/testfixtures/providerconfig-stdio-composer.json

# 3. create a resource — a bash leaf and a BOM that fans out three leaves:
conctl apply --type resource -f examples/demos/stdio/testfixtures/resource-stdio.json
conctl apply --type resource -f examples/demos/stdio/testfixtures/resource-stdio-composer.json
```

The manifest is a plain document applied by path — the worker never reads it at
runtime; it is operator-applied and lives in the provider dir. The CRD must exist
before a resource of that kind validates, so apply it first.

## Files

| File | Role |
|---|---|
| [`internal/providers/stdio/stdio.go`](../../../internal/providers/stdio/stdio.go) | the provider (shipped, not demo): `OnConfig` + `Work` (build Request, run program, map Response → Outcome) |
| [`internal/providers/stdio/types.go`](../../../internal/providers/stdio/types.go) | the two kind names + `Config` (command/args/env/settings/timeout) + the wire `Request`/`Response` shapes |
| [`internal/providers/stdio/stdio.kind.json`](../../../internal/providers/stdio/stdio.kind.json) | the leaf CRD (opaque spec/status/config + a `specChange → status` reaction) — ships WITH the provider |
| [`internal/providers/stdio/stdio-composer.kind.json`](../../../internal/providers/stdio/stdio-composer.kind.json) | the composer CRD (a `specChange → children + status` reaction) — ships WITH the provider |
| [`cmd/stdworker`](../../../cmd/stdworker) | the shipped worker binary that hosts the provider (its README covers run flags) |
| [`testfixtures/handler.sh`](testfixtures/handler.sh) | the canonical bash+jq protocol handler: `work` runs `spec.run`; `compose` fans out `spec.children` |
| [`testfixtures/providerconfig-stdio.json`](testfixtures/providerconfig-stdio.json), [`-stdio-composer.json`](testfixtures/providerconfig-stdio-composer.json) | the defaults (command from `STDIO_COMMAND`; `timeout_seconds`) |
| [`testfixtures/resource-stdio.json`](testfixtures/resource-stdio.json) | a leaf whose `spec.run` echoes host + timestamp |
| [`testfixtures/resource-stdio-composer.json`](testfixtures/resource-stdio-composer.json) | a BOM that composes three `stdio` children |

## How it fits together

```
stdio resource (spec.run = "echo hello…")        stdio-composer resource (spec.children = [a,b,c])
        │  specChange → work                              │  specChange → compose
        ▼                                                 ▼
stdworker ── STDIO_COMMAND=handler.sh          stdworker ── same handler.sh
        │  stdin ← {reaction:"work", spec}               │  stdin ← {reaction:"compose", spec, observed}
        │  handler: bash -c "$spec.run"                  │  handler: one stdio child per name
        │  stdout → {status:{run,exit_code,output,ok}}   │  stdout → {children:[…], status:{composed:3}}
        ▼                                                 ▼
status = the command result                       3 stdio children created (each runs its own bash) → roll up
```

The `stdio` provider is **shipped** (`internal/providers/stdio`), so its two CRDs
live **with it** — the co-located manifest signals *"this is a shipped provider; the
CRD defines what it is."* The demo's `testfixtures/` therefore hold only the
demo-specific pieces: the providerconfigs (where/how to run) and the sample
resources. (Contrast the [`classic`](../classic/) and [`datadriven`](../datadriven/)
demos: those providers are **fake, demo-only**, so both their provider code *and*
their `*.kind.json` live entirely under `examples/demos/`.)

Point `STDIO_COMMAND` (or a providerconfig `command`) at any program that speaks
the JSON protocol — a Python script, a compiled binary, a WASM shim — and the same
two kinds drive it. The bundle field carries an opaque artifact (e.g. a zip the
program interprets), so a program's *logic* can itself be config, not just inputs.
