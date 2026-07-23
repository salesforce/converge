# Shell demo

## Quickstart

`cd examples/demos/stdshell && just demo`, then open the UI at http://localhost:8080 and watch
it converge. `Ctrl+C` tears it down. The rest of this README explains what you're looking at.

Run a **user-supplied script as a converge resource** — the script text is carried
**inline in the resource spec**, so a resource is entirely self-contained: no
external program to install, no bundle to fetch, the DB row *is* the source of
truth. The shipped [`stdshell`](../../../internal/providers/stdshell) provider (hosted by
[`bin/stdworker`](../../../cmd/stdworker)) writes `spec.script` to a temp
file per task, runs it under a configurable interpreter (`bash` by default) with
the task's context + a timeout, captures stdout/stderr into the status, and emits a
`Ready` condition. Adding a "run this command and record the result" kind is a
*config edit + a script string*, not a Go change.

Unlike the generic [`stdio`](../stdio/) provider — whose spec is fully opaque and
interpreted by a separate stdio program — the stdshell provider interprets the spec
itself. There is nothing to point it at: the script lives in the spec, so this is
the simplest possible "run a command as a resource" kind. Converge still reconciles
it like any other kind — retry on transient failure, dead-letter on terminal
failure, record the outcome as durable status.

This demo ships **no Go of its own** — the stdshell provider is a first-class part of
the shipped `bin/stdworker`. It provides only the providerconfig (kind-wide
defaults) and a sample resource.

## What this demo shows

- **A script as a resource.** The `stdshell` kind's `spec.script` is written to a
  0700 temp file and run as `<interpreter> [args…] <scriptPath>`; the exit code and
  tail-bounded stdout/stderr are recorded as `status`. No external program, no
  fetch, no bundle — the resource is self-contained.
- **How to run it, from spec + config.** A resource picks the `interpreter`, extra
  `args`, extra `env`, and a `timeout_seconds`; kind-wide **defaults**
  (`default_interpreter`, a baseline `env`, `default_timeout_seconds`) come from
  the providerconfig, read **live** per task so an operator edit lands with no
  restart. Env precedence is *worker < config < spec* (the spec wins a key clash).
- **Failure semantics via exit code.** `exit 0` = success (record status + `Ready`);
  `exit 2` = **terminal** (a bad script a same-generation retry can't fix — the
  engine stops retrying and dead-letters); any other non-zero = **transient**
  (retried with backoff up to the kind's `max_transient_attempts` = 20). A missing
  interpreter, or an empty `spec.script`, is **terminal**. `stderr` is folded into
  the failure message.
- **Cancellation kills the whole tree.** The script runs under the task's context
  in its **own process group**; a drain, lease loss, or the `timeout_seconds`
  deadline kills the *entire* group — not just the interpreter — so a spawned
  `sleep` or child tool dies with it and no run outlives its claim.

This demo uses the built-in default kind name `stdshell`. A real deployment gives each
use case its OWN named kind (e.g. `cleanup`, `healthcheck`) via the worker's
`(fixed kind stdshell; no env var)` env — each a separate kind with its own CRD, providerconfig
(interpreter/env/timeout), and concurrency cap — see
[cmd/stdworker](../../../cmd/stdworker/README.md#naming-your-own-stdio-kinds).

| Kind | Role | Status it records |
|---|---|---|
| [`stdshell`](../../../internal/providers/stdshell/stdshell.kind.json) | leaf (`work`, `specChange → status`) | `status.exit_code` + tail-bounded `status.stdout` / `status.stderr` |

## The spec

A `stdshell` resource is just a script plus how to run it (see
[internal/providers/stdshell/types.go](../../../internal/providers/stdshell/types.go)):

```json
{
  "kind": "stdshell",
  "name": "hello-stdshell",
  "spec": {
    "script": "#!/usr/bin/env bash\nset -euo pipefail\necho \"hello from $(hostname) at $(date -u +%Y-%m-%dT%H:%M:%SZ)\"\necho \"greeting is: ${GREETING:-<unset>}\"\n",
    "env": { "GREETING": "hi" },
    "timeout_seconds": 60
  }
}
```

- **`script`** (required) — the script text, run inline. Empty fails **terminally**.
- **`interpreter`** — path or PATH name; defaults to the config's
  `default_interpreter`, then `bash`.
- **`args`** — extra arguments passed to the interpreter *before* the script path
  (e.g. `["-x"]` for `bash -x`).
- **`env`** — extra `KEY→VALUE` pairs, layered over the config's baseline `env`.
- **`timeout_seconds`** — per-run timeout; `0` falls back to the config's
  `default_timeout_seconds` (then no provider timeout — the task deadline still
  applies).

Kind-wide defaults come from the providerconfig
([`testfixtures/providerconfig-stdshell.json`](testfixtures/providerconfig-stdshell.json)):

```json
{ "default_interpreter": "bash", "default_timeout_seconds": 300 }
```

## Prerequisites

The repo root's `just setup`, plus **`bash`** on PATH (the sample script's
interpreter). The scripts run on the worker's host, so whatever a script invokes
must be on the worker's PATH. No Docker — the workers run on the host.

## Run the demo

First time only, from the repo root: `just setup` then `just gen`. Then, from this
directory:

```bash
just demo                 # 3 control + 3 brokers + 3 stdworkers (host the stdshell provider)
just demo 5               # override the worker count (>= the broker count, 3)
```

`just demo`:

1. builds the converge binary + the shipped `bin/stdworker` (via the root build);
2. brings up the cluster on the host via `deploy/dev-local.sh` with
   `WORKER_BIN=.../bin/stdworker` (the stdworker hosts the stdshell provider — no
   `STDIO_COMMAND` needed, the script comes from the resource spec, not an external
   program);
3. applies the stdshell CRD (from the provider package), its providerconfig, and one
   sample resource (a small inline bash script).

`Ctrl+C` tears down the cluster and Postgres.

## Watch it converge

In the UI (http://localhost:8080), or with the [`conctl`](../../../cmd/conctl/README.md)
CLI — `just demo` builds it at `bin/conctl` (run `./bin/conctl …` from the repo root, or
`just install` to put `conctl` on your `PATH`):

```bash
conctl list resources                          # stdshell/hello-stdshell → Ready
conctl get resource stdshell/hello-stdshell -o json   # status.stdout carries the script's output
```

`status.stdout` holds the tail-bounded captured output (here the `hello from …`
lines); `status.exit_code` is `0` on success, and a failed run's `stderr` shows up
in the failure message.

## Register the kind / add a providerconfig / create a resource

The demo does this for you, but the exact flow — the CRD ships **with the provider**,
the providerconfig + resource come **from the demo** — is:

```bash
# 1. register the kind (CRD ships with the provider):
conctl apply --type manifest -f ../../../internal/providers/stdshell/stdshell.kind.json

# 2. add the providerconfig (defaults: interpreter + timeout — from the demo):
conctl apply --type providerconfig -f testfixtures/providerconfig-stdshell.json

# 3. create a resource (an inline script that queues until a worker claims it):
conctl apply --type resource -f testfixtures/resource-stdshell.json
```

Order matters: a resource needs its kind's manifest to validate, so the CRD goes
first.

## Files

| File | Role |
|---|---|
| [`internal/providers/stdshell/stdshell.go`](../../../internal/providers/stdshell/stdshell.go) | the provider (shipped, not demo): `OnConfig` + `Work` — decode spec, resolve config, write the script, run it in its own process group, map exit code + output → `Outcome` |
| [`internal/providers/stdshell/types.go`](../../../internal/providers/stdshell/types.go) | the kind name + the `Spec` (script/interpreter/args/env/timeout), `Status` (exit_code/stdout/stderr), and `Config` (defaults) shapes |
| [`internal/providers/stdshell/stdshell.kind.json`](../../../internal/providers/stdshell/stdshell.kind.json) | the **shipped** CRD (co-located with the provider): opaque `spec`/`status`/`config` schemas + a `specChange → status` `work` reaction, `max_transient_attempts` |
| [`cmd/stdworker`](../../../cmd/stdworker) | the shipped worker binary that hosts the stdshell provider (its README covers run flags + `(fixed kind stdshell; no env var)`) |
| [`testfixtures/providerconfig-stdshell.json`](testfixtures/providerconfig-stdshell.json) | the kind-wide defaults (`default_interpreter: bash`, `default_timeout_seconds: 300`) |
| [`testfixtures/resource-stdshell.json`](testfixtures/resource-stdshell.json) | a sample `stdshell` resource whose inline bash prints host + timestamp + a `GREETING` env var |
| [`justfile`](justfile) | `just demo` (cluster + stdworkers + apply CRD/config/resource) and `just build` (delegates to the root build) |

## How it fits together

```
stdshell resource (spec.script = "echo hello…")
        │  specChange → work
        ▼
bin/stdworker  (hosts the stdshell provider; WORKER_BIN in dev-local.sh)
        │  write spec.script → temp file (0700)
        │  run: <interpreter> [args…] <script>   ← own process group, ctx + timeout
        │  capture stdout/stderr (tail-bounded), read exit code
        ▼
status = { exit_code, stdout, stderr }  +  Ready condition
    exit 0 = ok  |  exit 2 = terminal (no retry)  |  other non-zero / timeout / cancel = transient
```

The CRD ships with the provider to signal *this is a shipped provider — the CRD
defines what it is* (contrast the demo-only [`classic`](../classic/) /
[`datadriven`](../datadriven/) providers, whose Go **and** `*.kind.json` live under
`examples/demos/` because they are fake and not shipped). Point a named kind's
providerconfig at a different interpreter (`python3`, `pwsh`, …) and the same
provider runs those scripts — the kind name (`(fixed kind stdshell; no env var)`) is the only thing that
changes.
