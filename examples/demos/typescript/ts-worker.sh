#!/usr/bin/env bash
# ts-worker.sh is the WORKER_BIN shim for the dev-local launcher: it execs the compiled
# TypeScript worker (dist/worker.js) with `node`, inheriting the launcher's environment
# (BROKER_ADDR / TLS_* / HEALTH_ADDR / WORKER_MAX_PARALLEL / LOG_*). The launcher treats a
# worker as an opaque executable it runs with that env; for a Go demo that's a compiled
# binary, for this one it's node over the built JS.
#
# `just build` (this demo's justfile) runs `npm install` + `npm run build` so dist/worker.js
# exists before the launcher execs this. exec so signals (SIGTERM on teardown) reach node,
# which the SDK's serve() drains on.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec node "$here/dist/worker.js"
