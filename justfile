# Converge task runner. Building + testing the core needs only Go (see the `go`
# directive in go.mod), `just`, node (embedded UI), and a running Docker
# (integration tests). The demos + fleet recipes additionally shell out to `jq`.
# `just setup` verifies all of these.
#
# Go-based dev tools (sqlc, golangci-lint, buf, oapi-codegen) are NOT installed
# globally and are NOT in go.mod: each is run via `go run <module>@<pinned-version>`,
# so a new developer needs nothing beyond Go — the pinned tool is downloaded and
# cached on first use. Bump a tool by editing its version variable below.
#
# We deliberately do NOT use Go 1.24+'s `tool` directive here: it would pull each
# tool's full dependency tree into go.mod/go.sum — hundreds of modules the app never
# builds. Keeping tools as `go run @version` off the module graph is what keeps
# go.mod to the app's real deps. (Invocation is short via the recipes below —
# `just sqlc`, `just lint`, `just proto`, `just gen` — not the raw go run path.)
#
# `just` (no args) lists every recipe.

set shell := ["bash", "-uc"]

# ── pinned tool versions (single source of truth) ──────────────────────────
sqlc_version := "v1.31.1"
golangci_version := "v2.11.4"
buf_version := "v1.71.0"
protoc_gen_go_version := "v1.36.11"
protoc_gen_connect_go_version := "v1.20.0"
oapi_codegen_version := "v2.4.1"
govulncheck_version := "v1.6.0"
goimports_version := "v0.48.0"
# go-jsonschema (schema→Go structs) for `just gen-types`; the TS side pins
# json-schema-to-typescript in the consuming package's devDependencies.
gojsonschema_version := "v0.19.0"

# ── config (override on the CLI: `just docker image=myrepo/x:v1`) ──────────
database_url := env_var_or_default("DATABASE_URL", "postgres://localhost:5432/converge?sslmode=disable")
image := "converge:latest"

# Version stamped into the binary (-X main.BuildVersion), surfaced in the cluster
# view's VERSION column and /version. `git describe` yields the current tag plus
# commits-since + short commit when a tag is reachable (e.g. v1.2.3-5-gabc1234),
# and falls back to the bare short commit when no tag exists (--always); --dirty
# flags an uncommitted working tree. Override with `just build version=…`.
version := `git describe --tags --always --dirty 2>/dev/null || echo dev`

# Default: list recipes.
default:
    @just --list

# ── onboarding ───────────────────────────────────────────────────────────────

# One-shot environment check for a NEW developer: verifies every prerequisite is
# present (and the Go version satisfies go.mod), confirms Docker is reachable for
# the integration tests, then warms the offline build cache. Prints exactly what to
# run next. Idempotent + read-only except the cache warm — safe to re-run anytime.
#
# Philosophy (see the header): the only things a dev installs are Go, just, node,
# a running Docker, and jq (the demos + fleet recipes shell out to jq for JSON).
# Everything else — the Go dev tools (sqlc, buf, golangci-lint, protoc-gen-*,
# oapi-codegen) — is fetched on demand by the recipes at the versions pinned above,
# and demo-only extras (curl/gzip/zip/base64) are baseline-present + guarded
# per-recipe. Nothing else to install.
[doc("Verify a new dev's environment (Go/just/node/Docker/jq) and warm the build cache")]
setup:
    #!/usr/bin/env bash
    set -uo pipefail
    ok=0; fail=0
    say()  { printf '  \033[32m✓\033[0m %s\n' "$1"; ok=$((ok+1)); }
    bad()  { printf '  \033[31m✗\033[0m %s\n' "$1"; fail=$((fail+1)); }
    echo "Checking the converge dev environment…"

    # Go — present AND new enough for go.mod's directive.
    need="$(grep -E '^go ' go.mod | awk '{print $2}')"
    if have="$(go env GOVERSION 2>/dev/null)"; then
      # strip leading 'go' (go1.26.4 -> 1.26.4) and compare with the go.mod minimum.
      hv="${have#go}"
      if [ "$(printf '%s\n%s\n' "$need" "$hv" | sort -V | head -1)" = "$need" ]; then
        say "Go $hv (>= $need required by go.mod)"
      else
        bad "Go $hv is older than go.mod's $need — upgrade: https://go.dev/dl/"
      fi
    else
      bad "Go not found — install $need+: https://go.dev/dl/ (or 'brew install go')"
    fi

    # just — you're running it, but say so for completeness.
    if command -v just >/dev/null 2>&1; then say "just $(just --version | awk '{print $2}')"; else bad "just not found — 'brew install just' or https://just.systems"; fi

    # node + npm — for the embedded UI (just ui / just build). The real floor is
    # Vite 8's (Node >= 20.19); package.json has no `engines` pin, so check that
    # minimum, not a specific major.
    node_min="20.19.0"
    if command -v node >/dev/null 2>&1; then
      nv="$(node --version)"; nv="${nv#v}"
      if [ "$(printf '%s\n%s\n' "$node_min" "$nv" | sort -V | head -1)" = "$node_min" ]; then
        say "node v$nv + npm $(npm --version 2>/dev/null || echo '?')  (UI build)"
      else
        bad "node v$nv is below Vite 8's minimum $node_min — upgrade (https://nodejs.org); needed only for 'just ui'/'just build'"
      fi
    else
      bad "node not found — install Node $node_min+ (https://nodejs.org or 'brew install node'); needed only for 'just ui'/'just build'"
    fi

    # Docker — the ONLY runtime dependency for the integration suite (testcontainers
    # starts Postgres on it). 'docker info' succeeding means the daemon is reachable.
    if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
      say "Docker reachable  (integration tests: 'just test', local fleet: 'just dev')"
    elif command -v docker >/dev/null 2>&1; then
      bad "Docker installed but the daemon isn't reachable — start Docker Desktop/colima; 'just test'/'just dev' need it"
    else
      bad "Docker not found — install Docker Desktop or colima; the integration suite ('just test'/'just dev') needs it (build + unit tests do not)"
    fi

    # jq — the demos + a few fleet recipes (just demo, submit-boms, apply-bundle) shell
    # out to it to build/read JSON on the host; the stdio demo's reference handler is
    # bash+jq. Not needed for build or the integration suite, but a hard prerequisite so
    # a demo run doesn't fail deep in a recipe. (curl/gzip/zip/base64/uuidgen are
    # baseline-present and guarded per-recipe where used.)
    if command -v jq >/dev/null 2>&1; then
      say "jq $(jq --version 2>/dev/null | sed 's/^jq-//')  (demos + fleet JSON recipes)"
    else
      bad "jq not found — 'brew install jq' or https://jqlang.github.io/jq/ ('just demo' + the fleet recipes need it)"
    fi

    echo
    if [ "$fail" -gt 0 ]; then
      printf '\033[31m%d prerequisite(s) missing.\033[0m Fix the ✗ above, then re-run: just setup\n' "$fail"
      exit 1
    fi

    echo "All prerequisites present — fetching modules + warming the build cache…"
    if go build ./... >/dev/null 2>&1; then say "build cache warmed (go build ./... ok)"; else bad "go build ./... failed — see: go build ./..."; fi

    echo
    echo "Ready. Next (in order):"
    echo "  just gen         # 1. regenerate all generated code (sqlc, proto, goldens, client)"
    echo "  just build       # 2. build the binaries + embedded UI  (or skip to a run below)"
    echo "  just dev         #    run a local multi-pod fleet, then open the UI"
    echo "  just demo        #    (in examples/demos/*) run a demo end to end"
    echo "  just test        #    the integration suite (testcontainers start Postgres)"
    echo "  just             #    list every recipe"

# ── build ──────────────────────────────────────────────────────────────────

# Build + bundle the embedded React UI (npm install + vite build).
# We clean dist/ deterministically HERE — everything except the committed
# dist/.gitkeep placeholder (which keeps `//go:embed dist/*` in ui/embed.go
# resolving on a fresh clone, before any UI build) — rather than letting vite's
# emptyOutDir wipe the whole dir. vite's wipe would also delete .gitkeep, so the
# tracked file showed up as deleted after every build; `emptyOutDir:false` in
# vite.config.ts hands that job to this recipe. Result: builds are still fresh
# (no stale hashed assets linger) and .gitkeep is never touched.
[doc("Build + bundle the embedded React UI (npm install + vite build)")]
ui:
    find ui/dist -mindepth 1 ! -name .gitkeep -delete
    cd ui && npm install && npm run build

# Build the server binary (control/broker; embeds the UI built above) AND the
# worker binaries into ./bin. converge carries NO provider code — all providers
# live in a worker binary, the only one that executes handler code.
#   bin/worker       — the CLASSIC demo's worker (the reference example provider set);
#                      each demo also builds its own worker via examples/demos/*/justfile.
#   bin/stdworker    — the SHIPPED default worker: hosts the built-in std* providers
#                      (stdio / stdshell / stdterraform; see internal/providers). Its
#                      full-tooling image is Dockerfile.stdworker (repo root).
# conctl is the kubectl-style CLI (apply/get/list/delete over the REST API).
[doc("build the converge server + worker + conctl binaries (embeds the UI)")]
build: ui
    go build -ldflags "-X main.BuildVersion={{version}}" -o bin/converge ./cmd/converge
    go build -ldflags "-X main.BuildVersion={{version}}" -o bin/worker ./examples/demos/classic/cmd/worker
    go build -ldflags "-X main.BuildVersion={{version}}" -o bin/stdworker ./cmd/stdworker
    go build -ldflags "-X main.BuildVersion={{version}}" -o bin/conctl ./cmd/conctl

# Install the binaries onto $PATH (GOBIN, else $(go env GOPATH)/bin) via `go
# install`, so `converge` / `stdworker` / `conctl` are runnable anywhere — the
# same three server-side binaries the container ships, plus conctl. Builds the UI
# first so the server embeds it. (The classic-demo `worker` is a demo, not a
# shipped binary, so it is NOT installed — build it with `just build` if needed.)
[doc("go install the converge, stdworker, and conctl binaries onto $PATH")]
install: ui
    go install -ldflags "-X main.BuildVersion={{version}}" ./cmd/converge
    go install -ldflags "-X main.BuildVersion={{version}}" ./cmd/stdworker
    go install -ldflags "-X main.BuildVersion={{version}}" ./cmd/conctl

# Build the CONTROL/BROKER container image (the root Dockerfile). Binaries are built
# FIRST on the host (UI + cross-compiled linux/amd64), then `docker build` packages
# them (no Go toolchain runs in Docker). This lean image carries TWO DB-less binaries —
# converge (control/broker) + conctl (operator CLI) — on a small alpine base. The
# DEFAULT worker is a SEPARATE image (`just docker-stdworker`, Dockerfile.stdworker),
# since it must bundle the std* providers' tooling.
#
# CGO_ENABLED=0 -> fully static binaries (pure Go; pgx needs no cgo). The UI
# (ui/dist) and migrations are embedded into the converge binary.
#
# Build the linux/amd64 control/broker image (host-built binaries; override tag: image=myrepo/x:v1).
[doc("build the linux/amd64 control/broker container image (override tag: image=myrepo/x:v1)")]
docker: ui
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
        go build -trimpath -ldflags='-s -w -X main.BuildVersion={{version}}' \
        -o bin/converge.linux-amd64 ./cmd/converge
    # conctl — the operator CLI, packaged into the image so you can apply CRDs/config
    # from a `kubectl exec` without a separate tool.
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
        go build -trimpath -ldflags='-s -w -X main.BuildVersion={{version}}' \
        -o bin/conctl.linux-amd64 ./cmd/conctl
    # --platform is REQUIRED: on an arm64 host (Apple Silicon) `docker build`
    # otherwise stamps the image manifest as arm64 even though the COPYd binary
    # is amd64, producing an arch-mismatched image amd64 EKS nodes reject. The
    # amd64 base is pulled directly — no QEMU emulation, no slowdown.
    docker build --platform linux/amd64 -t {{image}} .

# Build the DEFAULT WORKER image (Dockerfile.stdworker): the shipped stdworker plus ALL
# the std* providers' tooling (tofu + aws + gcloud + az + git + curl + bash + jq), so
# one image runs any std* kind (stdio / stdshell / stdterraform). Override the tag with
# `image=myrepo/stdworker:v1`. The stdterraform demo builds this same image.
[doc("build the linux/amd64 stdworker image (override tag: image=myrepo/stdworker:v1)")]
docker-stdworker stdworker_image="converge-stdworker:latest":
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
        go build -trimpath -ldflags='-s -w -X main.BuildVersion={{version}}' \
        -o bin/stdworker.linux-amd64 ./cmd/stdworker
    docker build --platform linux/amd64 -f Dockerfile.stdworker -t {{stdworker_image}} .

# Remove build outputs.
[doc("remove build outputs (bin/ + ui/dist/)")]
clean:
    rm -rf bin/ ui/dist/

# ── dev sessions (require Docker for the Postgres testcontainer) ────────────
#
# Two execution models — same Postgres work_queue, different worker path:
#   dev-inproc* : the whole cluster runs IN ONE `go test` process; workers are
#                 in-process dispatcher goroutines that claim work_queue rows
#                 directly (FOR UPDATE SKIP LOCKED) and run handlers inline —
#                 NO broker, NO gRPC. Fast to start; best for engine/DAG/replica
#                 iteration. Built on `just ui` (go test compiles the rest).
#   dev         : the REAL separate binaries (control + broker + dumb gRPC
#                 workers) as N OS processes, exactly like the k8s Deployments —
#                 workers dial a broker over gRPC and pull work. End-to-end.

# Single-pod IN-PROCESS dev session (Postgres testcontainer + embedded UI; no
# broker/gRPC). Optional positional manifest path auto-submits it, e.g.
# `just dev-inproc examples/demos/classic/testfixtures/resource-classicbom.json`.
[doc("Single-pod in-process dev session (testcontainer + UI, no broker); optional positional manifest")]
dev-inproc bom="": ui
    go test -count=1 -timeout 0 ./test/ -run '^TestDevRun$' -v -args -dev \
        {{ if bom != "" { "-bom " + justfile_directory() / bom } else { "" } }}

# Convenience: single-pod in-process dev session auto-submitting the sample classic BOM.
dev-inproc-bom: (dev-inproc "examples/demos/classic/testfixtures/resource-classicbom.json")

# Multi-pod IN-PROCESS dev session: primary + replica, 3 control + 10 workers/kind
# on :18080..:18082 (pod sharding, replica reads, cascade paths) — all simulated as
# goroutines in one process, still no broker/gRPC. Optional positional manifest auto-submits.
[doc("Multi-pod in-process dev session (replica + sharding, no broker) on :18080..:18082; optional positional manifest")]
dev-inproc-multipod bom="": ui
    go test -count=1 -timeout 0 ./test/ -run '^TestDevRunMultiPod$' -v -args -dev-multipod \
        {{ if bom != "" { "-dev-multipod-bom " + justfile_directory() / bom } else { "" } }}

# Convenience: multi-pod in-process dev session auto-submitting the sample classic BOM.
dev-inproc-multipod-bom: (dev-inproc-multipod "examples/demos/classic/testfixtures/resource-classicbom.json")

# Local dev fleet of REAL binaries over gRPC (3 control + 3 brokers + 50 worker
# processes vs primary+replica; mirrors the k8s split — workers dial a broker and
# pull work). Requires Docker; Ctrl+C tears it down. Optional positional manifest.
#   crds = an OS-path-list of dirs holding *.kind.json CRDs; the launcher applies
#   them via conctl once the API is up (client-side — never passed to the server).
#   Equivalent to setting KIND_FIXTURES_DIR in the environment.
[doc("Local dev fleet of real binaries over gRPC (3 control + 3 brokers + 50 workers); optional positional manifest + crds dir")]
dev bom="" crds="": build
    BOM={{ if bom != "" { justfile_directory() / bom } else { "" } }} \
    {{ if crds != "" { "KIND_FIXTURES_DIR=" + crds } else { "" } }} \
    bash ./deploy/dev-local.sh

# Submit N manifest roots in parallel against an already-running multi-pod
# fleet, round-robin across control-pod API ports. The manifest is sent as-is
# except `name` is overridden per submission (so the API doesn't reject dups),
# so this works for any kind. Defaults: count=5, ports for `just dev`
# (:8080..:8082); `just dev-inproc-multipod` runs the API on :18080..:18082.
#
# just args are POSITIONAL (in the order below), not name=value — pass them in
# order:
#   just submit-boms 20                          # 20 submissions, default ports + bom
#   just submit-boms 5 "18080 18081 18082"       # custom ports (for `just dev-inproc-multipod`)
#   just submit-boms 20 "18080 18081 18082" big.json   # also a custom manifest
[doc("Submit <count> manifests to a running fleet, round-robin over <ports> (positional args)")]
submit-boms count="5" ports="8080 8081 8082" bom="examples/demos/classic/testfixtures/resource-classicbom.json":
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -f "{{bom}}" ]; then echo "BOM not found: {{bom}}"; exit 1; fi
    command -v jq >/dev/null   || { echo "jq is required";   exit 1; }
    command -v curl >/dev/null || { echo "curl is required"; exit 1; }
    command -v gzip >/dev/null || { echo "gzip is required"; exit 1; }
    bom_path="$(cd "$(dirname "{{bom}}")" && pwd)/$(basename "{{bom}}")"
    set -- {{ports}}; nports=$#
    echo "submitting {{count}} manifest(s) from $bom_path round-robin across {{ports}}"
    for i in $(seq 1 {{count}}); do
      idx=$(( (i - 1) % nports + 1 )); port=$(eval echo \$$idx)
      (
        suffix=$(uuidgen 2>/dev/null | tr A-Z a-z | cut -c1-8)
        [ -z "$suffix" ] && suffix=$(printf '%08x' $RANDOM$RANDOM)
        name="bp-$suffix-$i"
        out=$(jq -c --arg n "$name" '.name = $n | (if .spec.deployment_instance then .spec.deployment_instance.name = $n else . end)' "$bom_path" \
          | gzip -c \
          | curl -sS --compressed -X POST "http://localhost:$port/api/v1/resources" \
              -H "Content-Type: application/json" -H "Content-Encoding: gzip" \
              --data-binary @-)
        id=$(printf '%s' "$out" | jq -r '.id // empty')
        if [ -z "$id" ]; then echo "FAILED $name -> :$port: $out"; else echo "submitted $name ($id) -> :$port"; fi
      ) &
    done
    wait

# (The `apply-bundle` recipe moved to examples/demos/datadriven/justfile alongside
# the Starlark/CEL demo providers it serves — it is demo-specific, not core.)

# ── test / lint ──────────────────────────────────────────────────────────

# Integration tests (require Docker for the Postgres testcontainer). Runs FOUR phases:
#   1. the base ./test suite (the fast integration tests; ~2-3 min)
#   2. the broker/worker CHAOS soak (-chaos), ALL fault scenarios, one seed, noop
#      workload (a real per-task delay so chaos actually fires — see the memory note)
#   3. the control-plane HA chaos test (-chaos-ha)
#   4. the streaming-replica smoke test (-stress-replica)
#
# Each phase is a SEPARATE `go test` process with its OWN testcontainer Postgres
# (random ports, no collision), so the phases are INDEPENDENT and run CONCURRENTLY by
# default — wall-clock is max(phase), not sum(phase). Each phase's output streams to a
# per-phase log (tee'd live + printed on failure); a final summary names each phase
# PASS/FAIL and the recipe exits non-zero if ANY failed, preserving the "a failure names
# its phase" property. Set SERIAL=1 to run them one at a time (a resource-constrained
# box: 4 concurrent fleets + 4 Postgres containers is heavier on RAM/CPU).
#
# DELIBERATELY EXCLUDED (run them explicitly when needed, not on every `just test`):
#   - the 1M/250k stress test (-stress) — 30-90 min; run via a manual -args -stress
#   - TestRealBOMFlaky (-flaky) — a demo-style long-latency reproduction, not a gate
#   - the dev launchers (-dev / -dev-multipod / -dev-db / -dev-db-single) — interactive,
#     run via `just dev*` / `just dev-db`
# -v streams per-test progress; the timeouts leave headroom for a cold image pull +
# the chaos/HA budgets. Chaos uses -chaos-scenario=all so one pass covers every fault
# class (kill/drain/join/leave, network, db, rollout); bump to =worker|rollout|… or add
# a second -chaos-seed for a deeper local soak.
[doc("run the integration suite + chaos + HA + replica CONCURRENTLY (SERIAL=1 for one-at-a-time)")]
test:
    #!/usr/bin/env bash
    # NB: macOS ships bash 3.2 (no associative arrays), so this uses plain functions +
    # an indexed "name:pid" tracking string — no bash-4 features.
    set -uo pipefail
    tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT

    # Each phase is its OWN `go test` process with its OWN testcontainer Postgres
    # (random ports), so they're independent and parallel-safe. run_one streams the
    # phase's output to a per-phase log (printed on failure) and prints START/PASS/FAIL.
    run_one() { # <name> <logfile> <argv...>
      local name="$1" log="$2"; shift 2
      echo "── START ${name} ──"
      "$@" >"$log" 2>&1
      local rc=$?  # capture BEFORE any other command (local-on-same-line-as-$? resets it)
      if [ "$rc" = 0 ]; then echo "── PASS  ${name} ──"; else echo "── FAIL  ${name} (exit ${rc}) ──"; fi
      return "$rc"
    }
    p_base() { run_one base-suite     "$tmp/base.log"  go test -count=1 -timeout 600s -v ./test/; }
    p_chaos(){ run_one broker-chaos   "$tmp/chaos.log" go test -count=1 -timeout 15m -v ./test/ -run '^TestBrokerChaos$'     -args -chaos -chaos-scenario=all -chaos-workload=noop -chaos-budget=8m; }
    p_ha()   { run_one control-ha     "$tmp/ha.log"    go test -count=1 -timeout 15m -v ./test/ -run '^TestControlPlaneHA$'   -args -chaos-ha; }
    p_repl() { run_one stream-replica "$tmp/repl.log"  go test -count=1 -timeout 10m -v ./test/ -run '^TestStreamingReplica$' -args -stress-replica; }
    phases="base:$tmp/base.log chaos:$tmp/chaos.log ha:$tmp/ha.log repl:$tmp/repl.log"

    fail=0
    if [ "${SERIAL:-0}" = "1" ]; then
      echo "== running the 4 phases SERIALLY (SERIAL=1) =="
      for e in $phases; do
        name="${e%%:*}"; log="${e##*:}"
        "p_${name}" || { fail=1; printf "  ↳ %s last 40 lines:\n" "$name"; tail -40 "$log" | sed 's/^/    /'; }
      done
      [ "$fail" = 0 ] && { echo "ALL PHASES PASSED"; exit 0; } || { echo "ONE OR MORE PHASES FAILED"; exit 1; }
    fi

    echo "== running the 4 phases CONCURRENTLY (SERIAL=1 to serialize) =="
    tracked=""  # "name:log:pid" per launched phase
    for e in $phases; do
      name="${e%%:*}"; log="${e##*:}"
      "p_${name}" & tracked="$tracked ${name}:${log}:$!"
    done

    echo ""
    echo "════════════════════ integration summary ════════════════════"
    for e in $tracked; do
      name="${e%%:*}"; rest="${e#*:}"; log="${rest%:*}"; pid="${rest##*:}"
      if wait "$pid"; then printf "  PASS  %s\n" "$name"
      else fail=1; printf "  FAIL  %s — last 40 lines:\n" "$name"; tail -40 "$log" | sed 's/^/    /'; fi
    done
    echo "══════════════════════════════════════════════════════════════"
    [ "$fail" = 0 ] && { echo "ALL PHASES PASSED"; exit 0; } || { echo "ONE OR MORE PHASES FAILED"; exit 1; }

# Format HAND-WRITTEN Go code via the pinned goimports (gofmt + import
# grouping/pruning), fetched + cached on first run — same `go run @version`
# discipline as the other dev tools, not installed. Writes in place; run before
# committing. GENERATED code (internal/dbq, sdk/brokerpb, internal/conctl/apiclient)
# is EXCLUDED: it's emitted by `just gen` in its own layout, and reformatting it
# here would create spurious diffs and fight the generator's output.
[doc("format hand-written Go code (gofmt + imports) via the pinned goimports")]
fmt:
    #!/usr/bin/env bash
    set -euo pipefail
    files=$(find . -name '*.go' \
        -not -path './internal/dbq/*' \
        -not -path './sdk-go/workerpb/*' \
        -not -path './internal/meshpb/*' \
        -not -path './internal/conctl/apiclient/*' \
        -not -name '*.pb.go' -not -name '*.gen.go' -not -name '*.connect.go')
    go run golang.org/x/tools/cmd/goimports@{{goimports_version}} -w $files

# Lint via the pinned golangci-lint (fetched + cached on first run).
[doc("lint via the pinned golangci-lint (fetched + cached on first run)")]
lint:
    go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@{{golangci_version}} run ./...

# Scan for known vulnerabilities via the pinned govulncheck (fetched + cached on
# first run — same `go run @version` discipline as sqlc/buf/lint, not installed).
# Source-based analysis: only vulns REACHABLE by the code's call graph are
# reported, so a flagged finding is actually exercised. Exits non-zero on a
# finding, so this doubles as a CI gate.
[doc("scan for known vulnerabilities via the pinned govulncheck")]
vuln:
    go run golang.org/x/vuln/cmd/govulncheck@{{govulncheck_version}} ./...

# ── codegen / schema ───────────────────────────────────────────────────────

# Regenerate EVERY generated artifact end-to-end, in dependency order, so a
# fresh checkout has correct generated sources before the first build. This is
# the ONE codegen step a new dev runs — after `just setup` and before
# `just build`/`dev`/`demo`. The pieces:
#
#   sqlc         → internal/dbq/*        (Go compiled by `go build` — MUST be fresh)
#   proto        → sdk/brokerpb/*        (Go compiled by `go build` — MUST be fresh;
#                                         its embedded descriptor breaks at RUNTIME if stale)
#   golden       → openapi + schema goldens (test-verified; golden-schema needs Docker)
#   codegen-cli  → conctl API client     (from the openapi golden)
#
# Ordered so the two that `go build` COMPILES (sqlc, proto) run first and need no
# Docker; the golden/client steps follow (golden-schema spins a Postgres
# testcontainer, so this whole target expects Docker — same as the rest of the
# dev flow). The pinned tools (sqlc/buf/protoc-gen-*/oapi-codegen) are fetched on
# first run and cached, so re-runs are fast. Idempotent — safe to re-run anytime.
[doc("Regenerate ALL generated code (sqlc, proto, goldens, conctl client) — run after 'just setup'")]
gen: sqlc proto golden codegen-cli

# Generate provider TYPES from a kind's CRD manifest — schema-first: the *.kind.json
# JSON Schema is the single source of truth, and this derives the Go structs and/or TS
# interfaces (<Kind>Spec/Status/Config) FROM it so an author never hand-maintains a type
# in parallel with the schema. Deliberately NOT in `just gen` (which is CORE codegen) —
# this is an author/demo convenience whose inputs live in provider/demo dirs.
#
# The Go side uses the pinned go-jsonschema (fetched by cmd/gen-types via `go run`). The
# TS side needs json-schema-to-typescript's `json2ts` on PATH — pin it in the consuming
# package's devDependencies and run this with node_modules/.bin on PATH (see the TS demo's
# `npm run gen`, which calls cmd/gen-types directly for exactly that reason).
#
# ARGS ARE POSITIONAL (just, not name=value): manifests-glob, then optional go-out,
# go-package, ts-out. Set go-out and/or ts-out (an empty out skips that language).
#   just gen-types 'internal/providers/*/*.kind.json' gen gen          # Go structs into ./gen
#   just gen-types 'examples/demos/x/testfixtures/*.kind.json' '' gen ./x/src/gen  # TS only
[doc("generate Go/TS provider types from *.kind.json (schema-first; POSITIONAL args)")]
gen-types manifests go-out="" go-package="gen" ts-out="":
    go run ./cmd/gen-types \
      -manifests "{{manifests}}" \
      -go-out "{{go-out}}" -go-package "{{go-package}}" \
      -ts-out "{{ts-out}}" \
      -gojsonschema-version "{{gojsonschema_version}}"

# Regenerate sqlc Go from db/queries (pinned sqlc, fetched + cached on first run).
[doc("regenerate sqlc Go from db/queries")]
sqlc:
    go run github.com/sqlc-dev/sqlc/cmd/sqlc@{{sqlc_version}} generate

# Regenerate ALL proto stubs from proto/ (pinned buf + protoc-gen-go +
# protoc-gen-connect-go, fetched + cached on first run; no global install). The two Go
# plugins are built into a temp dir put on PATH so buf's `local:` lookup finds them;
# nothing is installed into GOBIN. Regenerates THREE stubs from the SAME source so the
# worker contract never drifts across bindings:
#   worker.proto -> sdk-go/workerpb (Go SDK)   via sdk-go/buf.gen.yaml
#   worker.proto -> sdk-ts/gen      (TS SDK)    via sdk-ts/buf.gen.yaml (needs node)
#   mesh.proto   -> internal/meshpb (core mesh) via the root buf.gen.yaml
# Each tree owns its stubs (scoped runs) and neither can pull the other's. The TS step
# runs the SDK's own `npm run gen` (buf + protoc-gen-es from sdk-ts/node_modules); it is
# SKIPPED with a warning if node isn't on PATH so a Go-only box still regenerates the Go
# stubs — but a proto edit is NOT fully applied until the TS stub is regenerated too (CI
# has node, so the drift is caught there).
[doc("regenerate ALL proto stubs (Go worker + TS worker + Go mesh) from proto/")]
proto:
    #!/usr/bin/env bash
    set -euo pipefail
    bin="$(mktemp -d)"
    trap 'rm -rf "$bin"' EXIT
    GOBIN="$bin" go install google.golang.org/protobuf/cmd/protoc-gen-go@{{protoc_gen_go_version}}
    GOBIN="$bin" go install connectrpc.com/connect/cmd/protoc-gen-connect-go@{{protoc_gen_connect_go_version}}
    # The PUBLIC worker contract is owned by the SDK: worker.proto -> sdk-go/workerpb via the
    # SDK's own buf.gen.yaml. The INTERNAL mesh contract -> internal/meshpb via the root
    # buf.gen.yaml. Two scoped Go runs so each tree owns its stubs.
    PATH="$bin:$PATH" go run github.com/bufbuild/buf/cmd/buf@{{buf_version}} generate --template sdk-go/buf.gen.yaml --path proto/converge/worker/v1/worker.proto
    PATH="$bin:$PATH" go run github.com/bufbuild/buf/cmd/buf@{{buf_version}} generate --template buf.gen.yaml --path proto/converge/mesh/v1/mesh.proto
    # The TS SDK owns its OWN worker stub (sdk-ts/gen) and MUST regenerate from the same
    # worker.proto, else the Go and TS bindings of the public contract drift on a field
    # renumber/rename. Run the SDK's `npm run gen` (its pinned buf + protoc-gen-es); install
    # deps first if missing. Skip with a warning on a node-less box (Go stubs still done).
    if command -v npm >/dev/null 2>&1; then
      [ -d sdk-ts/node_modules ] || (cd sdk-ts && npm ci)
      (cd sdk-ts && npm run gen)
    else
      echo "WARNING: node/npm not found — SKIPPED the TS worker stub (sdk-ts/gen). A proto change is NOT fully applied until it is regenerated; run 'cd sdk-ts && npm run gen' on a box with node."
    fi

# Regenerate both golden files (after a SQL migration or Huma route/type change; review the diff).
[doc("regenerate both golden files (after a SQL migration or API change)")]
golden: golden-schema golden-openapi

[doc("regenerate db/schema.golden.sql from the migration (pg_dump)")]
golden-schema:
    go test ./db/ -run TestMigrationSchema -update

[doc("regenerate api/openapi.golden.yaml from the Huma routes")]
golden-openapi:
    go test ./internal/api/ -run TestOpenAPISpec -update-openapi

# Regenerate the conctl API client from the OpenAPI golden spec. Runs after
# golden-openapi so the client always tracks the CURRENT API surface. Two steps:
# spectool down-converts the 3.1 spec to a 3.0.3 copy oapi-codegen v2 can parse
# (its 3.1 support is incomplete — see internal/conctl/apiclient/spectool), then
# oapi-codegen emits the typed models + client into client.gen.go. Run this after
# any API request/response type or route change. Pinned oapi-codegen, fetched +
# cached on first run (same discipline as sqlc/buf).
[doc("regenerate the conctl OpenAPI client from the golden spec")]
codegen-cli: golden-openapi
    #!/usr/bin/env bash
    set -euo pipefail
    cd internal/conctl/apiclient
    go run ./spectool ../../api/openapi.golden.yaml openapi.3.0.yaml
    go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@{{oapi_codegen_version}} -config codegen.yaml openapi.3.0.yaml

# ── migrations (operator tools against DATABASE_URL) ───────────────────────
# There is NO `migrate up` recipe: migrations run AUTOMATICALLY on every converge
# pod's startup (control + broker), serialized by a Postgres session advisory lock
# — so bringing up the fleet against an empty DB needs no separate step. These are
# the out-of-band operator/dev utilities for inspecting or rolling back the schema.

[doc("roll back the last migration against DATABASE_URL")]
migrate-down:
    DATABASE_URL={{database_url}} go run ./cmd/converge migrate down

[doc("show migration status against DATABASE_URL")]
migrate-status:
    DATABASE_URL={{database_url}} go run ./cmd/converge migrate status

[doc("reset the schema (down-all then up) against DATABASE_URL")]
migrate-reset:
    DATABASE_URL={{database_url}} go run ./cmd/converge migrate reset

# Spin up a single throwaway Postgres (testcontainers) with the converge schema
# applied, print its DSN, and block until Ctrl+C — a zero-setup local DB to point
# conctl / a hand-run `bin/converge` / psql at while iterating. Needs Docker
# running. The container (and its docker network) are torn down on Ctrl+C.
#
#   just dev-db                     # prints DATABASE_URL=..., then blocks
#   DATABASE_URL=$(just dev-db ...) # NOT this — it blocks; copy the printed DSN
#
# The DSN's host port is random per run; copy the printed `DATABASE_URL=` line into
# another shell:  export DATABASE_URL=<paste>;  ROLE=all go run ./cmd/converge
[doc("spin up one throwaway migrated Postgres + print its DSN; block until Ctrl+C (needs Docker)")]
dev-db:
    go test -count=1 -timeout 0 -v ./test/ -run '^TestDevDBSingle$' -args -dev-db-single

# ── dependencies ───────────────────────────────────────────────────────────
# Dependencies build from the module cache (fetched from the Go proxy on first
# build and cached; go.sum pins every checksum for reproducibility). This repo is
# NOT vendored — there is no vendor/ tree to refresh.

# Tidy go.mod/go.sum after adding/removing an import (commit go.mod + go.sum).
[doc("tidy go.mod/go.sum after an import change")]
tidy:
    go mod tidy

# CI gate: fail if go.mod/go.sum are out of sync with the source imports.
[doc("CI gate: fail if go.mod/go.sum are out of sync with the code")]
deps-check:
    #!/usr/bin/env bash
    set -euo pipefail
    go mod tidy
    if ! git diff --quiet go.mod go.sum; then
      echo "go.mod/go.sum out of sync with the code — run 'just tidy' and commit"
      git diff --stat go.mod go.sum
      exit 1
    fi
    go mod verify
