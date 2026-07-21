# Contributing to Converge

Thanks for your interest in Converge! This document covers how to build, test,
and submit changes.

## Development setup

The only local prerequisites are **Go** (see `go.mod` for the required version),
[`just`](https://github.com/casey/just), **Node** (for the embedded UI),
**Docker** (for the integration tests, via testcontainers), and **`jq`** (for the
demos). Dev tools (sqlc, buf, golangci-lint, oapi-codegen) are run via pinned
`go run <tool>@<version>` — you do not install them.

## Build, run, test

A fresh checkout needs exactly these steps, in order:

```sh
# 0. install the `just` command runner — https://github.com/casey/just  (e.g. `brew install just`)
just setup   # 1. verify the toolchain (Go, just, Node, Docker) + warm the build cache
just gen     # 2. regenerate ALL generated code (sqlc, proto stubs, goldens, conctl client)
just build   # 3. build converge + the worker binaries + conctl (embeds the UI)
```

All generated code is committed, so `just gen` is a no-op on a clean clone — but run it after
changing a source of truth (a `db/queries` SQL file, a `.proto`, the schema, or the API
surface) and commit the regenerated output alongside your change (CI fails on drift). Then, as
you work:

```sh
just         # (no args) list every available recipe
just fmt     # format hand-written Go (gofmt + imports) via pinned goimports
just test    # integration suite + chaos + HA + replica (starts a Postgres testcontainer)
just lint    # pinned golangci-lint
just vuln    # scan for known vulnerabilities (pinned govulncheck; non-zero on a finding)
just dev     # bring up a local fleet against a throwaway Postgres
```

Run the test suite in the background while you work — it starts a Postgres
container and takes a few minutes.

### Run the demos (they double as integration demos)

The runnable demos under [`examples/demos/`](examples/demos/) each exercise the
whole engine end to end — apply a spec, watch it compose, reconcile, flow values,
and roll up status. Run them to sanity-check a change against the real system.
Each demo has its own `just demo`:

```sh
cd examples/demos/classic     && just demo   # a hand-written Go composer
cd examples/demos/datadriven  && just demo   # the same DAG as data (CEL / kro-style CEL / Starlark)
cd examples/demos/stdterraform && just demo  # a real `tofu apply` kind (needs Docker)
cd examples/demos/stdio       && just demo   # any external program as a kind (stdio protocol)
cd examples/demos/stdshell    && just demo   # an inline shell script from the spec
```

See each demo's README for what it demonstrates.

## Coding standards & best practices

The engineering conventions this project holds every change to — SOLID design
(dependency injection, narrow composable interfaces, register-don't-switch),
code organization (one concern per file), naming (no magic literals — use named
consts / typed enums), SQL (parameterized, in the query layer only), security
(injection defense, mTLS/SPIFFE, fail-closed), comments (exhaustive, current, no
history narration), and the no-doc-drift rule — all live in
[`CLAUDE.md`](CLAUDE.md). **Read it before your first change and follow it.** It is
the single source of truth for how code here is written; this file only covers the
mechanics of building and submitting.

## Generated code

Several artifacts are generated; regenerate them after the corresponding change
and commit the result:

```sh
just gen     # regenerate ALL generated code (sqlc, proto stubs, goldens, conctl client)
just sqlc    # only the sqlc queries  (internal/dbq)  — after editing db/queries
just proto   # only the proto stubs   (sdk-go/workerpb + internal/meshpb)  — after editing a .proto
just golden  # only the golden files  (schema + OpenAPI)
```

## Branching

We use **trunk-based development**: `main` is the trunk and is always releasable.
Every change lands via a short-lived topic branch and a pull request — including a
maintainer's own. Branch off `main`, name the branch `type/short-description` (the
same `type` vocabulary as commits below — e.g. `feat/reactor-cap-ui`,
`fix/reactor-heartbeat-deadlock`, `docs/versioning`), and keep it short-lived (rebase
on `main` rather than letting it diverge). Outside contributors branch on their fork
and open the PR against `main`.

## Commit messages & PR titles (Conventional Commits)

Commit messages and PR titles follow **[Conventional Commits](https://www.conventionalcommits.org)**
— a documented spec that makes each change's intent (and its SemVer impact) machine-
and human-readable:

```
type(scope): summary

feat(reactor): add per-binding version pin
fix(dbq): epoch-pin the reactor heartbeat to break a claim/reap deadlock
docs: document the release model
refactor(api): split schemaRegistry out of Server
```

- **`type`** is one of `feat` (new feature → MINOR bump), `fix` (bug fix → PATCH bump),
  `docs`, `refactor`, `test`, `perf`, `build`, `chore`.
- **`scope`** (optional) names the touched area: `reactor`, `broker`, `sdk`, `dbq`,
  `api`, `mesh`, …
- A **breaking change** is marked with a `!` after the type/scope (`feat(api)!: …`) or a
  `BREAKING CHANGE:` footer → MAJOR bump.

We **squash-merge**, so the PR title becomes the single commit on `main` and feeds the
auto-generated release notes — the prefix is what maps each change to its version bump.

## Submitting a change

1. Fork and create a topic branch off `main` (see **Branching**).
2. Make your change, including tests, following the conventions in
   [`CLAUDE.md`](CLAUDE.md). Update every comment/doc the change touches in the
   same commit (no doc drift). Run `just fmt`, `just gen`, `just lint`, `just vuln`,
   and `just test`.
3. Keep commits focused; write commit messages in the Conventional Commits form above.
4. Open a pull request whose **title is a Conventional Commit** describing **what**
   changed, and whose body says **why** and how you validated it (include performance
   numbers if you touched a hot path).

## Versioning & releases

Releases follow **[Semantic Versioning](https://semver.org)** — `vMAJOR.MINOR.PATCH`,
tagged with the leading `v` (Go tooling requires it). The Conventional-Commit types
above drive the bump: `feat` → MINOR, `fix` → PATCH, a breaking change → MAJOR.

- **Pre-stable (`v0.x`, where we are now):** there are **no compatibility guarantees**
  on either wire — the REST API or the protos (see the pre-stable rule in
  [`CLAUDE.md`](CLAUDE.md)). Prefer the correct shape over compatibility; a feature or a
  breaking change bumps MINOR (`v0.1.0 → v0.2.0`), a fix bumps PATCH.
- **`v1.0.0`** is the promise of stability: from then on the REST/proto compatibility
  guarantees apply and a breaking change bumps MAJOR.
- **`v2.0.0`+ Go-module rule:** a major ≥ 2 requires the version suffix in the module
  path (`module github.com/salesforce/converge/v2`), which changes the SDK import path
  (`sdk-go/converge`). Plan this as part of the first breaking release after `v1.0.0`.

Cutting a release (maintainers): tag an **annotated** tag on a green `main`, push it,
and create a GitHub Release from it:

```sh
git checkout main && git pull
git tag -a v0.2.0 -m "converge v0.2.0"
git push origin v0.2.0
```

By contributing, you agree that your contributions are licensed under the
project's [Apache 2.0 license](LICENSE).
