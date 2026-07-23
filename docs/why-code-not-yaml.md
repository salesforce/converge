# Why workers are code, not YAML

Most control planes ask you to express your logic in a templating dialect — Helm's
Go-templated YAML, Kustomize overlays, a CRD's embedded CEL, a Crossplane `Composition`'s
patch-and-transform blocks. Converge deliberately does not. A **worker** — the thing that
reconciles a leaf, or a **composer** — the thing that fans a resource into a child graph —
is **ordinary code** in a real language (Go or TypeScript), or *any* external program.

This page explains why that's a feature, not an omission — and why, in the age of AI coding
agents, "just write the code" is now the *easiest* path, not the hardest.

## YAML is a data format, not a programming language

YAML (and its templating cousins) is wonderful for **declaring desired state** — a spec, a
manifest, a config. That's exactly what Converge uses it for: a kind's manifest, a resource's
spec, a provider config are all declarative data. Where it falls down is **business logic**:
the moment your composition needs a loop, a conditional, a lookup, arithmetic, string
manipulation, error handling, or a call to an external system, a data format has to grow a
runtime — and every such runtime reinvents a worse programming language:

- **Control flow becomes string soup.** `{{- range $i, $team := .Values.teams }}` nested
  inside `{{- if eq $team.tier "pci" }}` inside whitespace-significant indentation is a
  program written in the least ergonomic language imaginable — no types, no debugger, no
  stack trace, failures that surface as "rendered YAML is invalid" three layers down.
- **The abstraction leaks immediately.** Real compositions need to talk to the world —
  resolve an account id, look up a subnet, branch on a cloud API's response. A templating
  DSL either can't (so you bolt on yet another controller) or grows escape hatches (embedded
  CEL, `functions`, provider plugins) until you're writing code anyway, just in a cramped,
  half-formed dialect.
- **You can't test it like code.** There is no unit test for a Helm template branch, no
  `go test`, no breakpoint, no coverage. A composer written in Go is a pure function you
  call with a `ReactionRequest` and assert on the `Outcome` — see [Testing a
  kind](implementing-a-provider.md#testing-a-kind).
- **It doesn't compose or reuse.** Functions, packages, and imports are how real logic is
  factored and shared. A pile of templated manifests copy-pastes instead.

Converge's model is a clean split: **declare state as data, express logic as code.** The
manifest (schema, reactions, policy) is declarative YAML/JSON an operator applies and edits
live. The *handler* — the part with actual decisions in it — is code, with the full power of
the language: types, tests, libraries, a debugger, real error handling, and the ability to
call anything.

## …but the logic is still edited live, like data

The usual objection to "logic is code" is that code means a rebuild and a redeploy. Converge
keeps the live-editability without giving up the expressiveness:

- **Composers-as-data.** A composer can be a **CEL** ruleset or a **Starlark** program
  shipped in a provider-config bundle and **edited on a running cluster** — no redeploy. You
  get a real expression/scripting language (loops, conditionals, functions) that's still
  hot-swappable data. See the [data-driven demo](../examples/demos/datadriven/README.md).
- **Any external program.** The `stdio` provider makes *any* binary or script a kind: task
  JSON in, outcome JSON out. Your "worker" can be a 20-line Python file. See the [stdio
  demo](../examples/demos/stdio/README.md).
- **The manifest itself is live data.** Schemas, concurrency caps, resync intervals, grace
  windows, reactions — all editable over the API with no rebuild.

So you choose per kind: compiled Go/TS for the full toolchain, or CEL/Starlark/stdio for
edit-live-as-data — all behind the *same* `Provider` contract. The point is that **when you
need real logic, you get a real language**, not a templating DSL pretending to be one.

## Writing a worker is easy — especially with an AI agent

"Write code" used to be the higher-effort path. It no longer is. A worker is a small,
self-contained thing: implement one interface (`Kind` / `Work` / `OnConfig` / `Ready`),
return an `Outcome`, call `Serve`. There's no controller framework, no reconcile loop, no
Kubernetes client, no informers — the SDK owns all of that.

That shape makes it an ideal task for an AI coding agent. The repo ships **seven worked
demos** covering every pattern — a leaf worker, a composer with children + value flows +
rollup, a reactor, a multi-version kind, composition-as-data in CEL and Starlark, an
external-program kind, and a full TypeScript worker. Point your agent at them and describe
what you want:

> "Look at `examples/demos/classic` in this repo — the `account` leaf worker, the
> `classicbom` composer, and its `testfixtures/kind-*.json` manifests. Write me a new kind
> `database` with a spec of `{ engine, size_gb }` and a status of `{ endpoint }`, plus a
> composer `appstack` that fans an app into a `database` child and a `bucket` child and flows
> the database `endpoint` into the app's spec. Follow the same structure, generate the
> manifest, and add a unit test for `Work`."

An agent with the demos in front of it can produce the manifest, the handler, the type
generation, the worker `main`, and the tests in one pass — because every one of those pieces
exists in the demos as a copyable pattern. This is far more tractable than asking an agent to
author a correct multi-hundred-line templated `Composition` with patches and transforms,
where a whitespace error silently renders the wrong resource.

**The workflow we recommend:**

1. Skim [Implementing a provider](implementing-a-provider.md) for the mental model (a kind is
   a manifest + handler code).
2. Pick the closest [demo](../examples/demos/) to what you're building.
3. Ask your agent to read that demo and implement your kind by analogy — manifest, handler,
   generated types, worker, and a `Work` unit test.
4. Run it against a local cluster (`just dev`) and iterate.

## In short

- **State is data; logic is code.** Converge uses YAML/JSON for what it's good at
  (declaring specs, schemas, config) and a real language for what it's good at (decisions,
  loops, I/O, error handling) — instead of forcing logic into a templating dialect that
  becomes an unmaintainable, untestable program.
- **You don't sacrifice live-editability.** Composers can ship as CEL/Starlark/stdio and be
  edited on a running cluster; the manifest is always live data.
- **Code is now the easy path.** A worker is a tiny, well-shaped interface, and the seven
  demos are ready-made patterns an AI agent can extend for a new kind or composer in minutes.

See also: [How Converge compares](how-converge-compares.md) (the composition-as-code
differentiator vs. Crossplane/kro) and [Implementing a provider](implementing-a-provider.md)
(the end-to-end guide).
