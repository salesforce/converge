# converge-worker-sdk — the TypeScript worker SDK

`converge-worker-sdk` is the client SDK for building a **worker** — the process that hosts
your reconcile logic, pulls work from a Converge broker, and reports the result. You declare
what your worker can do as a list of `Provider`s and call `serve`. The SDK reads config from
the environment and owns the whole broker relationship — dialing, reconnect-with-backoff,
config prime/refresh, the readiness poller, and graceful drain — so you never touch a
transport, an HTTP client, or a reconnect loop.

You do **not** need a database and you do **not** write any control-plane code. The broker
owns the queue, the lease, and all DB I/O, and ships each task's effective config in the
task itself.

```ts
import { serve, terminal, type Outcome, type Provider, type ReactionRequest } from "converge-worker-sdk";

const enc = new TextEncoder();
const dec = new TextDecoder();

class DatabaseProvider implements Provider {
  kind() {
    return { kind: "database", version: 1 };
  }
  async work(req: ReactionRequest): Promise<Outcome> {
    let spec: { size?: string };
    try {
      spec = JSON.parse(dec.decode(req.resource.spec) || "{}");
    } catch (e) {
      throw terminal(`decode spec: ${e}`); // non-retryable
    }
    // …reconcile spec against the real world…
    return { status: enc.encode(JSON.stringify({ ready: true, size: spec.size })) };
  }
  onConfig(): void {} // no default providerconfig
  ready(): boolean {
    return true; // no downstream to dial
  }
}

// The caller owns the signal + exit code. serve reads BROKER_ADDR / TLS_* / WORKER_* from
// the environment, then dials, serves, reconnects, and drains until the signal aborts.
const controller = new AbortController();
process.on("SIGINT", () => controller.abort());
process.on("SIGTERM", () => controller.abort());

await serve([new DatabaseProvider()], controller.signal);
```

## The one contract: `Provider`

Everything the SDK needs from your business logic is one interface, **one instance per
`(kind, version)`**:

```ts
interface Provider {
  kind(): KindVersion;                     // the ONE (kind, version) this provider serves
  work(req: ReactionRequest): Promise<Outcome>; // run one task for that pair
  onConfig(cfg: ProviderConfig): void;     // react to a providerconfig push {spec, data}
  ready(): boolean;                        // "can I do work right now?"
}
```

- **`kind`** — the single `(kind, version)` pair. Versions are explicit and `>= 1` (no
  implicit v1). To serve two versions of one kind, list two providers, each with its own
  `work`.
- **`work`** — pure decision logic. It reads `req` (spec, generation, observed children, the
  per-resource config override on `req.env`) and returns an `Outcome` the core applies. Throw
  `terminal(msg)` to stop retries; a plain throw retries with backoff. A kind that declares
  several reactions switches on `req.reaction`.
- **`onConfig`** — receives the kind's **default** `{spec, data}` providerconfig: once at
  startup with the primed default, and again whenever an operator edits it (an empty `cfg`
  means the default was deleted). It's the hook to redial a client or recompile a bundle.
- **`ready`** — the SDK polls this. It is your **entire bring-up contract**: the SDK never
  calls a setup step, so you dial your own downstreams on your own schedule and simply report
  whether you can currently do work. A `true→false` edge tells the broker to stop sending
  this pair's work (an `Interest` RS-); `false→true` resumes it (RS+) — so one provider's
  degraded downstream sheds only its own kind, not the whole worker.

## Two tiers: `serve` and `runWorker`

- **`serve(providers, signal)`** — the high-level, env-configured entrypoint (the 99% path).
  It reads all config from the environment, dials the broker, and blocks until the signal
  aborts.
- **`runWorker(client, providers, signal, opts?)`** — the low-level tier for a caller who
  already holds a broker client (a test with an in-process broker, an embedder with a bespoke
  transport/auth). Both funnel through one run loop.

## Configuration

`serve` reads all config from the environment. A malformed value (a bad duration, a
non-numeric count) is a loud error, not a silent fallback.

| Concern             | Env var                                            |
| ------------------- | -------------------------------------------------- |
| Broker address      | `BROKER_ADDR`                                       |
| mTLS keypair + CA   | `TLS_CERT_FILE` / `TLS_KEY_FILE` / `BROKER_CA_FILE` |
| Cert reload poll    | `TLS_RELOAD_INTERVAL`                               |
| Per-pod parallelism | `WORKER_MAX_PARALLEL`                               |
| Drain grace         | `WORKER_DRAIN_TIMEOUT`                              |
| Kinds filter        | `WORKER_KINDS`                                       |
| k8s probe address   | `HEALTH_ADDR`                                        |
| Logging             | `LOG_LEVEL` / `LOG_FORMAT`                            |

Without `TLS_CERT_FILE`/`TLS_KEY_FILE` the worker dials cleartext h2c; with them it dials
mTLS (presenting the client cert, verifying the broker against `BROKER_CA_FILE`), and the
cert/key/CA hot-reload on `TLS_RELOAD_INTERVAL` so a rotation needs no restart.

## Consuming a providerconfig

A providerconfig has two axes, delivered together to `onConfig`: a JSON `spec` (a config
document you parse) and opaque `data` bytes (a bundle). The **effective** config a task runs
on is the kind default overlaid with the resource's optional per-resource override (which
rides the task in `req.env`) — merge them in `work`:

- `effectiveConfig<T>(defaultSpec, req.env.providerConfig)` — deep-merges the override onto
  the default `spec` (override wins per key) and parses into your `T`.
- `effectiveBundle(defaultData, req.env.providerBundle)` — whole-replaces the bundle (opaque
  bytes can't deep-merge).

## Typing spec/status/config from the manifest

`req.resource.spec`, the `status` you return, and the config `T` above are all shapes a
kind's **manifest** already describes as JSON Schema — the source of truth the core validates
against. Rather than hand-write a matching interface, **generate it from the manifest** with
the repo's `just gen-types` recipe (it derives `<Kind>Spec` / `<Kind>Status` / `<Kind>Config`
for both Go and TS from `*.kind.json`), then `import type` the generated interfaces. The
[TypeScript demo](../examples/demos/typescript) does exactly this — see its `npm run gen`. A
schema edit + regenerate keeps the types in lockstep across languages, no drift.

## Building

```sh
npm install
npm run gen        # regenerate the worker.proto stubs into gen/ (only after a proto change)
npm run typecheck  # tsc --noEmit
npm run build      # emit the distributable dist/ (JS + .d.ts + sourcemaps)
```

`npm run build` compiles `src/` + `gen/` to `dist/` with plain `tsc` (see
`tsconfig.build.json`). The published surface is `dist/src/index.js` + its `.d.ts`
(`package.json` `exports`/`types`); `dist/` is gitignored and rebuilt automatically on
`prepare` (git installs) and `prepublishOnly` (registry publish).

## Installing

```sh
npm install converge-worker-sdk
```

Then import from it:

```ts
import { serve, type Provider } from "converge-worker-sdk";
```

To use an unreleased build (e.g. before the next publish, or from a fork), install it
straight from the repo or a local tarball — both build `dist/` automatically via the
`prepare` script:

```sh
# git dependency (builds on install):
#   { "dependencies": { "converge-worker-sdk": "github:salesforce/converge#main" } }

# or a local tarball:
cd sdk-ts && npm pack        # → converge-worker-sdk-0.1.0.tgz
npm install /path/to/converge-worker-sdk-0.1.0.tgz
```

## See also

- **[docs/spec/sdk-spec.md](../docs/spec/sdk-spec.md)** — the normative capability + naming
  spec every worker SDK (this one, the Go `sdk-go/converge`, and future bindings) satisfies.
- **[sdk-go/converge](../sdk-go/converge)** — the Go worker SDK, the same shape.
