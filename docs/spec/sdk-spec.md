# Worker SDK parity spec

Every converge worker SDK — Go (`sdk-go/`), TypeScript (`sdk-ts/`), and any future
binding (Java, Python, …) — is a wrapper around the **same** contract: the public worker
proto `proto/converge/worker/v1/worker.proto`. That proto is the ONLY thing an SDK shares
with the broker (see [architecture.md](../architecture.md) and the archtest guards). Nothing
Go/TS-specific and nothing from the control-plane core is shared.

Because they wrap one proto, the SDKs must be **feature-matched and name-matched**: a
reader who knows one SDK must find the same capability, in the same file, under the same
function name, in another. This document is that contract. It is normative — a new SDK is
"done" when it satisfies every capability below, and a change to one SDK's shape updates
this spec and the sibling SDKs in the same change.

## Public surface (all SDKs)

The surface an author touches is exactly:

- **`serve(providers, …)`** — the high-level entrypoint. Reads env config, DIALS the
  broker itself, blocks, owns the whole relationship, returns on clean shutdown. The 99%
  path.
- **`runWorker(ctx, client, providers, opts)`** — the low-level tier beside `serve`, for a
  caller who already HOLDS a broker client (a test with an in-process broker, an embedder
  with a bespoke transport/auth). `serve` is a thin wrapper: read env → dial a `Transport`
  → `runWorker`. Both funnel through ONE run loop; there is no second worker
  implementation. (Go: `converge.RunWorker` + `converge.Transport` + `converge.RunOptions`;
  a language binding exposes the analogous low-level runner.)
- **`Provider`** — the one contract a worker author implements: `kind()` / `work()` /
  `onConfig()` / `ready()`. ONE provider serves ONE `(kind, version)`.
- **the value types `work()` speaks** — `ReactionRequest`, `Outcome`, `Resource`,
  `Condition`, `ProviderConfig`, `KindVersion`, `Trigger`, and the `terminal()` error
  marker + the `EffectiveConfig`/`EffectiveBundle` config-merge helpers.

Everything else (the runner loop, the transport, the config cache, the proto converters)
is an INTERNAL implementation detail — unexported in Go, non-exported in TS.

## File map (name-for-name)

| Concern | `sdk-go/converge/` | `sdk-ts/src/` |
|---|---|---|
| the SDK's own contract types | `types.go` | `types.ts` |
| proto ⇄ types converter | `convert.go` | `convert.ts` |
| `serve()` — high-level env entrypoint | `serve.go` | `serve.ts` |
| `runWorker()` — low-level tier + `RunOptions` | `runworker.go` | `runworker.ts` |
| the WorkStream runner loop | `run.go` + `runner.go` | `runner.ts` |
| provider registration / fan-out | `register.go` + `worker.go` | (in `serve.ts`) |
| the `Provider` contract | `provider.go` | (in `types.ts`) |
| env config | `env.go` | `config.ts` |
| broker transport (dial, mTLS, h2c) | `transport.go` + `transport_iface.go` | `transport.ts` |
| default-config cache (prime/pull/refresh/push) | `configcache.go` | `configcache.ts` |
| readiness poller (`ready()` → RS-/RS+) | `health.go` | `health.ts` |
| reconnect/retry backoff | `backoff.go` | `backoff.ts` |
| package doc / public re-exports | `doc.go` | `index.ts` |
| example worker | `example_test.go` | `../example/*.ts` |
| in-process single-task codec (test seam) | `executetask.go` | (Go-only) |

Shared **function names** across the converter (add to BOTH when you add one):
`requestFromTask`, `completeFromOutcome`, `failComplete`.

`executetask.go`'s `ExecuteTask` is a Go-only seam: it runs one proto `StageTask` through
the REAL decode→`work()`→encode in-process, so the integration harness exercises the exact
wire path a shipped worker does without a broker (see [architecture.md](../architecture.md)).
A TS SDK needs no analogue unless it grows an in-process test harness.

## Capability checklist (normative)

Every SDK MUST implement all of these. `sdk-go` is the reference implementation.

### 1. Configuration — env-first, zero-config
Read from the environment (the twelve-factor / cloud-SDK path), each variable optional
with a documented default:

| Env var | Meaning | Default |
|---|---|---|
| `BROKER_ADDR` | broker Connect URL the worker dials | — (required) |
| `TLS_CERT_FILE` / `TLS_KEY_FILE` | worker client cert + key (mTLS) | unset → h2c cleartext |
| `BROKER_CA_FILE` | broker CA to verify the server | unset → system roots |
| `TLS_RELOAD_INTERVAL` | cert hot-reload poll interval | 30s |
| `WORKER_MAX_PARALLEL` | per-worker concurrency ceiling | 100 |
| `WORKER_DRAIN_TIMEOUT` | graceful-drain budget on shutdown | 30s |
| `WORKER_KINDS` | comma-list filter — serve only these kinds of the registered set | unset → all |
| `HEALTH_ADDR` | address to serve `/livez` + `/readyz` probes | unset → no probes |
| `LOG_LEVEL` / `LOG_FORMAT` | log verbosity + text/json | info / text |

A malformed value is a LOUD error from `serve`, never a silent fallback.

### 2. Transport — dial the broker, hardened
- Prior-knowledge cleartext **h2c** when no TLS is set (the bidi WorkStream requires
  end-to-end HTTP/2; plain HTTP/1.1 is rejected).
- **mTLS** when `TLS_CERT_FILE`/`TLS_KEY_FILE` are set: present the client cert, verify
  against `BROKER_CA_FILE`.
- **Cert hot-reload**: re-read cert/key/CA on `TLS_RELOAD_INTERVAL` so a rotation needs no
  restart (present per-handshake; re-derive the CA pool per dial).
- Pool tuning so a fleet reconnect storm reuses warm connections (raised
  MaxIdleConnsPerHost), bounded so one worker can't exhaust broker fds.
- Message-size caps agreed with the broker (`wirelimits`), gzip on outbound.

### 3. WorkStream runner loop
- Open the bidi `WorkStream`, send `Subscribe` (kinds + kind_versions + max_inflight +
  worker_id) as the FIRST message.
- Pull `StageTask`s; for each, `requestFromTask` → dispatch to the matching provider's
  `work()` → `completeFromOutcome` (or `failComplete`) sent back UP THE SAME stream (never
  a separate unary — pins the completion to the dispatching broker).
- Concurrency bounded by `WORKER_MAX_PARALLEL`; a panic in `work()` is isolated (fails
  that task transient, never crashes the worker).
- A `work()` error → transient (re-dispatched); a `terminal()`-wrapped error → terminal.

### 4. Readiness — `ready()` → RS-/RS+
- Poll each provider's `ready()`. On a true→false edge, advertise `Interest{has_worker:false}`
  (RS-) for that `(kind, version)` so the broker STOPS sending it work; false→true → RS+.
- Initial readiness rides the `Subscribe` burst (a kind that boots degraded gets zero
  tasks) — `InitialUnready`.
- Never poison-pills: an in-flight task finishes; readiness only steers where work lands.

### 5. Providerconfig — the DEFAULT lifecycle
- **Prime** each registered `(kind, version)`'s default at startup via `GetProviderConfig`
  (retried with jittered backoff until the broker answers), so `work()` sees config from
  task one.
- **Refresh** periodically (failsafe behind the push).
- **Push**: apply broker `ProviderConfigUpdate` frames live.
- On any change (prime / refresh / push), fire the provider's `onConfig(cfg)` with the
  full `{Spec, Data}` monolith; an empty frame = the default was deleted.
- Re-pull on every reconnect (OnConnect) so a reconnecting worker converges at once.

### 6. Reconnect + backoff
- A dropped stream reconnects internally (never surfaced to the author) until the context
  ends.
- **Exponential backoff + jitter** (`backoff`): a fleet loses a broker at once, so an
  un-jittered backoff re-storms in lockstep. Escalate on the raw value; jitter the sleep;
  reset only after a genuinely healthy session.

### 7. Graceful drain + lifecycle
- `serve` blocks until the caller's context/signal ends, then drains in-flight work within
  `WORKER_DRAIN_TIMEOUT` before returning.
- The CALLER owns the signal + exit code; `serve` touches no signals and never exits the
  process.

## Divergences allowed

Only idioms that don't change behavior: Go returns `error`, TS throws/rejects; Go uses
`context.Context`, TS uses `AbortSignal`; naming follows each language's case convention
(`requestFromTask` in both; `KindVersion` vs `kindVersion`). Everything in the capability
checklist must be present and behave identically.
