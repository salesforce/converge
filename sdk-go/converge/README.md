# converge — the worker client SDK

[![Go Reference](https://pkg.go.dev/badge/github.com/salesforce/converge/sdk-go/converge.svg)](https://pkg.go.dev/github.com/salesforce/converge/sdk-go/converge)

`converge` is the client SDK for building a **worker** — the process that hosts your
reconcile logic, pulls work from a Converge broker, and reports the result. It is shaped
like a cloud SDK, not a framework: you declare what your worker can do as a slice of
`converge.Provider`, install a signal context, and call `converge.Serve`. The SDK reads
config from the environment and owns the whole broker relationship — dialing,
reconnect-with-backoff, config prime/refresh, the readiness poller, and graceful drain —
so you never touch a transport, an HTTP client, or a reconnect loop. The caller owns only
the context and the exit decision.

You do **not** need a database, and you do **not** write any control-plane code. The broker
owns the queue, the lease, and all DB I/O, and ships each task's effective config in the
task itself.

```go
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/salesforce/converge/sdk-go/converge"
	"example.com/myworker/dbprovider"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Serve reads BROKER_ADDR / TLS_* / WORKER_* from the environment, then dials, serves,
	// reconnects, and drains; it blocks until ctx ends.
	if err := converge.Serve(ctx, []converge.Provider{dbprovider.New()}); err != nil {
		log.Fatal(err)
	}
}
```

## Install

```sh
go get github.com/salesforce/converge/sdk-go/converge
```

You import **one** package. `converge` carries everything: `Serve`, the `Provider`
interface, and the per-task types your `Work` reads and returns (`ReactionRequest`,
`Outcome`, `Terminal`, `EffectiveConfig`). Nothing from the control-plane core leaks in —
the SDK wraps only the generated worker proto.

## The one contract: `converge.Provider`

Everything the SDK needs from your business logic is one interface, and it is **one
instance per `(kind, version)`**:

```go
type Provider interface {
	Kind() KindVersion               // the ONE (kind, version) this provider serves
	Work(ctx, req) (Outcome, error)  // run one task for that pair
	OnConfig(cfg ProviderConfig)     // react to a providerconfig push {Spec, Data}
	Ready() bool                     // "can I do work right now?"
}
```

- **`Kind`** — the single `(kind, version)` pair. Pure; called once at registration.
  Versions are explicit and `>= 1` (no implicit v1).
- **`Work`** — pure decision logic. It reads `req` (spec, generation, observed children,
  the per-resource config override on `req.Env`) and returns an `Outcome` the core applies.
  Wrap the error with `converge.Terminal(err)` to stop retries; a plain error retries with
  backoff. A kind that declares several reactions switches on `req.Reaction`. See
  [Consuming a providerconfig](#consuming-a-providerconfig) for how to resolve the
  effective config here.
- **`OnConfig`** — receives the kind's **default** `{Spec, Data}` providerconfig: once at
  startup with the primed default, and again whenever an operator edits it (an empty `cfg`
  means the default was deleted). It is the hook to redial a client or recompile a bundle.
  It does **not** carry a per-resource override, and it is not where the merge happens; see
  [Consuming a providerconfig](#consuming-a-providerconfig). A no-config
  provider makes this a no-op.
- **`Ready`** — the SDK polls this. It is your **entire bring-up contract**: the SDK never
  calls a setup step, so you dial your own downstreams on your own schedule (in a
  constructor, a background goroutine, or lazily on first `Work`) and simply report
  whether you can currently do work. See [Readiness](#readiness--shedding-a-degraded-kind).

A pure leaf provider is tiny:

```go
type Provider struct{}

func (Provider) Kind() converge.KindVersion {
	return converge.KindVersion{Kind: "database", Version: 1}
}
func (Provider) Work(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	// …reconcile req.Resource.Spec against the real world; honor ctx…
	return converge.Outcome{Status: status}, nil
}
func (Provider) OnConfig(converge.ProviderConfig) {}        // no config
func (Provider) Ready() bool                      { return true } // no downstream to dial
```

## Serving several kinds or versions

List one provider per pair in the slice you pass to `Serve`:

```go
providers := []converge.Provider{dbProvider{}, cacheV1{}, cacheV2{}}
if err := converge.Serve(ctx, providers); err != nil {
	log.Fatal(err)
}
```

**Two versions of one kind are two providers**, each with its own `Work` — a v1 spec and a
v2 spec have different shapes, so v2 code handles v2 tasks and a v1 worker never sees a v2
task. A provider that owns a set of pairs (a shared backend, or an env-driven set) exposes
a constructor returning `[]converge.Provider`, which you splice into the slice:

```go
ps, err := myprovider.New() // []converge.Provider — one per (kind, version)
if err != nil {
	log.Fatal(err)
}
if err := converge.Serve(ctx, append([]converge.Provider{dbProvider{}}, ps...)); err != nil {
	log.Fatal(err)
}
```

## Consuming a providerconfig

A providerconfig has two axes, delivered together: a JSON **`Spec`** (a structured config
document you unmarshal into your own type) and opaque raw **`Data`** bytes (a bundle — a
zip of Starlark files, a template archive, whatever your provider materialises).

There are two levels:

- The **kind default** — one document per `(kind, version)`, applied by an operator.
- A **per-resource override** — an optional providerconfig carried by an individual
  resource, layered on top of the default just for that resource.

The **effective** config a task runs on is `default ⊕ override`:

- **`Spec` deep-merges** — the default is the base and the override wins **per key** (nested
  objects are merged, not replaced). An absent/empty override is the default verbatim; a
  malformed override never wipes the default.
- **`Data` (the bundle) is a whole-artifact replace** — the override bundle wins if present,
  else the default's. A bundle is opaque bytes and can't be deep-merged.

**Where each piece lives:** `OnConfig` gives you the **default** (and fires again when an
operator edits it). The **override** rides on the task in `req.Env`. The **merge happens in
`Work`, per task** — two helpers, both taking raw bytes (you pass what you already hold):

- `converge.EffectiveConfig[T](defaultSpec, override)` — deep-merges the override onto the
  default `spec` and decodes into your `T`.
- `converge.EffectiveBundle(defaultData, override)` — whole-replaces the bundle (override
  wins if present).

```go
type dbConfig struct {
	Endpoint string `json:"endpoint"`
	PoolSize int    `json:"pool_size"`
}

type Provider struct {
	mu  sync.RWMutex
	def converge.ProviderConfig // the kind default, kept current by OnConfig
}

// OnConfig stores the latest DEFAULT (and is the place to react to an operator's edit,
// e.g. redial). It does not carry a per-resource override.
func (p *Provider) OnConfig(cfg converge.ProviderConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.def = cfg
}

func (p *Provider) Work(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	p.mu.RLock()
	def := p.def
	p.mu.RUnlock()

	// effective spec   = kind default ⊕ this resource's per-resource override
	cfg := converge.EffectiveConfig[dbConfig](def.Spec, req.Env.ProviderConfig)
	// effective bundle = override bundle if present, else the default's
	bundle := converge.EffectiveBundle(def.Data, req.Env.ProviderBundle)

	_ = cfg
	_ = bundle
	// …reconcile using the effective config…
	return converge.Outcome{}, nil
}
```

**Startup:** you don't wait for an operator's edit to see the default. The SDK **primes**
each pair's default at startup (before the first task) and fires your `OnConfig` with it
then — so `OnConfig` is called once at bring-up with the current default, and again on every
later edit (and on the periodic re-pull if the default moved). A provider that needs its
config to dial a downstream before it reports `Ready` can therefore do that work in
`OnConfig`: the primed call arrives before any task, and `Ready` flips true once the dial
succeeds.

## Configuration

`Serve` reads all config from the environment. A malformed value (a bad duration, a
non-numeric count) is a loud error rather than a silent fallback to a default.

| Concern             | Env var                                             |
| ------------------- | --------------------------------------------------- |
| Broker address      | `BROKER_ADDR`                                        |
| mTLS keypair + CA   | `TLS_CERT_FILE` / `TLS_KEY_FILE` / `BROKER_CA_FILE`  |
| Cert reload poll    | `TLS_RELOAD_INTERVAL`                                |
| Per-pod parallelism | `WORKER_MAX_PARALLEL`                                |
| Drain grace         | `WORKER_DRAIN_TIMEOUT`                               |
| Kinds filter        | `WORKER_KINDS`                                       |
| k8s probe address   | `HEALTH_ADDR`                                        |
| Logging             | `LOG_LEVEL` / `LOG_FORMAT`                            |

`WORKER_MAX_PARALLEL` is a **per-pod** cap applied **per pair** (not shared across kinds);
the cluster-global per-kind cap is a separate operator concern set in the kind manifest.
`WORKER_KINDS` is a comma-separated allowlist that narrows which of the declared providers
this pod actually serves; `HEALTH_ADDR` is an optional `:port` that exposes Kubernetes
liveness/readiness probes.

### Production TLS

The broker requires mutual TLS in production. Point the worker at its keypair and the
broker CA via the environment; short-lived certs **hot-reload** on both sides of the
handshake (polled at `TLS_RELOAD_INTERVAL`), so a rotation needs no restart:

```sh
TLS_CERT_FILE=/tls/worker.crt \
TLS_KEY_FILE=/tls/worker.key \
BROKER_CA_FILE=/tls/ca.crt \
TLS_RELOAD_INTERVAL=30s \
BROKER_ADDR=broker:9090 ./myworker
```

## Readiness — shedding a degraded kind

A worker process hosts many providers over ONE stream. If one provider's downstream breaks
(its Kafka is unreachable, its cloud API is throttling), you don't want the broker to keep
pushing that kind's work to a worker that will only fail it — and you don't want to drop
the whole stream, because the other kinds are fine.

`Ready` is how a worker sheds a **single** pair:

- The SDK polls `Ready` (default every 2s). A `true→false` edge tells the broker "stop
  sending me this pair's work" (an `Interest` **RS-**); `false→true` resumes it (**RS+**).
- On RS-, the pair's resources **park** (stay queued for a healthy worker); the pair's
  healthy siblings on the same stream are unaffected.
- A task already **running** when you go unready finishes normally; a task that arrives in
  the tiny race window fails **transiently** (never terminal — a health outage must not
  poison-pill a resource) and re-dispatches elsewhere.
- If `Ready` is already false at connect, the RS- rides the initial subscribe, so a pair
  that boots degraded gets zero tasks.

`Ready` is *advisory* — it never fences correctness (the broker owns the lease); it only
steers where work lands.

## What the SDK does not do

It does not manage **your** external clients (a Kafka producer, a DB pool, a cloud SDK):
open those in your `main` and `defer client.Close()` them, exactly as you would without
Converge. The SDK owns only the broker relationship.

## See also

- **[Implementing a provider](../../docs/implementing-a-provider.md)** — the full
  end-to-end guide: provider → worker → kind manifest (CRD), plus the reaction contract,
  composition, and the versioning rules.
- **Reference providers** — [`cmd/stdworker`](../../cmd/stdworker) (the shipped `std*`
  providers, incl. the generic `stdio` "any language" seam) and
  [`examples/demos`](../../examples/demos) (the classic + data-driven demos).
- Runnable examples: see the **Example** blocks on the
  [package reference](https://pkg.go.dev/github.com/salesforce/converge/sdk-go/converge).
