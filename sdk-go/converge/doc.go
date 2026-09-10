// Package converge is the CLIENT SDK for building a converge worker — a normal,
// high-level library shaped like a cloud SDK, not a framework. The author declares what
// the worker can do as a slice of converge.Provider, installs a signal context, and calls
// converge.Serve. The SDK hides ALL the plumbing — it reads config from the environment,
// dials the broker (h2c or mTLS with cert hot-reload), handles reconnect-with-backoff,
// primes + refreshes config, polls readiness, and drains on shutdown. The dev never
// touches connectrpc, an HTTP transport, or a reconnect loop.
//
// The one thing the SDK does NOT do is manage the author's OWN external clients (a
// Kafka producer, a DB pool, a cloud SDK): the author opens those in their own main and
// `defer client.Close()`s them, exactly as they would without converge. A worker is:
//
//	func main() {
//	    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
//	    defer stop()
//	    // Serve reads BROKER_ADDR / TLS_* / WORKER_* from the env, then blocks until ctx
//	    // ends; it dials, serves, reconnects, and drains. The caller owns ctx + exit code.
//	    if err := converge.Serve(ctx, []converge.Provider{myProvider}); err != nil {
//	        log.Fatal(err)
//	    }
//	}
//
// The whole author contract is ONE type — converge.Provider — one instance per (kind,
// version), and the SDK calls only its four methods, NEVER a setup/bring-up step:
//
//	type Provider interface {
//	    Kind() KindVersion               // the ONE (kind, version) this provider serves
//	    Work(ctx, req) (Outcome, error)  // run one task for that pair
//	    OnConfig(cfg ProviderConfig)     // the kind DEFAULT {Spec, Data} (at startup + on edit)
//	    Ready() bool                     // "can I do work right now?"
//	}
//
// A worker that serves several pairs lists ONE Provider per pair in the slice passed to
// Serve, and the SDK routes each claimed task to the matching provider, so
// Work/OnConfig/Ready never take a kind/version argument (the pair is fixed by Kind). Two
// versions of one kind are two providers, each with its own Work. A provider dials its OWN
// downstreams on its own schedule and reports readiness through Ready — that is the entire
// bring-up contract (readiness, not a Setup the SDK drives).
//
// OnConfig delivers the kind's DEFAULT providerconfig (primed at startup, re-fired on an
// operator's edit). The EFFECTIVE config a task runs on is that default overlaid with the
// resource's optional per-resource override, which rides on the task; compute it in Work
// with converge.EffectiveConfig (Spec deep-merges, the Data bundle is a whole replace).
//
// Configuration is ENV-ONLY: BROKER_ADDR (the broker's Connect address); TLS_CERT_FILE /
// TLS_KEY_FILE / BROKER_CA_FILE (mTLS keypair + CA) with TLS_RELOAD_INTERVAL (cert reload
// poll); WORKER_MAX_PARALLEL (per-pod parallelism, applied per pair); WORKER_DRAIN_TIMEOUT
// (drain grace on SIGTERM); WORKER_KINDS (comma-separated filter of kinds to serve);
// HEALTH_ADDR (optional :port for k8s liveness/readiness probes); LOG_LEVEL / LOG_FORMAT.
// A malformed value (a bad duration, a non-numeric count) is a loud error from Serve, not
// a silent fallback.
//
// Per-kind READINESS (Ready) is how a worker hosting many providers over one stream
// sheds a SINGLE degraded kind: when Ready flips false the SDK tells the broker "stop
// sending me THIS (kind, version)'s work" (an Interest RS-), resuming on recovery — so
// one provider's Kafka breaking an hour after boot doesn't drop the whole stream.
//
// Like the rest of the SDK seam, this imports ONLY sdk-go/*, the domain-free leaf
// utils in pkg/* (drainctx, tlsreload, wirelimits), and third-party — never
// internal/* and never a database. The broker owns the work_queue lease + all DB I/O:
// it ships each task's per-resource config override on the StageTask and the kind default
// through the config cache (prime + push), so a worker needs no DATABASE_URL — only the
// broker address and, for mTLS, a client keypair.
package converge
