package converge

import (
	"context"
	"log/slog"
	"time"
)

// runworker.go is the LOW-LEVEL worker tier — the advanced entrypoint beside Serve.
//
// Serve is the 99% path: it reads config from the environment, DIALS the broker itself
// (h2c/mTLS + cert hot-reload), and owns the whole relationship. RunWorker is for the
// caller who already HOLDS a broker client — a test standing up an in-process broker, an
// embedder with a bespoke transport/auth, a program that dials on its own schedule — and
// wants the SDK to run the WorkStream loop over it. It is the analogue of driving a
// grpc.ClientConn directly instead of a generated high-level client.
//
// Both tiers share ONE body: Serve builds the env config, dials a Transport, and calls
// the same internal run loop RunWorker does — there is no second worker implementation.

// Transport is the broker RPC surface a worker speaks: the bidi WorkStream (pull tasks,
// report results/readiness) + GetProviderConfig (prime/refresh the default config). The
// generated workerpbconnect.WorkerServiceClient satisfies it, so a caller passes that
// (built over any http.Client + base URL + TLS); a test passes an in-process fake. It is
// deliberately the worker-facing service ONLY — a Transport cannot reach the broker↔broker
// mesh, so a worker structurally cannot.
type Transport = transport

// RunOptions are the knobs RunWorker needs that Serve would otherwise read from the
// environment. All optional — a zero RunOptions runs with the SDK defaults (MaxInflight
// defaultMaxInflight, DrainGrace defaultDrainGrace, the default slog logger).
//
// A worker does not report its own identity: the broker derives it from the connection it
// OBSERVES (the mTLS client cert's SPIFFE ID, a trusted service-mesh header, or the peer IP).
type RunOptions struct {
	// MaxInflight bounds concurrent task execution (also sizes the broker's per-kind fanout
	// buffer headroom for this worker). 0 → the SDK default.
	MaxInflight int
	// DrainGrace is how long RunWorker lets in-flight handlers finish after ctx is cancelled
	// before abandoning them. 0 → the SDK default.
	DrainGrace time.Duration
	// Logger is the base logger; per-task loggers derive from it. nil → slog.Default().
	Logger *slog.Logger
}

// RunWorker serves the given providers over an ALREADY-BUILT broker client until ctx is
// cancelled, then drains in-flight work and returns. It owns the WorkStream loop —
// subscribe, pull tasks, dispatch to each provider's Work, report results, prime/refresh
// the default config (firing OnConfig), poll Ready (RS-/RS+), and reconnect-with-backoff
// across stream drops — everything Serve does EXCEPT dialing (the caller supplies the
// client) and reading the environment (the caller supplies RunOptions).
//
// It requires at least one provider (a worker that can execute nothing would poll forever)
// and a non-nil client; it returns an error only for a fatal setup problem or nil on a
// clean ctx-cancel shutdown. Most workers should use Serve; reach for RunWorker only when
// you must inject the transport (tests, embedders, custom auth).
func RunWorker(ctx context.Context, client Transport, providers []Provider, opts RunOptions) error {
	w, err := newWorker(withTransport(client))
	if err != nil {
		return err
	}
	if opts.MaxInflight > 0 {
		w.cfg.maxInflight = opts.MaxInflight
	}
	if opts.DrainGrace > 0 {
		w.cfg.drainGrace = opts.DrainGrace
	}
	if opts.Logger != nil {
		w.cfg.logger = opts.Logger
	}
	for _, p := range providers {
		w.register(p)
	}
	return w.run(ctx)
}
