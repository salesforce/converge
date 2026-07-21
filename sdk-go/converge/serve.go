package converge

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// Serve is the reusable BODY of a converge worker binary: it configures logging, reads
// config from the environment (via New — BROKER_ADDR / TLS_* / WORKER_*, cloud-SDK
// style), registers the given providers (scoped by WORKER_KINDS), optionally starts the
// DB-free k8s probe listener (HEALTH_ADDR), then BLOCKS on the worker's Run until ctx is
// cancelled — draining in-flight work — and returns its error. It is the convenience
// shell (kinds filter + probes + logging) over New(...).Add(...).Run(ctx).
//
// The caller owns ctx and the exit decision: build the context (a SIGINT/SIGTERM
// signal.NotifyContext for a standalone binary, or a parent errgroup/request ctx when a
// worker is embedded in a bigger app) and act on the returned error. A standalone worker
// main is 4 lines:
//
//	func main() {
//		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
//		defer stop()
//		if err := converge.Serve(ctx, []converge.Provider{myprov.Provider{}}); err != nil {
//			log.Fatal(err)
//		}
//	}
//
// A program that wants finer control skips Serve and drives the Worker directly
// (New(...).Add(...).Run(ctx)) — see examples/demos/datadriven/cmd/worker.
//
// providers each satisfy Provider (the ONE SDK provider contract). Returns a non-nil
// error if WORKER_KINDS matched none of the providers (nothing to run), or whatever
// Worker.Run returns.
func Serve(ctx context.Context, providers []Provider) error {
	setupLogger()
	// The worker reads BROKER_ADDR / TLS_* / WORKER_* from the environment itself
	// (cloud-SDK style) and owns dial + reconnect + config + readiness + drain.
	// Construction errors only on a malformed SET env var — surface it rather than run
	// mis-configured.
	w, err := newWorker()
	if err != nil {
		return err
	}

	// Register the providers this binary serves, scoped by WORKER_KINDS. Each provider
	// self-manages its bring-up and reports Ready — the SDK sends it work only for the
	// (kind, version) pair it advertises via Kind().
	want := kindFilter(os.Getenv("WORKER_KINDS"))
	added := 0
	for _, p := range providers {
		if want != nil && !providerWanted(p, want) {
			continue // no wanted kind → don't register
		}
		w.register(p)
		added++
	}
	if added == 0 {
		return errNoKinds(os.Getenv("WORKER_KINDS"))
	}

	// Optional DB-free k8s probe listener (a worker holds no DB, so liveness is a bare
	// 200 while the process is up; /readyz flips NotReady once shutdown begins so k8s
	// pulls the pod from Service endpoints during the drain).
	if addr := os.Getenv("HEALTH_ADDR"); addr != "" {
		serveProbes(ctx, addr)
	}

	slog.Info("worker starting", "providers", added)
	return w.run(ctx) // blocks until ctx is cancelled, then drains + returns
}

// serveProbes starts the DB-free k8s liveness/readiness listener on addr, bound to ctx.
func serveProbes(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	live := func(rw http.ResponseWriter, _ *http.Request) { rw.WriteHeader(http.StatusOK) }
	mux.HandleFunc("/healthz", live)
	mux.HandleFunc("/livez", live)
	mux.HandleFunc("/readyz", func(rw http.ResponseWriter, _ *http.Request) {
		if ctx.Err() != nil { // shutdown began → NotReady
			rw.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		rw.WriteHeader(http.StatusOK)
	})
	hs := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := hs.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("worker health listener", "err", err)
		}
	}()
}

// kindFilter parses the comma-separated WORKER_KINDS env into a set, or nil (all kinds).
func kindFilter(csv string) map[Kind]bool {
	csv = strings.TrimSpace(csv)
	if csv == "" {
		return nil
	}
	m := map[Kind]bool{}
	for k := range strings.SplitSeq(csv, ",") {
		if k = strings.TrimSpace(k); k != "" {
			m[Kind(k)] = true
		}
	}
	return m
}

// providerWanted reports whether a provider's kind is in want (reads the pure Kind(), no
// dial), so a provider whose kind isn't wanted is skipped without registering.
func providerWanted(p Provider, want map[Kind]bool) bool {
	return want[p.Kind().Kind]
}

// errNoKinds is the "nothing to run" error, kept as a named builder so the message stays
// in one place.
func errNoKinds(workerKinds string) error {
	return &noKindsError{workerKinds: workerKinds}
}

type noKindsError struct{ workerKinds string }

func (e *noKindsError) Error() string {
	return "converge: no providers to run (WORKER_KINDS=" + strconv.Quote(e.workerKinds) + " matched nothing)"
}

// setupLogger configures the default slog logger from LOG_LEVEL (a level name or a
// numeric slog level) + LOG_FORMAT (text|json). Defaults: info / text.
func setupLogger() {
	lvl := slog.LevelInfo
	if s := os.Getenv("LOG_LEVEL"); s != "" {
		if l, err := strconv.Atoi(s); err == nil {
			lvl = slog.Level(l)
		} else {
			switch strings.ToLower(s) {
			case "debug":
				lvl = slog.LevelDebug
			case "warn":
				lvl = slog.LevelWarn
			case "error":
				lvl = slog.LevelError
			}
		}
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler = slog.NewTextHandler(os.Stderr, opts)
	if strings.EqualFold(os.Getenv("LOG_FORMAT"), "json") {
		h = slog.NewJSONHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(h))
}
