// Package health serves the Kubernetes probe endpoints — /healthz, /livez,
// and /readyz — for every Converge pod regardless of role.
//
// The three endpoints map to the two kubelet probe semantics:
//
//   - /livez, /healthz  — LIVENESS. "Is the process up and its HTTP loop
//     responsive?" These do NOT touch the database: a DB blip must not make
//     k8s kill an otherwise-healthy pod (that would turn a transient outage
//     into a restart storm and pull connections out from under in-flight
//     work). They return 200 as long as the server can answer. /healthz is
//     a conventional alias for /livez so either name works in a probe spec.
//   - /readyz  — READINESS. "Should this pod receive traffic / be counted
//     up right now?" This pings the primary pool with a short timeout: a
//     pod that can't reach Postgres can't serve API reads or drain work, so
//     it reports NotReady and k8s pulls it from endpoints until the DB
//     recovers — without restarting it.
//
// The handlers are mounted on a caller-supplied mux so a control/all pod
// reuses its main API mux and a worker-only pod gets a tiny probe-only
// listener (see Handler), keeping every pod probeable with no per-role
// special-casing in the probe logic itself.
package health

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"
)

// draining flips true the moment a pod begins graceful shutdown (SIGTERM). While
// set, /readyz reports NotReady (503) BEFORE any DB ping, so k8s pulls the pod
// from its Service endpoints promptly — no NEW API request or WorkStream connection
// is routed to a pod that's tearing down. Liveness (/livez) stays 200 so the
// kubelet doesn't SIGKILL the still-draining process. Process-wide (one pod = one
// process), set once via SetDraining on the SIGTERM edge; never reset.
var draining atomic.Bool

// SetDraining marks this pod as shutting down so /readyz turns NotReady. Call it
// the instant the run ctx is cancelled (SIGTERM), before the drain — k8s then
// stops routing new traffic while in-flight work finishes.
func SetDraining() { draining.Store(true) }

// readyTimeout bounds the /readyz DB ping. Short on purpose: a readiness
// probe should fail fast and let k8s mark the pod NotReady rather than hang
// the probe (a hung probe eventually trips the probe's own timeout, but a
// tight bound here keeps the signal crisp and the pool connection freed).
const readyTimeout = 2 * time.Second

// Pinger is the subset of *pgxpool.Pool /readyz needs: a context-bounded
// round-trip to the database. Narrowed to an interface so the package has
// no pgx dependency and is trivially faked in tests.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Register mounts /healthz, /livez, and /readyz on mux. live and readyz are
// independent: liveness never touches the DB; readiness pings db. Pass a nil
// db to make /readyz a pure liveness check too (e.g. a pod with no pool). When
// metricsHandler is non-nil it is also mounted at GET /metrics (the Prometheus
// scrape endpoint — served here, on the plaintext port, never behind (m)TLS).
func Register(mux *http.ServeMux, db Pinger, metricsHandler http.Handler) {
	mux.HandleFunc("GET /livez", live)
	mux.HandleFunc("GET /healthz", live)
	mux.HandleFunc("GET /readyz", ready(db))
	if metricsHandler != nil {
		mux.Handle("GET /metrics", metricsHandler)
	}
}

// Handler builds a standalone probe-only http.Handler — used by pods (e.g.
// worker-<kind>) that don't run the full API server but still must answer
// kubelet probes. Also serves /metrics when metricsHandler is non-nil. Control/all
// pods instead call Register on their API mux.
func Handler(db Pinger, metricsHandler http.Handler) http.Handler {
	mux := http.NewServeMux()
	Register(mux, db, metricsHandler)
	return mux
}

// live answers liveness: the process is up and the HTTP loop is serving.
// Deliberately DB-free — see the package doc.
func live(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// ready answers readiness by pinging the database within readyTimeout. A
// nil db degrades to a pure liveness check (always ready). Derives its
// context from the request so a probe cancel/timeout aborts the ping.
func ready(db Pinger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		// Draining: report NotReady so k8s removes this pod from Service endpoints
		// while it finishes in-flight work — checked first, no DB ping needed.
		if draining.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("draining\n"))
			return
		}
		if db == nil {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
		defer cancel()
		if err := db.Ping(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready: " + err.Error() + "\n"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	}
}
