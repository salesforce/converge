package runtime

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// listenReconnectBackoff is the pause before re-establishing a dropped LISTEN.
const listenReconnectBackoff = 1 * time.Second

// Listener is the resilient LISTEN/NOTIFY capability the runtime's reactive
// duties (drainer, reactor dispatcher, resharder) depend on — the ONE
// Postgres-facing dependency that isn't already behind a Repo role interface.
// Injecting it (rather than a concrete *pgxpool.Pool) keeps those duties
// testable against a fake wake source and keeps pgx out of their signatures;
// the production implementation is PgxListener, a thin adapter over the pool.
//
// It is a pure LATENCY optimisation over each caller's failsafe poll: a window
// with no listener just means wakes fall back to the periodic tick, never a
// stuck task — so Listen never returns an error and exits cleanly only on ctx
// cancel.
type Listener interface {
	// Listen runs a resilient LISTEN loop on channel until ctx is cancelled,
	// calling onNotify for each notification and reconnecting (with backoff) on
	// any connection error. name tags the reconnect log line. onReady (optional;
	// pass 0 or 1) fires ONCE per successful (re)subscribe, right after the LISTEN
	// registers and BEFORE any notification is awaited — callers use it for a
	// catch-up read that closes the subscribe-vs-read race.
	Listen(ctx context.Context, name, channel string, onNotify func(), onReady ...func())
}

// PgxListener is the production Listener: a resilient LISTEN/NOTIFY loop over a
// pgx pool. Every runtime component that reacts to a DB NOTIFY shares this one
// implementation: the drainer (outbox_ready), the reactor dispatcher
// (lifecycle_ready), and the resharder (cluster_changed).
type PgxListener struct{ pool *pgxpool.Pool }

// NewPgxListener wraps a pool as a Listener. The pool is the substrate — a
// LISTEN registration lives on a session, so each Listen call holds one
// dedicated pooled connection for its lifetime.
func NewPgxListener(pool *pgxpool.Pool) *PgxListener { return &PgxListener{pool: pool} }

// Listen implements Listener over the pgx pool: it re-establishes the
// subscription on any connection error until ctx is cancelled.
func (l *PgxListener) Listen(ctx context.Context, name, channel string, onNotify func(), onReady ...func()) {
	var ready func()
	if len(onReady) > 0 {
		ready = onReady[0]
	}
	for ctx.Err() == nil {
		if err := l.listenOnce(ctx, channel, onNotify, ready); err != nil && ctx.Err() == nil {
			slog.Warn(name+" listener; will reconnect", "channel", channel, "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(listenReconnectBackoff):
			}
		}
	}
}

// listenOnce acquires one dedicated pooled connection — a LISTEN registration
// lives on its session, so the conn is held for the whole call and released on
// return — LISTENs on channel, and calls onNotify for each notification until
// ctx is cancelled or the connection errors. Returns nil only on ctx cancel.
// onReady (may be nil) fires once, right after the LISTEN registers.
func (l *PgxListener) listenOnce(ctx context.Context, channel string, onNotify func(), onReady func()) error {
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN "+channel); err != nil {
		return err
	}
	if onReady != nil {
		onReady()
	}
	for {
		// WaitForNotification blocks until a notification arrives, the
		// connection breaks, or ctx is cancelled.
		if _, err := conn.Conn().WaitForNotification(ctx); err != nil {
			if ctx.Err() != nil {
				return nil // clean shutdown
			}
			return err // conn dropped — caller reconnects
		}
		onNotify()
	}
}

// notify does a non-blocking, coalescing send on a buffered-1 wake channel: it
// records a wake without blocking, and if one is already pending it's a no-op
// (the pending wake already covers it). The single primitive behind every
// Trigger / wakeup edge in the runtime.
func notify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// Debounce forwards a SINGLE coalescing wake to out once raw has been QUIET for
// window, collapsing a BURST of source events into one downstream action on the
// SETTLED state. Trailing-edge: each raw event during the window RESETS the quiet
// timer, so the wake fires `window` after the LAST event, not the first. Runs
// until ctx is cancelled. The single debounce primitive shared by the
// membership-reactive loops (the Resharder's re-tile, the broker mesh's peer
// reconcile) — a scale event fires one INSERT/DELETE per pod, and without this
// each would act N times mid-flux instead of once on the final topology. It can
// only DELAY a reaction by at most window, never drop one; a failsafe tick always
// backstops it independently. Exported for the broker mesh (a different package).
func Debounce(ctx context.Context, window time.Duration, raw <-chan struct{}, out chan struct{}) {
	t := time.NewTimer(0)
	if !t.Stop() {
		<-t.C
	}
	defer t.Stop()
	armed := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-raw:
			// (Re)arm the quiet timer on every event — extends the window so a
			// steady burst collapses to one trailing wake.
			if armed && !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
			t.Reset(window)
			armed = true
		case <-t.C:
			armed = false
			notify(out) // coalescing send — the consumer sees one wake per settled burst
		}
	}
}
