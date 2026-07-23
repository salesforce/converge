package runtime

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// pgDeadlock is Postgres SQLSTATE 40P01.
const pgDeadlock = "40P01"

// isDeadlock reports whether err is a Postgres deadlock_detected error.
// Drainer / reaper / resyncer retry transiently on deadlock since
// their concurrent cascade-trigger writes (work_queue ON CONFLICT
// inside a resources UPDATE) cross lock orders that can't be
// statically reconciled without rearchitecting the cascade — retry
// is the standard fix for short-lived transactional queue
// processors.
func isDeadlock(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgDeadlock
}

// retryOnDeadlock invokes fn up to maxAttempts times, retrying only
// on Postgres deadlock errors. Other errors return immediately. The
// caller's name is used in the debug log so the source of the
// retried op is visible in the trace.
//
// Between retries it sleeps a CAPPED EXPONENTIAL backoff (1ms,2ms,4ms,… ≤100ms)
// with ±10% jitter. This is load-bearing, not cosmetic: Postgres only returns
// 40P01 after aborting THIS tx as the deadlock victim, while the blocking holder
// (a drainer's cascade_ready_change → schedule_eligible inside a seconds-long
// drain tx) still holds its locks. Retrying with ZERO delay would just re-collide
// against that still-held lock, exhausting all attempts in one round-trip burst
// so the caller drops its work (e.g. a post-commit ScheduleEligible(~1M) that
// would strand a whole composed subtree until the reaper's requeue ~RetryAfter
// later). The growing delay instead lets the retry
// block on an ORDINARY lock-wait and succeed once the holder commits; the jitter
// de-synchronizes concurrent victims so they don't re-converge (thundering
// herd). ctx-aware: a cancel during the long compose/drain or a clean shutdown
// returns at once rather than sleeping. Happy path (no deadlock) is unchanged —
// backoff is never reached.
func retryOnDeadlock(ctx context.Context, name string, fn func(context.Context) (int, error)) (int, error) {
	const maxAttempts = 3
	const baseDelay = time.Millisecond
	const maxDelay = 100 * time.Millisecond
	for attempt := 1; ; attempt++ {
		n, err := fn(ctx)
		if err == nil || !isDeadlock(err) {
			return n, err
		}
		if attempt >= maxAttempts {
			// EXHAUSTED all retries on a live deadlock — the dangerous case: the
			// caller gets the error and its work (e.g. a post-commit ScheduleEligible)
			// is dropped, stranding it until a slow-path backstop. Logged at ERROR so
			// it is never invisible; a sustained rate here means the backoff isn't
			// letting the holder commit and the cycle needs a lock-order fix, not more
			// retries. (Distinct from a single retried deadlock, which self-heals.)
			slog.Error(name+" deadlock: exhausted retries, dropping op", "attempts", attempt)
			return n, err
		}
		// A deadlock we WILL retry (self-heals once the holder commits). WARN, not
		// Debug, so a deadlock rate is visible during a normal run without turning on
		// debug logging — the ground-truth counter for "are we deadlocking".
		slog.Warn(name+" deadlock; retrying", "attempt", attempt)
		// attempt≥1 → first shift is <<0 = 1ms (no uint underflow); the <=0 guard
		// catches the shift overflowing past the type width to 0 at high attempts.
		delay := baseDelay << uint(attempt-1)
		if delay > maxDelay || delay <= 0 {
			delay = maxDelay
		}
		// ±10% jitter; int64(delay)/5+1 is always ≥1 so Int63n never panics.
		jitter := time.Duration(rand.Int63n(int64(delay)/5+1)) - delay/10
		select {
		case <-ctx.Done():
			return n, err
		case <-time.After(delay + jitter):
		}
	}
}

// pollLoop drives an adaptive busy/idle polling loop. tick is invoked each
// iteration and returns whether it found work.
//
// Backoff has THREE regimes, designed so a missed / rate-limited / dropped
// NOTIFY costs at most one `busy` interval of latency — never seconds:
//
//  1. PRODUCTIVE tick — refreshes the HOT WINDOW (see below) and:
//     - repollOnWork=true (the drainer): re-poll IMMEDIATELY (zero delay) so a
//     backlog, or rows that landed DURING the tick, drain back-to-back at full
//     speed.
//     - repollOnWork=false (reaper/resyncer/specgc): keep the fixed `busy`
//     cadence — these run a deliberately PACED, expensive bounded scan and
//     must not re-poll back-to-back.
//
//  2. HOT WINDOW — for `hotWindow` after the LAST productive tick (or wake), an
//     empty tick re-polls at `busy` (≈50ms), NOT `idle`. This is the key
//     reactivity guarantee: right after we did work, MORE work is likely
//     imminent (a cascade re-pend, a sibling finishing), and a wake for it may
//     be coalesced/lost — so we keep probing cheaply instead of trusting the
//     NOTIFY. A productive tick OR a `wakeup` resets the window, so a busy
//     system stays hot continuously.
//
//  3. COLD — once empty for the whole `hotWindow`, the next sleep grows to
//     `idle` (the failsafe sweep). A `wakeup` collapses straight back to a tick
//     and re-opens the hot window, so the long `idle` is only ever the steady-
//     state floor of a genuinely-quiescent system.
//
// hotWindow<=0 disables regime 2 (cold immediately after one empty tick) — used
// by the paced sweepers, whose `idle` already equals or barely exceeds `busy`.
//
// wakeup is an OPTIONAL latency-wake channel. When non-nil, a receive on it (a
// LISTEN/NOTIFY) interrupts the current sleep, runs a tick immediately, and
// re-opens the hot window. A nil wakeup reverts to a pure timer loop, so the
// reaper/resyncer/specgc call sites are unaffected — they pass nil.
//
// Errors from tick are logged with `name` for context — the loop
// continues unless ctx is done.
func pollLoop(
	ctx context.Context,
	name string,
	busy, idle time.Duration,
	hotWindow time.Duration,
	repollOnWork bool,
	wakeup <-chan struct{},
	tick func(ctx context.Context) (bool, error),
) {
	if idle <= 0 {
		idle = busy
	}

	t := time.NewTimer(busy)
	defer t.Stop()
	// hotUntil is the deadline through which empty ticks stay at `busy`. Seeded
	// to now so the loop isn't artificially hot before any work; the first tick
	// still runs promptly via the initial busy timer. time.Now() carries a
	// monotonic reading, so the Before/Add comparisons are wall-clock-skew-safe.
	hotUntil := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-wakeup:
			// Woken by a NOTIFY before the timer fired. Drain the (now stale)
			// timer so the next Reset is race-free; a fresh row is waiting, so
			// run a tick now and treat the wake as activity → re-open the hot
			// window so we stay reactive even if the FOLLOW-UP wake is coalesced.
			drainTimer(t)
			if hotWindow > 0 {
				hotUntil = time.Now().Add(hotWindow)
			}
		}
		work, err := tick(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error(name+" tick", "err", err)
		}
		var next time.Duration
		switch {
		case work && repollOnWork:
			// Drainer: re-poll immediately so a backlog — or rows that landed
			// DURING this tick — drain with zero delay. Refresh the hot window.
			if hotWindow > 0 {
				hotUntil = time.Now().Add(hotWindow)
			}
			next = 0
		case work:
			// Paced sweeper: found work but keep the fixed `busy` cadence so an
			// expensive bounded scan doesn't run back-to-back. Refresh the window.
			if hotWindow > 0 {
				hotUntil = time.Now().Add(hotWindow)
			}
			next = busy
		default:
			// Empty: stay HOT (poll at `busy`) until the window closes, then cool
			// to `idle`. A coalesced/lost wake for work that lands during the
			// window is caught within one `busy` interval, not after `idle`.
			next = idle
			if hotWindow > 0 && time.Now().Before(hotUntil) {
				next = busy
			}
		}
		drainTimer(t)
		t.Reset(next)
	}
}
