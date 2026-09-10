package runtime

import (
	"context"
	"sync"
	"time"
)

// NotifyRefresher is the shared "keep an in-memory view convergent with a DB
// table" driver: it runs a supplied reload on a Postgres LISTEN/NOTIFY edge
// (prompt) AND on a periodic failsafe tick (so a dropped NOTIFY still
// converges within the interval). It is the mechanism every reactive cache in
// the runtime uses to stay current across a multi-pod deployment — the caller
// owns WHAT it caches and HOW reload rebuilds it; this owns only the resilient
// refresh loop (subscribe, catch-up, coalesce, tick, reconnect-via-Listener).
//
// The contract mirrors the Listener it wraps: the NOTIFY is a pure latency
// optimisation over the failsafe interval. A window with no listener (or a lost
// NOTIFY) just means the reload falls back to the next tick — never a stuck
// view. reload must be idempotent and tolerate being called concurrently with
// nothing else (it runs on a single dedicated goroutine, serialized).
//
// Refresh guarantees, once Start returns:
//   - an IMMEDIATE reload, so a change made between the caller's boot load and
//     Start is observed at once (not after the first interval);
//   - a reload on every LISTEN (re)subscribe (onReady catch-up), closing the
//     subscribe-vs-change race and re-syncing after a listener reconnect;
//   - a reload on every NOTIFY on channel (coalesced — a burst folds into one);
//   - a reload every interval regardless, the authoritative backstop.
type NotifyRefresher struct {
	listener Listener
	channel  string
	interval time.Duration
	name     string
	// reload rebuilds the caller's view. Its error (if any) is the caller's to
	// log inside reload; the loop ignores the return and keeps ticking, so a
	// transient failure never stops future refreshes.
	reload func(ctx context.Context)

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// NewNotifyRefresher builds a refresher that calls reload on a `channel` NOTIFY
// and every `interval`. name tags the listener's reconnect log line. A
// non-positive interval falls back to a safe default so a mis-wired caller
// still converges.
func NewNotifyRefresher(listener Listener, name, channel string, interval time.Duration, reload func(ctx context.Context)) *NotifyRefresher {
	if interval <= 0 {
		interval = 60 * time.Second
	}
	return &NotifyRefresher{
		listener: listener,
		channel:  channel,
		interval: interval,
		name:     name,
		reload:   reload,
	}
}

// Start launches the LISTEN subscription + failsafe loop. Both run until Stop
// (or the parent ctx) cancels. Call once; not re-entrant.
func (r *NotifyRefresher) Start(ctx context.Context) {
	loopCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	// buffered-1 wake channel: the listener (onNotify + onReady) and the loop's
	// own immediate kick all coalesce onto it via notify().
	wakeup := make(chan struct{}, 1)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		// onReady fires on each (re)subscribe → a catch-up reload; onNotify fires
		// per NOTIFY. Both funnel to the single wakeup the loop drains.
		r.listener.Listen(loopCtx, r.name, r.channel,
			func() { notify(wakeup) },
			func() { notify(wakeup) })
	}()
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.loop(loopCtx, wakeup)
	}()
}

// Stop cancels the listener + loop and joins them, releasing the held LISTEN
// connection before the pool closes.
func (r *NotifyRefresher) Stop() {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
}

// loop is the authoritative refresher: an immediate reload, then a reload on
// every wakeup (NOTIFY / onReady catch-up) and every interval tick. A manual
// wakeup drains + resets the timer so the failsafe interval restarts from the
// last reload, not from a fixed phase.
func (r *NotifyRefresher) loop(ctx context.Context, wakeup <-chan struct{}) {
	r.reload(ctx)
	t := time.NewTimer(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-wakeup:
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
		}
		r.reload(ctx)
		t.Reset(r.interval)
	}
}
