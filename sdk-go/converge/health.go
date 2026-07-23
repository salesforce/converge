package converge

import (
	"context"
	"time"
)

// initialUnready evaluates every registered Ready probe and returns the (kind,
// kindVersion) pairs currently NOT ready. runner.Run sends an RS- for each in the same
// ordered burst as the Subscribe, so a kind whose probe is false at connect is gated
// BEFORE the broker can dispatch it (no subscribe→first-poll window). A kind with no probe
// is never here (always ready).
func (w *worker) initialUnready() []KindVersion {
	probes := w.healthSnapshot()
	var out []KindVersion
	for kv, fn := range probes {
		if !fn() {
			out = append(out, kv) // kv is already a KindVersion
		}
	}
	return out
}

// runHealthPoller polls each registered readiness probe every pollEvery and drives the
// broker-facing RS+/RS- via runner.SendReadiness. It owns the EDGE detection: a probe
// is remembered as its last-observed bool, and only a CHANGE emits a frame — a
// true→false edge sends RS- (has_worker=false: stop sending this pair's work), a
// false→true edge sends RS+.
//
// Two subtleties keep it correct across reconnects:
//   - CONNECT-TIME STATE rides the Subscribe (the runner's InitialUnready), not this
//     poller: the currently-unready pairs are advertised RS- in the subscribe burst,
//     before any dispatch. The poller SEEDS its last-seen map from that same state
//     (without re-sending) so it only emits on a subsequent EDGE — a recovery RS+ or a
//     later degradation. Since the poller is created fresh per connect (run is one
//     session), the seed re-establishes correctly after every reconnect.
//   - The poller is bound to the Run ctx (not the drain ctx): it STOPS the moment
//     shutdown begins. A draining worker is going away entirely, so the stream teardown
//     is the RS- for every kind — emitting per-kind health flaps during drain would be
//     noise.
//
// A pair with no probe is absent from `probes` and thus never gated — always ready,
// the unchanged default.
func runHealthPoller(ctx context.Context, rnr *runner, probes map[KindVersion]func() bool, pollEvery time.Duration) {
	if len(probes) == 0 {
		return // nothing to poll: every kind is always ready
	}
	if pollEvery <= 0 {
		pollEvery = defaultHealthPollInterval
	}

	// last-observed readiness per pair. A pair is recorded here ONLY once its current
	// state has been DELIVERED to the broker (SendReadiness enqueued it) — a pair absent
	// from `last` is treated as "the broker's default, ready", so an undelivered flip
	// stays an edge and re-attempts on the next tick. This is what makes the poller
	// robust to a dropped frame: it doesn't latch a state the broker never learned.
	last := make(map[KindVersion]bool, len(probes))

	// emit sends the pair's current readiness and records it as delivered only on a
	// successful enqueue; a ready pair with no prior tracked state is left implicit.
	emit := func(kv KindVersion, ready bool) {
		prev, tracked := last[kv]
		if ready && !tracked {
			return // never been unready and not yet tracked → ready is the implicit default; no frame
		}
		if tracked && ready == prev {
			return // no change from the last DELIVERED state
		}
		if rnr.SendReadiness(kv.Kind, kv.Version, ready) {
			last[kv] = ready // latch only what the broker actually learned
		}
	}

	// SEED (don't emit) the connect-time state: runner.Run already advertised the
	// currently-unready pairs via InitialUnready in the Subscribe burst (closing the
	// subscribe→first-poll window). Recording them here as delivered means the poller
	// only sends on a subsequent EDGE (recovery RS+, or a later degradation), never a
	// duplicate of the initial RS-.
	for kv, fn := range probes {
		if !fn() {
			last[kv] = false
		}
	}

	t := time.NewTicker(pollEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for kv, fn := range probes {
				emit(kv, fn())
			}
		}
	}
}
