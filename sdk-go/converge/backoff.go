package converge

import (
	"math/rand"
	"time"
)

// backoff.go holds the SDK's reconnect/config-load retry timing. The one rule it
// encodes is the anti-thundering-herd policy: a whole worker fleet loses a broker at
// once, so an UN-jittered backoff reconnects them in lockstep — a synchronized herd
// that just re-storms the recovered listener each tick. jitter decorrelates the herd;
// the ESCALATION (the doubling toward the ceiling) stays on the raw, un-jittered value
// so the growth is deterministic. Used only by run.go's reconnect + boot config-load
// loops, so it lives here in the SDK rather than a shared pkg.

// jitter returns d perturbed by ±25% so reconnect/config-load sleeps across many
// workers decorrelate instead of firing in lockstep. d <= 0 is returned unchanged.
// Callers escalate on the RAW d (jitter only shapes the sleep), so the sequence still
// climbs deterministically toward its ceiling.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	span := int64(d) / 2 // the full ±25% window is d/2 wide
	return d - time.Duration(span/2) + time.Duration(rand.Int63n(span+1))
}
