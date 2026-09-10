// Package fault is the classic demo's simulated-failure knob: a tiny, shared
// fault injector the leaf workers (account, networking: vpc/route/tgw) consult
// before doing their stub work. It exists to make the engine's at-least-once
// RETRY/backoff path visible in a demo — `just inject-faults` sets FAKE_FAULT_RATE
// and you watch resources go Reconciling → (transient fail) → Reconciling → Ready.
//
// The fault is TRANSIENT: a reaction returns a plain (non-Terminal) error, which
// the dispatcher records as a retryable failure and re-queues with backoff. The
// roll is INDEPENDENT per React call (random per attempt), so a faulty resource
// heals on a later retry — expected ~1/(1-rate) attempts — with no per-resource
// state to track. Rate 0 (the default, and what `just demo` uses) is a no-op, so
// the normal demo is unaffected; only `just inject-faults` turns it on.
package fault

import (
	"fmt"
	"math/rand/v2"
)

// Injector decides whether a given reaction attempt should fail. The zero value
// (Rate 0) never injects — so a worker built without the env wired is a clean
// pass-through. Rate is clamped to [0,1].
type Injector struct {
	Rate float64 // probability ANY single reaction attempt fails transiently
}

// Roll reports whether this attempt should fail, drawing an independent uniform
// sample per call. math/rand/v2's top-level source is concurrency-safe, so this
// is safe to call from the worker pool without locking. Rate<=0 short-circuits
// (no draw) so the common no-fault path costs nothing.
func (in Injector) Roll() bool {
	if in.Rate <= 0 {
		return false
	}
	if in.Rate >= 1 {
		return true
	}
	return rand.Float64() < in.Rate
}

// Fail returns the TRANSIENT error a faulted reaction should return: a plain
// error (NOT converge.Terminal), so the engine retries it with backoff. kind +
// name name the resource so the worker log shows which one is flapping.
func (in Injector) Fail(kind, name string) error {
	return fmt.Errorf("injected fault (FAKE_FAULT_RATE=%.2f): %s/%s transiently failed; will retry", in.Rate, kind, name)
}
