package fault

import (
	"context"
	"time"

	"github.com/salesforce/converge/sdk-go/converge"
)

// DefaultDeleteDelay is the classic demo's simulated teardown latency: how long a
// leaf/root "delete" (finalizer) reaction pretends its external cleanup takes. It
// defaults to 5s (not 0) precisely so a `just demo` deletion is WATCHABLE — you can
// see the reverse-dependency cascade collapse the tree bottom-up in the UI, one
// layer at a time, rather than blinking out instantly. Override with FAKE_DELETE_DELAY.
const DefaultDeleteDelay = 5 * time.Second

// Teardown is the classic demo's simulated finalizer/delete handler, shared by every
// fake kind (account, vpc, tgw, route, and the classicbom root). Its whole job is to
// take a visible moment (delay) then succeed, so the engine strips the kind's
// finalizer and removes the row — after which the node's parent/dependency becomes
// unblocked and its own teardown runs. A real provider replaces the sleep with the
// actual external delete (deprovision the account, delete the VPC, …) and returns
// converge.Terminal only for a permanent, un-retryable failure.
//
// Delete tasks carry no spec/status; the handler needs none — it just simulates the
// cleanup and reports success (an empty Outcome; the reaction's Emits=finalizer tells
// the core to strip the finalizer).
type Teardown struct {
	Delay time.Duration
}

// React simulates the external cleanup, honoring cancellation so a drain/lease-loss
// doesn't leave a sleep running past the claim.
func (t Teardown) React(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	if t.Delay > 0 {
		select {
		case <-time.After(t.Delay):
		case <-ctx.Done():
			return converge.Outcome{}, ctx.Err()
		}
	}
	if req.Env != nil && req.Env.Logger != nil {
		req.Env.Logger.Info("simulated teardown complete (finalizer will be stripped)")
	}
	return converge.Outcome{}, nil
}

// DeleteDelayFromEnv resolves the teardown delay: the FAKE_DELETE_DELAY duration if
// set (and parseable), else DefaultDeleteDelay. Providers call this in Setup so the
// knob is uniform across the fake kinds.
func DeleteDelayFromEnv(raw string) time.Duration {
	if raw == "" {
		return DefaultDeleteDelay
	}
	if d, err := time.ParseDuration(raw); err == nil {
		return d
	}
	return DefaultDeleteDelay
}
