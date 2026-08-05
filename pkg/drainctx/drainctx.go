// Package drainctx provides the graceful-drain context split shared by every
// long-running Converge driver — the worker runner (sdk-go/converge) and the
// broker's work + reactor dispatchers (internal/runtime). It's a leaf package
// importing only the stdlib, so both ends use ONE implementation without a
// dependency edge between them (same rationale as pkg/wirelimits).
//
// The pattern: a component's Run loop "stops pulling new work" the instant its
// ctx is cancelled (SIGTERM), but lets ALREADY in-flight work finish for a grace
// window before abandoning it. New returns a child ctx for the in-flight work
// that outlives the parent by grace, plus a cancel to release it.
package drainctx

import (
	"context"
	"time"
)

// New returns drainCtx — a context for IN-FLIGHT work that follows parent
// normally but, once parent is cancelled, lingers an extra grace before being
// cancelled itself — and a cancel func the caller must defer.
//
// Usage: the Run loop's claim/pull path follows `parent` (stops at once on
// cancel); the in-flight handlers/goroutines run under `drainCtx` (finish for up
// to grace). grace <= 0 cancels drainCtx as soon as parent is — the no-drain
// case. The caller's `defer cancel()` releases the watcher goroutine the moment
// Run returns (drained early), so it never lingers past the loop.
func New(parent context.Context, grace time.Duration) (drainCtx context.Context, cancel context.CancelFunc) {
	// WithoutCancel so drainCtx does NOT inherit parent's cancellation — the
	// watcher below decides when it dies (parent-cancel + grace, or the caller's
	// defer when Run returns).
	drainCtx, cancel = context.WithCancel(context.WithoutCancel(parent))
	go func() {
		<-parent.Done()
		if grace <= 0 {
			cancel() // no drain: in-flight dies with the parent
			return
		}
		t := time.NewTimer(grace)
		defer t.Stop()
		select {
		case <-t.C: // grace elapsed → cancel the stragglers
		case <-drainCtx.Done(): // Run already returned and cancelled it → stop early
		}
		cancel()
	}()
	return drainCtx, cancel
}
