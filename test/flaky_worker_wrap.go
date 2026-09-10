package test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/salesforce/converge/sdk-go/converge"
)

// flakyWrap decorates an existing converge.Provider with two test-only behaviors,
// applied in its Work before delegating to the inner provider:
//
//   - Random per-call latency in [MinDelay, MaxDelay] before delegating to the inner
//     Work. Models a slow provider API.
//
//   - Random pre-call failures at FailRate. Failure short-circuits Work before the
//     wrapped provider runs — no provider call is made, the core records the failure,
//     the reaper re-pends after RetryAfter, and on the next claim the same wrap gets
//     another chance to succeed.
//
// It is itself a converge.Provider (same Kind/OnConfig/Ready as the inner), so a test
// registers it in place of the real provider. Counts are atomic; tests assert against
// them at the end.
type flakyWrap struct {
	inner    converge.Provider
	minDelay time.Duration
	maxDelay time.Duration
	failRate float64 // 0.0 = never fail, 1.0 = always fail

	rng   *rand.Rand
	rngMu sync.Mutex
	calls atomic.Int64
	fails atomic.Int64
}

var _ converge.Provider = (*flakyWrap)(nil)

// newFlakyWrap wraps an existing Provider. Pass min/max equal to disable random delay;
// pass failRate=0 to disable random failure.
func newFlakyWrap(inner converge.Provider, minDelay, maxDelay time.Duration, failRate float64, seed int64) *flakyWrap {
	if maxDelay < minDelay {
		maxDelay = minDelay
	}
	return &flakyWrap{
		inner:    inner,
		minDelay: minDelay,
		maxDelay: maxDelay,
		failRate: failRate,
		rng:      rand.New(rand.NewSource(seed)),
	}
}

// Kind, OnConfig, and Ready pass straight through to the inner provider — the wrap only
// intercepts Work.
func (w *flakyWrap) Kind() converge.KindVersion           { return w.inner.Kind() }
func (w *flakyWrap) OnConfig(cfg converge.ProviderConfig) { w.inner.OnConfig(cfg) }
func (w *flakyWrap) Ready() bool                          { return w.inner.Ready() }

func (w *flakyWrap) Work(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	w.calls.Add(1)
	w.sleep(ctx)
	if w.maybeFail() {
		w.fails.Add(1)
		return converge.Outcome{}, fmt.Errorf("flaky-wrap: simulated transient failure for %s/%s", req.Resource.Kind, req.Resource.ID)
	}
	return w.inner.Work(ctx, req)
}

// sleep picks a random delay in [minDelay, maxDelay] and waits, but
// returns early if ctx is cancelled (test shutdown).
func (w *flakyWrap) sleep(ctx context.Context) {
	if w.maxDelay <= 0 {
		return
	}
	span := w.maxDelay - w.minDelay
	w.rngMu.Lock()
	d := w.minDelay
	if span > 0 {
		d += time.Duration(w.rng.Int63n(int64(span)))
	}
	w.rngMu.Unlock()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func (w *flakyWrap) maybeFail() bool {
	if w.failRate <= 0 {
		return false
	}
	w.rngMu.Lock()
	defer w.rngMu.Unlock()
	return w.rng.Float64() < w.failRate
}

// Calls returns the number of Work invocations (succeeded + failed).
func (w *flakyWrap) Calls() int64 { return w.calls.Load() }

// Fails returns the number of Work invocations that returned a
// flaky failure (does not include errors raised by the inner provider).
func (w *flakyWrap) Fails() int64 { return w.fails.Load() }

// flakeRuntime wraps a provider in a flakyWrap so its Work gains the test-only
// latency/failure behavior. It returns the wrapping provider plus a one-element wraps
// slice (mirroring the old per-runtime shape) so tests can read the per-provider
// counters.
func flakeRuntime(p converge.Provider, minDelay, maxDelay time.Duration, failRate float64, seed int64) (converge.Provider, []*flakyWrap) {
	w := newFlakyWrap(p, minDelay, maxDelay, failRate, seed)
	return w, []*flakyWrap{w}
}

// sumFlaky aggregates Calls() and Fails() across a slice of wraps —
// helps tests log a single per-provider total instead of per-handler.
func sumFlaky(wraps []*flakyWrap) (calls, fails int64) {
	for _, w := range wraps {
		calls += w.Calls()
		fails += w.Fails()
	}
	return
}
