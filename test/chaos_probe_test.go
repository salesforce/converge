package test

// chaos_probe.go — the correctness instrumentation for the chaos test: a handler
// wrap that (a) injects random transient failures + latency (like flakyWrap) AND
// (b) records how many times a given (resource, generation) was APPLIED, so the
// test can prove the fence's exactly-once-APPLY guarantee holds even when a task
// EXECUTES more than once under churn (reaper reclaim, relay, crash re-dispatch).
//
// Distinction the audit drew: under chaos a handler may RUN twice for the
// same (resource, generation) — that's allowed (at-least-once execution). What must
// NEVER happen is two APPLIED results for it. We approximate "applied" as "the
// worker returned success for this (resource,generation) and the broker's fenced
// AppendOutbox accepted it". The wrap can't see the DB write, so instead we count
// SUCCESSFUL RETURNS per (resource,generation) and separately let the test assert
// (via the DB) that synced_gen advanced exactly once. The returns counter surfaces
// the EXECUTION multiplicity (informational), and a monotonic-generation check on
// the resource proves no stale generation was applied over a newer one.

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/salesforce/converge/sdk-go/converge"
)

// execKey identifies one logical unit of work for double-apply accounting.
type execKey struct {
	kind string
	res  string
	gen  int64
}

// chaosProbe is shared across every worker instance in the fleet (all managed
// workers wrap their handlers with the SAME probe), so counts aggregate the whole
// cluster — a task claimed on broker A, relayed to a worker on broker B, then
// (after a crash) re-run on broker C all land in the same key's counter.
type chaosProbe struct {
	failRate float64
	minDelay time.Duration
	maxDelay time.Duration

	rng   *rand.Rand
	rngMu sync.Mutex

	calls    atomic.Int64 // total React invocations
	fails    atomic.Int64 // simulated transient failures
	ctxAbort atomic.Int64 // React calls aborted by ctx (partition/shutdown mid-run)

	mu       sync.Mutex
	succeeds map[execKey]int  // successful returns per (kind,res,gen) — execution multiplicity
	maxGen   map[string]int64 // highest generation ever seen per resource (staleness guard)
}

func newChaosProbe(failRate float64, minDelay, maxDelay time.Duration, seed int64) *chaosProbe {
	if maxDelay < minDelay {
		maxDelay = minDelay
	}
	return &chaosProbe{
		failRate: failRate,
		minDelay: minDelay,
		maxDelay: maxDelay,
		rng:      rand.New(rand.NewSource(seed)),
		succeeds: make(map[execKey]int),
		maxGen:   make(map[string]int64),
	}
}

// probeWrap decorates one provider, funnelling its outcomes into the shared probe.
// Multiple kinds/providers share one probe. It is itself a converge.Provider —
// Kind/OnConfig/Ready pass straight to the inner provider; it intercepts only Work.
type probeWrap struct {
	inner converge.Provider
	p     *chaosProbe
}

// compile-time proof it satisfies the one provider contract.
var _ converge.Provider = (*probeWrap)(nil)

func (w *probeWrap) Kind() converge.KindVersion           { return w.inner.Kind() }
func (w *probeWrap) OnConfig(cfg converge.ProviderConfig) { w.inner.OnConfig(cfg) }
func (w *probeWrap) Ready() bool                          { return w.inner.Ready() }

func (w *probeWrap) Work(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	p := w.p
	p.calls.Add(1)

	// track the highest generation observed for this resource; a LATER call with a
	// LOWER generation than one already applied would signal a stale-task apply.
	key := execKey{kind: string(req.Resource.Kind), res: req.Resource.ID.String(), gen: req.Resource.Generation}
	p.mu.Lock()
	if req.Resource.Generation > p.maxGen[key.res] {
		p.maxGen[key.res] = req.Resource.Generation
	}
	p.mu.Unlock()

	// random latency, ctx-aware (a partition/shutdown mid-sleep aborts cleanly).
	if d := p.pickDelay(); d > 0 {
		tm := time.NewTimer(d)
		select {
		case <-ctx.Done():
			tm.Stop()
			p.ctxAbort.Add(1)
			return converge.Outcome{}, ctx.Err()
		case <-tm.C:
		}
	}

	// random transient failure BEFORE the real provider runs (no partial side effect).
	if p.maybeFail() {
		p.fails.Add(1)
		return converge.Outcome{}, &transientChaosErr{key: key}
	}

	out, err := w.inner.Work(ctx, req)
	if err == nil {
		p.mu.Lock()
		p.succeeds[key]++
		p.mu.Unlock()
	}
	return out, err
}

// transientChaosErr is a plain (non-terminal) error → the core records a transient
// failure and re-pends after RetryAfter, so the same work retries.
type transientChaosErr struct{ key execKey }

func (e *transientChaosErr) Error() string {
	return "chaos: simulated transient failure for " + e.key.kind + "/" + e.key.res
}

func (p *chaosProbe) pickDelay() time.Duration {
	if p.maxDelay <= 0 {
		return 0
	}
	span := p.maxDelay - p.minDelay
	p.rngMu.Lock()
	defer p.rngMu.Unlock()
	d := p.minDelay
	if span > 0 {
		d += time.Duration(p.rng.Int63n(int64(span)))
	}
	return d
}

func (p *chaosProbe) maybeFail() bool {
	if p.failRate <= 0 {
		return false
	}
	p.rngMu.Lock()
	defer p.rngMu.Unlock()
	return p.rng.Float64() < p.failRate
}

// multiApplied returns the (resource,generation) keys that returned success MORE
// THAN ONCE — i.e. the handler ran to completion twice for the same logical unit.
// This is EXECUTION multiplicity (allowed by at-least-once), reported so the test
// can log how much re-execution the chaos induced. The DB-side synced_gen check is
// what proves no DOUBLE-APPLY (the fence rejects the second write).
func (p *chaosProbe) multiApplied() map[execKey]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[execKey]int)
	for k, n := range p.succeeds {
		if n > 1 {
			out[k] = n
		}
	}
	return out
}

func (p *chaosProbe) stats() (calls, fails, ctxAbort int64) {
	return p.calls.Load(), p.fails.Load(), p.ctxAbort.Load()
}

// wrapRuntime returns a provider whose Work funnels through the shared probe. All
// kinds/workers share ONE probe so double-apply accounting is fleet-wide.
func (p *chaosProbe) wrapRuntime(pr converge.Provider) converge.Provider {
	return &probeWrap{inner: pr, p: p}
}

// wrapRuntimes maps wrapRuntime over a slice.
func (p *chaosProbe) wrapRuntimes(prs []converge.Provider) []converge.Provider {
	out := make([]converge.Provider, len(prs))
	for i, pr := range prs {
		out[i] = p.wrapRuntime(pr)
	}
	return out
}
