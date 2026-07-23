// Package engine assembles the process from composable DUTIES over one shared
// Postgres.
//
// A pod is not a ROLE; it is a SET OF DUTIES. Each Duty is one unit of work the
// process performs — the broker, the outbox drainer, the reaper, the spec
// GC, the resync sweeper, the cluster-member GC, the reactor dispatcher. main.go
// resolves the configured role (or an explicit DUTIES= list) into a []Duty via
// dutiesFromConfig and hands it to one Engine, which Start/Stops them through a
// single uniform loop. "What does this pod do?" is read off its duty list.
//
// One duty — claim — drives provider execution, fanning each task's selected
// reaction out to dumb workers. Every other duty is kind-agnostic plumbing
// that operates on work_queue / work_outbox / resources rows directly and reads
// any per-kind operational settings it needs (cap, resync) from the kind_config
// DB table — never a constructed provider. converge carries no provider code at
// all: the broker fans each claimed task out to a dumb Connect worker that holds the
// handlers, so a converge pod dials no downstream. The duties
// coordinate entirely through the database, never via in-process state; the real
// runtime atoms are the drivers they wrap (Dispatcher, Drainer, Reaper,
// Resyncer); see internal/runtime/doc.go.
//
// Control-side duties are grouped behind one ControlPlane bundle (it runs
// migrations + verifies the shard modulus once, then starts drain + reap +
// specgc + resync + membergc together) because every deployment runs those
// sweepers as a unit on a control pod; the bundle is an internal construction
// detail, not a tier — the Engine sees it as one controlDuty among the list.
//
// Eligibility scheduling (deciding which pending rows are ready to dispatch and
// inserting work_queue rows) is not a Go duty — it lives in Postgres triggers +
// the schedule_eligible() function (a DB-hosted reactor). The
// cascade_on_ready_change AFTER-STATEMENT trigger fires reactively when a row's
// synced_gen catches up to generation; value-flow re-substitution happens inside
// drain_outbox_batch. The reaper (requeue_failed_and_pending) is the backstop
// for rows the cascade misses.
package engine

import (
	"context"
	"net/http"

	"connectrpc.com/connect"

	"github.com/salesforce/converge/internal/broker"
	"github.com/salesforce/converge/internal/model"
)

// Engine is the process's run unit: the ordered SET OF DUTIES this pod performs,
// plus the shared dependencies they run against. main.go builds one duty list
// (dutiesFromConfig) and hands it to one Engine; Start and Stop are a single
// uniform loop over that list.
//
// The Engine owns NO policy of its own — every behavior lives in a Duty. It only
// folds the shared Deps onto each duty at Start and joins them at Stop, and
// surfaces the cross-cutting hooks main.go needs from the dispatch duty:
// AddKindLive (for the ProviderRetrier) and InFlight (for the cluster-view
// reporter).
type Engine struct {
	duties []Duty
	deps   Deps

	// dispatch is the pod's single provider-executing duty (the claimDuty, which
	// implements dispatchSurface), cached at Start so AddKindLive / InFlight /
	// ReleaseClaims don't re-scan the list. nil on a control/sweeper-only pod —
	// the hooks then no-op / return 0.
	dispatch dispatchSurface
}

// The dispatch surface splits into three cohesive sub-roles a consumer can
// depend on individually. dispatchSurface composes them for the one duty that
// provides all three (the claimDuty).

// dispatchLifecycle is the claim duty's run-time control: teach it a live kind,
// read its in-flight count, release its claims on shutdown.
type dispatchLifecycle interface {
	AddKindLive(m model.KindManifest)
	InFlight() int
	ReleaseClaims(ctx context.Context) (int64, error)
}

// dispatchConfigPush pushes a (kind, kindVersion)'s new default providerconfig — the
// whole monolith, spec + data/bundle — to every connected worker serving that exact
// pair over its WorkStream.
type dispatchConfigPush interface {
	BroadcastConfig(kind model.Kind, kindVersion int, spec, data []byte)
}

// dispatchClusterView exposes the broker's live worker presence + mesh counters
// for the cluster view and metrics gauges.
type dispatchClusterView interface {
	// WorkerKindsSnapshot returns the LIVE set of kinds with a connected worker on
	// this broker — the relay reporter advertises it so peers' cluster-aware claim
	// gate learns this broker's worker presence. Empty on a broker with no workers.
	WorkerKindsSnapshot() []model.Kind
	// ConnectedWorkers snapshots the workers connected to this broker (one row per
	// WorkStream stream: friendly id, kinds, live/ceiling slots) for the
	// connected-worker cluster view. Empty on a broker with no workers.
	ConnectedWorkers() []broker.ConnectedWorker
	// ConnectedWorkerCount is the live worker-stream count (metrics gauge source).
	ConnectedWorkerCount() int
	// MeshCounters returns the cumulative broker-mesh fast-recovery counts (metrics).
	MeshCounters() (peerGoneWakes, foreignDropSignals, syntheticDelivered int64)
}

// dispatchSurface is the full cross-cutting hook set the Engine forwards from the
// duty that executes the dispatch surface (the claimDuty). It composes the three
// sub-roles; the Engine caches a value of this type at Start and each Engine
// method forwards to the sub-role it needs.
type dispatchSurface interface {
	dispatchLifecycle
	dispatchConfigPush
	dispatchClusterView
}

// NewEngine builds the run unit from a duty list and the shared dependencies
// every duty draws from (the pool, the read pool, the Setup'd registry, the
// pod's shard range). The duties are typically produced by dutiesFromConfig.
func NewEngine(duties []Duty, deps Deps) *Engine {
	return &Engine{duties: duties, deps: deps}
}

// Start launches every duty against the shared Deps, in list order. It stops at
// the first duty that fails to start and returns its error (the caller aborts
// boot); duties already started keep running until the caller cancels the ctx
// or calls Stop. Each duty's Start is non-blocking (it spawns goroutines and
// returns), so this returns once the whole pod is running.
func (e *Engine) Start(ctx context.Context) error {
	for _, d := range e.duties {
		if ds, ok := d.(dispatchSurface); ok {
			e.dispatch = ds
		}
		if err := d.Start(ctx, e.deps); err != nil {
			return err
		}
	}
	return nil
}

// Stop joins every duty in REVERSE start order (so the dispatch duty, usually
// last, drains before the control sweepers it fed stop). Best-effort: it stops
// them all even if one errors, returning the first error seen.
func (e *Engine) Stop(ctx context.Context) error {
	var firstErr error
	for i := len(e.duties) - 1; i >= 0; i-- {
		if err := e.duties[i].Stop(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// AddKindLive forwards a late-applied kind's manifest to the dispatch duty so it
// starts claiming with no restart. No-op on a pod with no dispatch duty. The
// ProviderRetrier / a manifest apply calls this.
func (e *Engine) AddKindLive(m model.KindManifest) {
	if e.dispatch != nil {
		e.dispatch.AddKindLive(m)
	}
}

// BroadcastConfig pushes a (kind, kindVersion)'s new default providerconfig — the whole
// monolith, spec + data/bundle — to every connected worker serving that exact
// (kind, kindVersion) (via the claim duty's broker). No-op on a control-only pod (no
// broker to broadcast through). main wires the ProviderConfigCache's OnKindConfigChange here.
func (e *Engine) BroadcastConfig(kind model.Kind, kindVersion int, spec, data []byte) {
	if e.dispatch != nil {
		e.dispatch.BroadcastConfig(kind, kindVersion, spec, data)
	}
}

// InFlight returns the pod's live in-flight task count for the cluster-view
// reporter, or 0 on a pod with no dispatch duty.
func (e *Engine) InFlight() int {
	if e.dispatch == nil {
		return 0
	}
	return e.dispatch.InFlight()
}

// WorkerKindsSnapshot returns the pod's LIVE connected-worker kind set, or nil on
// a pod with no dispatch duty.
func (e *Engine) WorkerKindsSnapshot() []model.Kind {
	if e.dispatch == nil {
		return nil
	}
	return e.dispatch.WorkerKindsSnapshot()
}

// ConnectedWorkers snapshots the workers connected to this pod's broker for the
// connected-worker cluster view (one row per WorkStream stream). nil on a pod with
// no dispatch duty. main wires it to the reporter's Workers hook so the
// cluster_members.workers column tracks streams connecting/dropping.
func (e *Engine) ConnectedWorkers() []broker.ConnectedWorker {
	if e.dispatch == nil {
		return nil
	}
	return e.dispatch.ConnectedWorkers()
}

// RunsBroker reports whether this pod runs the broker (dispatch) duty — the gate
// for registering broker metrics.
func (e *Engine) RunsBroker() bool { return e.dispatch != nil }

// ConnectedWorkerCount is the live worker-stream count (metrics gauge source); 0 on
// a pod with no dispatch duty.
func (e *Engine) ConnectedWorkerCount() int {
	if e.dispatch == nil {
		return 0
	}
	return e.dispatch.ConnectedWorkerCount()
}

// MeshCounters returns the cumulative broker-mesh fast-recovery counts (metrics);
// zeros on a pod with no dispatch duty.
func (e *Engine) MeshCounters() (peerGoneWakes, foreignDropSignals, syntheticDelivered int64) {
	if e.dispatch == nil {
		return 0, 0, 0
	}
	return e.dispatch.MeshCounters()
}

// ReleaseClaims frees every work_queue row this pod's dispatcher holds, so a
// surviving pod re-claims the work immediately on graceful shutdown instead of
// waiting out the reaper's stale window. Call AFTER Stop (the dispatcher is
// then quiesced). Returns the count freed; 0 on a pod with no dispatch duty.
func (e *Engine) ReleaseClaims(ctx context.Context) (int64, error) {
	if e.dispatch == nil {
		return 0, nil
	}
	return e.dispatch.ReleaseClaims(ctx)
}

// ClaimHandler returns the broker Connect service route to mount on the broker's
// listener, or ok=false when this pod runs no claim duty. Call AFTER Start (the
// claim duty builds its server in Start). main.go mounts it on the broker
// listener, reusing the TLS/mTLS scaffold.
func (e *Engine) ClaimHandler(opts ...connect.HandlerOption) (path string, h http.Handler, ok bool) {
	// Range for the first duty that PUBLISHES a route (HandlerProvider), rather
	// than type-asserting a concrete *claimDuty — a new route-publishing duty
	// needs no edit here, it just implements HandlerProvider. Today only the claim
	// duty does.
	for _, d := range e.duties {
		if hp, isProvider := d.(HandlerProvider); isProvider {
			if p, handler, ok := hp.Handler(opts...); ok {
				return p, handler, true
			}
		}
	}
	return "", nil, false
}
