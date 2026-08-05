package engine

import (
	"context"
	"fmt"
	"net/http"

	"connectrpc.com/connect"

	"github.com/salesforce/converge/internal/broker"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/store"
)

// claimDuty runs a BROKER: it owns the work_queue shard tile, claims tasks, and
// fans each selected reaction out to a DUMB worker instead of running it
// locally. It IS the server side of the universal work-distribution API and the
// only provider-executing duty. It exposes the dispatchSurface hook set and
// serves the broker Connect services — main.go mounts Handler() on the broker listener
// (reusing the API server's TLS/mTLS scaffold). All provider execution is
// remote, INCLUDING reactors: the reactor spine (runtime.ReactorDispatcher) runs
// inside this duty using the broker's fanout executor, so a reactor reaction
// fans out to a connected worker over the SAME WorkStream stream (STAGE_REACT)
// as every other stage — there is no separate react duty or in-process reactor
// tier, and the core stays kind-blind.
type claimDuty struct {
	cfg       *BrokerConfig
	server    *broker.Server
	manifests *runtime.KindManifestCache
	reactor   *runtime.ReactorDispatcher // lifecycle-reactor spine, fanned out over the SAME workers
	cancel    context.CancelFunc         // cancels the dispatcher + reactor loops on Stop
	done      chan struct{}
	reactDone chan struct{}
}

var (
	_ Duty = (*claimDuty)(nil)
	// The claim duty also satisfies HandlerProvider (publishes the broker Connect services
	// route) and dispatchSurface (the cross-cutting hook set Engine forwards).
	// Engine.Start caches it via a d.(dispatchSurface) assertion; a SILENT miss (a
	// method added to the interface but not to *claimDuty) would leave
	// Engine.dispatch nil — BroadcastConfig/BroadcastBundle/AddKindLive/InFlight
	// all no-op and a worker never receives a live config/bundle push (it waits out
	// the 5-min poll, and a config/bundle-bootstrapped composer terminal-fails "no
	// bundle yet" first). These assertions turn that latent runtime break into a
	// compile error.
	_ HandlerProvider = (*claimDuty)(nil)
	_ dispatchSurface = (*claimDuty)(nil)
)

// ClaimDuty builds a claim duty from a BrokerConfig. cfg.BrokerID is the
// broker's lease identity.
func ClaimDuty(cfg *BrokerConfig) Duty { return &claimDuty{cfg: cfg} }

func (d *claimDuty) Name() string { return "claim" }

func (d *claimDuty) Start(ctx context.Context, deps Deps) error {
	if d.cfg.Pool == nil {
		d.cfg.Pool = deps.Pool
	}
	if d.cfg.Shards == nil {
		d.cfg.Shards = deps.Shards
	}
	if d.cfg.Pool == nil {
		return fmt.Errorf("claim duty: Pool is required")
	}
	if err := d.buildDispatcher(ctx, deps); err != nil {
		return err
	}

	// Launch the long-running loops under one cancellable ctx so Stop halts them
	// all: the claim dispatcher, the peer-mesh relay, the attribution batcher, and
	// the reactor delivery loop.
	loopCtx, cancel := context.WithCancel(ctx)
	d.cancel = cancel
	d.done = make(chan struct{})
	go func() {
		defer close(d.done)
		_ = d.server.Dispatcher().Run(loopCtx)
	}()
	// RELAY: keep the peer-broker set synced from cluster_members (no-op if relay
	// off). ATTRIBUTION: coalesce "which worker ran which task" into batched UI
	// updates (best-effort, off the dispatch path). Both bound to loopCtx.
	go d.server.StartRelay(loopCtx)
	go d.server.StartAttributionBatcher(loopCtx)
	d.startReactor(loopCtx)
	return nil
}

// buildDispatcher constructs the broker's work dispatcher and wires everything it
// needs BEFORE any loop launches (so there's no data race with the running
// dispatcher): the manifest cache + its live-registration callback, the OTel
// counters, per-service authz, the providerconfig reader, the peer mesh, and the
// dispatcher tuning knobs. Leaves the loops to Start.
func (d *claimDuty) buildDispatcher(ctx context.Context, deps Deps) error {
	// The manifest cache drives task-type pair selection + the engine. Load it
	// synchronously so the broker's NewDispatch sees each kind's reactions, then
	// start its live refresh (NOTIFY + failsafe).
	d.manifests = runtime.NewKindManifestCache(d.cfg.Pool)
	if err := d.manifests.Load(ctx); err != nil {
		return fmt.Errorf("claim duty: load kind manifests: %w", err)
	}
	d.server = broker.NewDispatch(d.cfg.Pool, d.manifests, d.cfg.BrokerID, d.cfg.WorkerMaxParallel, d.cfg.Shards)
	// Wire the OTel dispatch counters AT CONSTRUCTION (before the dispatcher goroutine
	// launches → no data race). No-op when metrics are off (nil counters).
	d.server.SetMetrics(d.cfg.Claimed, d.cfg.Completed)
	// Per-service SPIFFE-ID authz: WorkerAuthz gates the worker-facing WorkStream/
	// GetProviderConfig, MeshAuthz gates the peer-broker Route. Set
	// BEFORE Handler() builds the Connect services so the interceptors are installed.
	// nil matchers = no allowlist for that service (chain trust only).
	d.server.SetAuthz(d.cfg.WorkerAuthz, d.cfg.MeshAuthz, d.cfg.PeerIdentitySource)
	// LIVE CRD REGISTRATION: a CRD is almost always applied AFTER the broker
	// boots (the fleet starts, then the operator applies CRDs), so NewDispatch
	// above registers pairs only for kinds already applied at boot. Wire the
	// manifest cache's reload callback to register every kind's claim pairs the
	// moment its manifest lands — the dispatcher dedups, so re-firing on every
	// reload is safe. Without this the broker sits at "no pairs" and never claims
	// a kind applied post-boot. A broker always claims EVERY manifested kind (no
	// subset filter). Set BEFORE Start so the first reload (and any later NOTIFY)
	// flows through it.
	d.manifests.SetOnReplace(func(ms []model.KindManifest) {
		for _, m := range ms {
			d.server.AddKindLive(m)
		}
	})
	d.manifests.Start(ctx)
	// Serve each kind's live DEFAULT providerconfig to dumb workers (GetProviderConfig).
	d.server.SetConfigReader(deps.Configs)
	// MESH (NATS-style push): this broker discovers peers from cluster_members, holds
	// a persistent Route stream to each, advertises its live worker CREDIT, and pushes
	// a claimed task it has no local worker for to a credited peer — decoupling
	// placement from consumption. Enabled whenever a peer transport is present
	// (always, for a broker); a no-op with zero peers (single-broker fleet).
	if d.cfg.RelayHTTPClient != nil {
		// The mesh reconciles peer routes on a cluster_changed NOTIFY (prompt
		// join/leave) with its own failsafe poll behind it, reading the LIVENESS-
		// FILTERED member view (MeshLivenessWindow) — the SAME liveness window the
		// Resharder uses, so a crashed peer drops out of the mesh peer set within one
		// window (not the ClusterMemberGC TTL), its reaction being dial/drop peer routes.
		liveness := d.cfg.MeshLivenessWindow
		if liveness <= 0 {
			liveness = runtime.DefaultMeshLivenessWindow
		}
		d.server.EnableRelay(d.cfg.BrokerID, d.cfg.RelayHTTPClient, store.New(d.cfg.Pool), runtime.NewPgxListener(d.cfg.Pool), 0, liveness)
	}
	disp := d.server.Dispatcher()
	if d.cfg.HeartbeatEvery > 0 {
		disp.HeartbeatEvery = d.cfg.HeartbeatEvery
	}
	if d.cfg.WorkerPollMin > 0 {
		disp.PollMin = d.cfg.WorkerPollMin
	}
	if d.cfg.WorkerPollMax > 0 {
		disp.PollMax = d.cfg.WorkerPollMax
	}
	// GRACEFUL DRAIN: on Stop (ctx cancel) the dispatcher stops claiming but lets
	// in-flight tasks — and the parked dispatchStage awaiting a worker's Complete —
	// finish for DrainGrace so fanned-out work lands its result instead of being
	// abandoned + re-dispatched. Owned by the component (one ctx in/out); the host
	// just sets the budget and cancels normally.
	disp.DrainGrace = d.cfg.DrainGrace
	return nil
}

// startReactor launches the unified reactor delivery loop: the broker also drains
// lifecycle_outbox and fans each matched transition out to a connected worker over
// the SAME WorkStream (STAGE_REACT), via the fanout executor. The worker
// advertising the reactor kind runs the handler; the dispatcher acks the outbox
// only after a successful Complete (at-least-once preserved — a failed/undelivered
// react leaves the row claimed for reap_stale_lifecycle). Bound to loopCtx.
func (d *claimDuty) startReactor(loopCtx context.Context) {
	d.reactor = runtime.NewReactorDispatcher(store.New(d.cfg.Pool), runtime.NewPgxListener(d.cfg.Pool), d.server.StageDispatcher(), d.cfg.BrokerID)
	d.reactor.ReadyToDispatch = d.server.HasWorkerForKind // skip (re-arm) a reactor kind with no connected worker
	d.reactor.DrainGrace = d.cfg.DrainGrace               // same graceful in-flight drain as the work dispatcher
	if d.cfg.Shards != nil {
		d.reactor.Shards = d.cfg.Shards
	}
	d.reactDone = make(chan struct{})
	go func() {
		defer close(d.reactDone)
		_ = d.reactor.Run(loopCtx)
	}()
}

// Handler returns the broker Connect service route to mount on the broker's listener.
// nil before Start.
func (d *claimDuty) Handler(opts ...connect.HandlerOption) (string, http.Handler, bool) {
	if d.server == nil {
		return "", nil, false
	}
	path, h := d.server.Handler(opts...)
	return path, h, true
}

// AddKindLive forwards a late-applied kind's manifest to the running broker so
// its task-type pairs are registered without a restart.
func (d *claimDuty) AddKindLive(m model.KindManifest) {
	if d.server != nil {
		d.server.AddKindLive(m)
	}
}

// BroadcastConfig pushes a (kind, kindVersion)'s new default providerconfig — the whole
// monolith, spec + data/bundle — to every connected worker serving that exact
// (kind, kindVersion) over its WorkStream. No-op before Start.
func (d *claimDuty) BroadcastConfig(kind model.Kind, kindVersion int, spec, data []byte) {
	if d.server != nil {
		d.server.BroadcastConfig(kind, kindVersion, spec, data)
	}
}

// InFlight returns the broker's live in-flight (claimed, fanning-out) count.
func (d *claimDuty) InFlight() int {
	if d.server == nil {
		return 0
	}
	return d.server.Dispatcher().InFlight()
}

// WorkerKindsSnapshot returns the broker's LIVE connected-worker kind set (for
// the relay reporter's cluster_members advertisement). Empty before Start.
func (d *claimDuty) WorkerKindsSnapshot() []model.Kind {
	if d.server == nil {
		return nil
	}
	return d.server.WorkerKindsSnapshot()
}

// ConnectedWorkers snapshots the workers connected to this broker for the
// cluster view (one row per WorkStream stream). Empty before Start.
func (d *claimDuty) ConnectedWorkers() []broker.ConnectedWorker {
	if d.server == nil {
		return nil
	}
	return d.server.ConnectedWorkers()
}

// ConnectedWorkerCount is the live worker-stream count (a metrics gauge source).
// 0 before Start / on a broker with no workers.
func (d *claimDuty) ConnectedWorkerCount() int {
	if d.server == nil {
		return 0
	}
	return d.server.ConnectedWorkerCount()
}

// MeshCounters returns the cumulative broker-mesh fast-recovery counts (metrics).
// All zero before Start / when the relay mesh is off.
func (d *claimDuty) MeshCounters() (peerGoneWakes, foreignDropSignals, syntheticDelivered int64) {
	if d.server == nil {
		return 0, 0, 0
	}
	return d.server.MeshCounters()
}

// Stop cancels the dispatcher + reactor loops and waits for them to drain. The
// GRACEFUL DRAIN is internal to each loop (DrainGrace): cancel makes them stop
// CLAIMING immediately, but already-claimed tasks and the parked dispatchStage
// awaiting a worker's Complete finish for up to DrainGrace so fanned-out work
// lands its result instead of being abandoned + re-dispatched. The wait here is
// bounded by ctx (cmd/converge's shutdown budget, below the k8s grace) so a
// wedged worker can't pin shutdown past SIGKILL — a straggler then falls to the
// reaper.
func (d *claimDuty) Stop(ctx context.Context) error {
	if d.cancel != nil {
		d.cancel()
	}
	if d.done != nil {
		select {
		case <-d.done:
		case <-ctx.Done():
		}
	}
	if d.reactDone != nil {
		select {
		case <-d.reactDone:
		case <-ctx.Done():
		}
	}
	if d.manifests != nil {
		d.manifests.Stop()
	}
	return nil
}

// ReleaseClaims frees every work_queue row the broker holds (shutdown fast
// handoff). No-op before Start.
func (d *claimDuty) ReleaseClaims(ctx context.Context) (int64, error) {
	if d.server == nil {
		return 0, nil
	}
	return d.server.Dispatcher().ReleaseClaims(ctx)
}
