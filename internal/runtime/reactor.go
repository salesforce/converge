package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/pkg/drainctx"
)

// lifecycleReadyChannel is the LISTEN/NOTIFY channel the DB fires (DIRECT
// pg_notify, never notify_gated — the work_ready scar) when a lifecycle
// transition is written: from cascade_on_ready_change for 'synced'/'degraded'/
// 'failed', and from the apply tx for 'created'. Empty payload — the wake just
// tells the dispatcher to re-claim.
const lifecycleReadyChannel = "lifecycle_ready"

// ReactorDispatcher drains lifecycle_outbox: it claims due transitions that
// match an enabled binding, runs the kind's lifecycle reaction through the
// shared model.StageDispatcher (the SAME seam the main engine uses), and
// acks (deletes) the row on success. It is a structural clone of the Drainer —
// listen('lifecycle_ready') + pollLoop failsafe — so it inherits the same
// wake/backstop characteristics:
//
//   - reactive: a committed transition fires lifecycle_ready; the dispatcher
//     wakes and claims within a window.
//   - durable: lifecycle_outbox is LOGGED, so a dropped NOTIFY costs at most one
//     IdleInterval (the poll backstop), never the reaper's StaleAfter.
//   - at-least-once: a delivery that errors is NOT acked; its claimed row's
//     heartbeat goes stale and reap_stale_lifecycle (on the Reaper tick) frees
//     it for another attempt.
//
// Sharded with NO leader: every react pod runs one, claiming its shard range
// (shard_id BETWEEN lo AND hi). A resource's transition lands on the same
// pod-range that drains its work (same shard_of hash), so there's no cross-pod
// handoff; FOR UPDATE SKIP LOCKED makes a membership-overlap window safe.
type ReactorDispatcher struct {
	Repo     ReactorRepo
	Listener Listener // lifecycle_ready wake subscription
	// Dispatch is the seam that runs a kind's lifecycle reaction. In production a
	// broker installs its remote executor that ships the reaction to a worker; the
	// integration test harness injects an in-process executor (test/internal/inproc)
	// that runs handlers directly from a ReactionRegistry. Nil falls back to the
	// fail-closed noHandlerExecutor via dispatcher() — a delivery then fails
	// transiently and is retried, never lost.
	Dispatch model.StageDispatcher
	BrokerID string

	Interval     time.Duration
	IdleInterval time.Duration
	HotWindow    time.Duration
	// DrainGrace is the graceful in-flight drain window on shutdown: when Run's ctx
	// is cancelled, claiming stops but in-flight deliveries finish for up to this
	// long before cancellation. 0 → cancel in-flight at once with ctx.
	DrainGrace time.Duration
	BatchSize  int

	// MaxParallel bounds concurrent reactor invocations per tick — an external
	// upload/POST holds a goroutine, so cap the blast radius (the high-risk
	// outbound path). 0 → a sensible default.
	MaxParallel int

	// HeartbeatEvery refreshes this pod's in-flight claims so a slow reactor
	// (a long upload/POST) isn't reaped mid-delivery and double-fired. Pairs with
	// the Reaper's short LifecycleStaleAfter: the stale window must exceed this
	// cadence. 0 → a sensible default.
	HeartbeatEvery time.Duration

	// Shards is the pod's CURRENT claim range, read lock-free each tick via
	// Snapshot(); the Resharder swaps it on membership change.
	Shards *ShardSet

	// ReadyToDispatch, when set, reports whether a reactor KIND can be delivered
	// right now — on a broker that fans deliveries out to dumb workers, this is
	// the fanout's HasSubscriber, so the dispatcher skips a delivery whose reactor
	// kind has NO connected worker instead of parking it (which would block the
	// per-delivery goroutine until one connects). A skipped delivery is left
	// claimed; its heartbeat goes stale and the reaper re-arms it once a worker is
	// up (at-least-once). nil → always ready (the in-process executor case, where a
	// missing handler surfaces as a Terminal error instead).
	ReadyToDispatch func(kind model.Kind) bool

	// inflight is the set of deliveries this pod currently holds (claimed, reactor
	// running, not yet acked), keyed by identity and carrying the claim_epoch each was
	// claimed under. The heartbeat refreshes EXACTLY these (key, claim_epoch) pairs —
	// NOT a bare `broker_id = me` predicate. That epoch pin is what makes the heartbeat
	// deadlock-safe: a row a concurrent claim/reap re-issued has a BUMPED epoch, so it no
	// longer matches this set and the heartbeat skips it — it never chases the row's
	// updated tuple version and so can't form a 40P01 cycle with the re-claimer (the same
	// reason WorkQueueHeartbeat pins (id, claim_epoch)). An empty set → the heartbeat
	// makes ZERO DB writes (an idle react pod is silent, mirroring the work dispatcher).
	mu       sync.Mutex
	inflight map[inflightKey]inflightDelivery
}

// inflightKey identifies a claimed lifecycle_outbox row uniquely (the PK minus shard_id,
// which is a function of resource_id — unique within its partition). It is the map key
// for the in-flight set, so two bindings on the same (resource, transition, generation)
// are tracked distinctly.
type inflightKey struct {
	resourceID  uuid.UUID
	transition  string
	generation  int64
	bindingName string
}

// inflightDelivery is one held delivery's heartbeat identity: its key plus the
// claim_epoch it was claimed under — everything HeartbeatReactorClaims needs to refresh
// EXACTLY this row (epoch-fenced). The shard is NOT carried: the heartbeat joins on the
// full key (which uniquely identifies the row) and prunes partitions by the pod's shard
// RANGE, so a per-row shard would be redundant.
type inflightDelivery struct {
	inflightKey
	claimEpoch int64
}

// trackInflight adds a claimed delivery to the in-flight set before it is dispatched, so
// the heartbeat keeps its lease fresh while the reaction runs.
func (d *ReactorDispatcher) trackInflight(del store.ReactorDelivery) {
	k := inflightKey{del.ResourceID, del.Transition, del.Generation, del.BindingName}
	d.mu.Lock()
	if d.inflight == nil {
		d.inflight = make(map[inflightKey]inflightDelivery)
	}
	d.inflight[k] = inflightDelivery{inflightKey: k, claimEpoch: del.ClaimEpoch}
	d.mu.Unlock()
}

// untrackInflight removes a delivery once its reaction finished (acked, failed, or
// skipped) — its lease no longer needs refreshing.
func (d *ReactorDispatcher) untrackInflight(del store.ReactorDelivery) {
	d.mu.Lock()
	delete(d.inflight, inflightKey{del.ResourceID, del.Transition, del.Generation, del.BindingName})
	d.mu.Unlock()
}

// inflightSnapshot returns the (key, epoch) tuples to heartbeat this tick, in the shape
// HeartbeatReactorClaims consumes. Empty when the pod holds nothing → the heartbeat loop
// skips its DB write entirely.
func (d *ReactorDispatcher) inflightSnapshot() []store.ReactorLeaseKey {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.inflight) == 0 {
		return nil
	}
	out := make([]store.ReactorLeaseKey, 0, len(d.inflight))
	for _, v := range d.inflight {
		out = append(out, store.ReactorLeaseKey{
			ResourceID:  v.resourceID,
			Transition:  v.transition,
			Generation:  v.generation,
			BindingName: v.bindingName,
			ClaimEpoch:  v.claimEpoch,
		})
	}
	return out
}

// NewReactorDispatcher returns a dispatcher with the Drainer's tuned cadence:
// 25ms hot poll, 5s hot window, 2s cold failsafe. BatchSize is small because a
// delivery does external I/O (unlike a cheap DB drain), so we keep the per-tick
// claim's lock-hold short and let the work-conserving re-poll burst a backlog.
func NewReactorDispatcher(repo ReactorRepo, listener Listener, dispatcher model.StageDispatcher, brokerID string) *ReactorDispatcher {
	if brokerID == "" {
		// Stable per-pod claim identity, same format as the work dispatcher's, so
		// reap_stale_lifecycle can attribute a dead pod's claims.
		hn, _ := os.Hostname()
		if hn == "" {
			hn = "unknown"
		}
		brokerID = fmt.Sprintf("react-%s-%d-%s", hn, os.Getpid(), uuid.New().String()[:8])
	}
	return &ReactorDispatcher{
		Repo:           repo,
		Listener:       listener,
		Dispatch:       dispatcher,
		BrokerID:       brokerID,
		Interval:       25 * time.Millisecond,
		IdleInterval:   2 * time.Second,
		HotWindow:      5 * time.Second,
		BatchSize:      50,
		MaxParallel:    16,
		HeartbeatEvery: 10 * time.Second,
		Shards:         NewShardSet(AllShards()),
	}
}

// dispatcher returns the reaction executor, defaulting to the fail-closed
// noHandlerExecutor when unset. The default returns a non-terminal "no executor
// wired" error → the delivery is left claimed and the reaper re-arms it
// (at-least-once), never a silent drop. Production installs the broker fanout via
// SetDispatcher; the test harness injects an in-process executor.
func (d *ReactorDispatcher) dispatcher() model.StageDispatcher {
	if d.Dispatch != nil {
		return d.Dispatch
	}
	return noHandlerExecutor{}
}

// Run drives the reactor until ctx is cancelled.
//
// GRACEFUL DRAIN (mirrors the work dispatcher): when ctx is cancelled the poll
// loop / listener / heartbeat stop claiming new lifecycle deliveries, but
// in-flight deliveries (a worker running the lifecycle reaction, awaiting its
// Complete) finish for up to DrainGrace before being cancelled — so a graceful
// broker shutdown doesn't abandon a reaction a worker is mid-way through.
// DrainGrace 0 → in-flight cancelled at once with ctx.
func (d *ReactorDispatcher) Run(ctx context.Context) error {
	// In-flight deliveries run under drainCtx: it lingers DrainGrace past ctx so a
	// worker mid-lifecycle-reaction finishes before being cancelled. The poll loop
	// / listener / heartbeat below follow ctx (stop claiming at once).
	drainCtx, cancelDrain := drainctx.New(ctx, d.DrainGrace)
	defer cancelDrain()

	wakeup := make(chan struct{}, 1)

	var wg sync.WaitGroup
	defer wg.Wait()

	// One held LISTEN conn turns a committed transition into a wake. Pure latency
	// optimisation over the poll: a dead listener just degrades to IdleInterval.
	wg.Add(1)
	go func() {
		defer wg.Done()
		d.Listener.Listen(ctx, "reactor lifecycle_ready", lifecycleReadyChannel, func() { notify(wakeup) })
	}()

	// Heartbeat loop: refresh THIS pod's in-flight claims so a slow reactor isn't
	// reaped mid-delivery (the Reaper's LifecycleStaleAfter is intentionally short
	// for fast failed-delivery re-arm, so a legitimately slow upload MUST keep its
	// claim fresh). It refreshes EXACTLY the (key, claim_epoch) tuples this pod holds
	// (inflightSnapshot) — not a bare `broker_id = me` predicate: the epoch pin makes a
	// row a concurrent claim/reap re-issued (bumped epoch) no longer match, so the
	// heartbeat skips it instead of chasing its updated tuple version and deadlocking
	// with the re-claimer. An empty set → ZERO DB writes (idle react pod is silent).
	// Mirrors WorkQueueHeartbeat's (id, claim_epoch) contract exactly.
	//
	// DRAIN INVARIANT: this loop is bound to the Run `ctx`, so it stops at SIGTERM while
	// an in-flight delivery keeps running under its own drain ctx. That is safe ONLY while
	// the graceful-drain window (SHUTDOWN_DRAIN, default 45s) stays BELOW LifecycleStaleAfter
	// (default 60s): a delivery draining past the stale window would get its lifecycle_outbox
	// claim reaped + re-armed and double-fire the external side effect (the dedup token is the
	// only backstop then). The two knobs are independent — if SHUTDOWN_DRAIN is raised above
	// LifecycleStaleAfter, that safety margin inverts. Keep DrainGrace < LifecycleStaleAfter.
	if d.HeartbeatEvery > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(d.HeartbeatEvery)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					held := d.inflightSnapshot()
					if len(held) == 0 {
						continue // nothing claimed → no row to keep alive → skip the write
					}
					shards := d.Shards.Snapshot().Shards
					if len(shards) == 0 {
						continue
					}
					if err := d.Repo.HeartbeatReactorClaims(ctx, held, shards); err != nil && ctx.Err() == nil {
						slog.Warn("reactor heartbeat", "err", err)
					}
				}
			}
		}()
	}

	// repollOnWork=true: like the Drainer, a tick that delivered re-polls
	// immediately so a backlog (and rows that landed DURING the tick) clear at
	// full speed without waiting out the interval. The poll loop ticks under
	// claimCtx (stops claiming on shutdown); each tick runs its deliveries under
	// drainCtx so in-flight reactions finish during the drain window.
	pollLoop(ctx, "reactor", d.Interval, d.IdleInterval, d.HotWindow, true, wakeup,
		func(claimCtx context.Context) (bool, error) { return d.tick(claimCtx, drainCtx) })
	return nil
}

// tick claims one batch in the pod's shard range (under claimCtx) and dispatches
// it (deliveries run under drainCtx). Reports work=true if any delivery was
// claimed so pollLoop re-polls immediately.
func (d *ReactorDispatcher) tick(ctx, drainCtx context.Context) (bool, error) {
	shards := d.Shards.Snapshot().Shards
	deliveries, err := d.Repo.ClaimReactorDeliveries(ctx, d.BrokerID, d.BatchSize, shards)
	if err != nil {
		return false, fmt.Errorf("claim reactor deliveries: %w", err)
	}
	if len(deliveries) == 0 {
		return false, nil
	}

	parallel := d.MaxParallel
	if parallel < 1 {
		parallel = 1
	}
	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup
	for _, del := range deliveries {
		// Acquire a slot, but stay cancellable: on shutdown the send must not
		// block behind a wedged outbound reactor that ignores ctx (the high-risk
		// path the MaxParallel cap exists for). Stop launching; join what's running.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return true, nil
		}
		wg.Add(1)
		d.trackInflight(del) // heartbeat this delivery's (key, epoch) while its reaction runs
		go func(del store.ReactorDelivery) {
			defer wg.Done()
			defer d.untrackInflight(del)
			defer func() { <-sem }()
			// Deliver under drainCtx, not claimCtx: a graceful shutdown stops the sem
			// loop above from launching NEW deliveries, but an in-flight one (a worker
			// running the lifecycle reaction) finishes to its deadline under drainCtx
			// instead of being abandoned. tick's wg.Wait joins them either way.
			d.deliver(drainCtx, del)
		}(del)
	}
	wg.Wait()
	return true, nil
}

// deliver runs one claimed transition's lifecycle reaction through the
// StageDispatcher and acks on success. On failure (reaction error or no
// handler registered for the kind) the row is LEFT claimed: its heartbeat goes
// stale and the reaper re-arms it for another at-least-once attempt — so this
// never loses a delivery, only retries it.
func (d *ReactorDispatcher) deliver(ctx context.Context, del store.ReactorDelivery) {
	if ctx.Err() != nil {
		return // shutting down — leave the claim for the reaper to re-arm
	}
	// The kind to dispatch to is the reactor handle (del.Reactor) — the kind whose
	// worker runs the reaction, distinct from the watched del.Kind. The reaction
	// NAME is del.Reaction, resolved by the claim from the reactor's OWN CRD (its
	// single `reactor`-trigger reaction), NOT parsed from the binding name (which
	// is operator-chosen and opaque). An empty Reaction means the reactor CRD
	// isn't applied yet — leave the row claimed for the reaper (at-least-once).
	kind := del.Reactor
	reactionName := del.Reaction
	if reactionName == "" {
		slog.Warn("reactor: no reactor reaction resolved for kind; leaving claimed for retry",
			"reactor", del.Reactor, "binding", del.BindingName, "resource", del.ResourceID)
		return
	}

	// On a broker that fans out to workers, skip a delivery whose reactor kind
	// has NO connected worker rather than parking it (which would block this
	// goroutine on the fanout buffer until a worker connects). Left claimed → the
	// reaper re-arms it once a worker is up (at-least-once). In-process executor:
	// ReadyToDispatch is nil → never skip (a missing handler fails Terminal below).
	if d.ReadyToDispatch != nil && !d.ReadyToDispatch(kind) {
		return
	}

	rx, req := buildReactorRequest(del, reactionName)

	// A missing handler (kind's reaction not registered on this pod / Setup still
	// pending) surfaces as a Terminal "no handler" error here, which — like any
	// reactor error — leaves the row claimed for the reaper to re-arm. So the
	// at-least-once contract covers both the failed-side-effect and the
	// not-yet-registered cases without a special pre-check.
	// The claim already resolved the reactor kind_version this reaction came from
	// (the binding's pin, else the reactor's highest published version) into
	// del.ReactorKindVersion; pass it so the worker looks up THAT version's handler
	// and pulls THAT version's default config — a v2 reactor runs its v2 handler,
	// never silently v1. It is always ≥1 here (reaction + reactor_kind_version come
	// from the same claim_reactor_deliveries LATERAL row; a NULL reaction returned
	// early above). The registry Lookup matches the version EXACTLY — a 0 would be a
	// hard "no handler" miss (a real failure), never coerced to v1.
	if _, _, err := d.dispatcher().DispatchStage(ctx, kind, del.ReactorKindVersion, rx, req); err != nil {
		// At-least-once: don't ack, let the reaper re-deliver.
		slog.Error("reactor: delivery failed; will retry",
			"reactor", del.Reactor, "reaction", reactionName, "binding", del.BindingName,
			"resource", del.ResourceID, "err", err)
		return
	}
	if err := d.Repo.AckReactorDelivery(ctx, del.ResourceID, del.Transition, del.Generation, del.BindingName, del.ClaimEpoch); err != nil {
		// The side effect succeeded but the ack didn't commit — the row will be
		// re-delivered (at-least-once). Idempotent reactors (keyed on the dedup
		// token) make this a no-op overwrite.
		slog.Warn("reactor: ack failed; delivery may repeat (idempotent by token)",
			"resource", del.ResourceID, "transition", del.Transition, "err", err)
	}
}

// buildReactorRequest maps a claimed lifecycle delivery into the (ReactionDecl,
// ReactionRequest) the dispatcher runs. No per-binding ProviderConfig: the
// reactor kind's WHERE-to-deliver config (bucket/endpoint/prefix) comes from its
// kind's DEFAULT providerconfig, which the worker pulls via GetProviderConfig like
// any kind — the Env here carries only the logger; the worker rebuilds Env from
// the kind's config on its side. The transition that fired is passed as DATA
// (req.Transition), not on rx.
func buildReactorRequest(del store.ReactorDelivery, reactionName string) (model.ReactionDecl, model.ReactionRequest) {
	res := model.Resource{
		ID:         del.ResourceID,
		Kind:       del.Kind,
		Name:       del.Name,
		Status:     del.Status,
		Generation: del.Generation,
	}
	rx := model.ReactionDecl{
		Name:    reactionName,
		Trigger: model.TriggerReactor,
	}
	req := model.ReactionRequest{
		Reaction: reactionName,
		Trigger:  model.TriggerReactor,
		Resource: res,
		// del.Transition is the DB's stringly-typed edge; hand the reactor the
		// typed constant. store/DB keep string (the CHECK constraint is the DB gate).
		Transition: model.Transition(del.Transition),
		Generation: del.Generation,
		DedupToken: store.DedupToken(del.ResourceID, del.Transition, del.Generation, del.BindingName),
		Env: &model.Env{
			Logger: slog.With(
				"reactor", del.Reactor,
				"reaction", reactionName,
				"binding", del.BindingName,
				"resource", del.ResourceID,
				"transition", del.Transition,
				"generation", del.Generation,
			),
		},
	}
	return rx, req
}
