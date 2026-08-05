package broker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/wire"
	"github.com/salesforce/converge/sdk-go/workerpb"
)

// dispatch.go: the fanoutExecutor's task-dispatch tail — mint a lease token,
// attribute a task to a local worker stream (+ credit accounting), resolve a
// Complete back to its parked stage (deliver/drop), and the StageDispatcher entry
// points (dispatchStage/DispatchStage) that encode a reaction to a proto task, park
// it for a worker, and decode the result. Subscriber management, credit
// advertisement, and mesh reference forwarding live alongside in fanout.go.

// newToken mints an UNGUESSABLE lease token. A predictable "lease-<seq>" let any
// authenticated peer on the shared claim tier spray Completes to resolve another
// worker's stage (e.g. an empty Compose → subtree teardown); a random UUID closes
// that. Delivery is ALSO stream-bound (see deliver/owner), so even a leaked token
// can't be Completed from a foreign stream.
func (f *fanoutExecutor) newToken() string {
	return uuid.NewString()
}

// attributeToStream marks that task ps was sent to local worker stream sub: it
// records sub on the pending stage and consumes one of sub's slots, then
// re-advertises the kind's now-lower credit to the mesh so peers stop over-forwarding.
//
// A FOREIGN task (a reference forwarded by a peer; ps.foreign) does NOT consume a
// credit slot: this broker holds no lease for it, so no local resolution path (deliver)
// would ever free that slot — a slot consumed here would leak. Credit is a best-effort
// routing HINT (correctness is the epoch fence, not the slot count), so a forwarded
// reference merely runs on a worker's real capacity without being reflected in the
// advertised hint. It is always "delivered" (true). NB: a foreign task DOES carry a
// non-nil resultCh (its forwarder goroutine drains it to send the result home), so
// ps.foreign — not resultCh==nil — is the discriminator.
//
// Consume-once + still-live guard (fix for the credit-leak race) on a LOCAL token: it
// may already have been freed between the buffer send and this call — a deadline/drain
// drop() can delete it while ps sits in the per-kind buffer. If a local token is gone,
// DON'T consume a slot (the increment would be unrecoverable, since no resolution path
// would ever match this ps again) and report false so WorkStream treats the send as
// un-delivered. slotHeld makes the consume idempotent with the free path.
func (f *fanoutExecutor) attributeToStream(ps *pendingStage, sub *configSub) bool {
	km := taskKindVer(ps.task)
	kind := km.Kind // for the (kind-only) metrics label; credit is re-advertised per (kind, kindVersion)
	token := ps.task.GetLeaseToken()
	if ps.foreign {
		return true // forwarded reference: run it, but don't touch the credit-hint slot
	}
	f.mu.Lock()
	if _, live := f.inflight[token]; !live {
		f.mu.Unlock()
		return false // resolved out from under us while buffered — do not attribute
	}
	newlySent := false
	if !ps.slotHeld { // idempotent: never consume twice
		ps.sentBy = sub
		sub.inflight++
		ps.slotHeld = true
		newlySent = true
	}
	// Capture attribution inputs UNDER the lock (sub.peer / ps.lease may be
	// mutated by a concurrent teardown once we release f.mu). The attributed id is the
	// worker's OBSERVED identity (SPIFFE ID / mesh header / peer IP), never a self-report.
	lease := ps.lease
	workerID := sub.peer.id
	f.mu.Unlock()
	f.advertiseCredits([]model.KindVersion{km})
	// UI attribution (best-effort): enqueue "task -> worker" for the coalescing
	// batcher. Only for a LOCAL task (a forwarded reference belongs to a peer's row)
	// and only on the first send. Non-blocking send — a full channel just drops the
	// event (the UI keeps the broker id); attribution never blocks or slows dispatch.
	//
	// KNOWN cosmetic window (accepted): if THIS broker re-claims the SAME row at a
	// newer generation while a prior event for the old generation is still buffered,
	// the batched UPDATE (scoped only by id+shard+worker_id, no generation predicate)
	// can briefly stamp the old worker. It self-heals on the next dispatch, and the
	// fresh claim already NULLs worker_id, so the label is at worst transiently
	// stale — not worth threading a generation predicate through the write for.
	if newlySent && f.attrCh != nil && lease.WorkID != uuid.Nil && workerID != "" {
		select {
		case f.attrCh <- execAttr{id: lease.WorkID, shard: lease.ShardID, workerID: workerID}:
		default: // batcher busy / channel full — drop (best-effort)
		}
	}
	// Metrics: count a stage dispatched to a worker (once, on first send). nil until
	// SetMetrics wires it. Cheap Add on an already-computed kind attribute.
	if newlySent && f.claimed != nil {
		f.claimed.Add(context.Background(), 1, metric.WithAttributes(attribute.String("kind", string(kind))))
	}
	return true
}

// claimGateSlot records, on the pending stage, the worker stream whose capacity-gate
// slot this stage occupies for its whole in-flight life. The WorkStream main loop has
// already acquired that slot (sub.slots) before committing the task; recording it here
// lets drop/deliver return the token exactly once when the stage leaves f.inflight.
// Covers a foreign/mesh reference too (which the credit-hint slotHeld does not).
func (f *fanoutExecutor) claimGateSlot(ps *pendingStage, sub *configSub) {
	f.mu.Lock()
	ps.gateSub = sub
	f.mu.Unlock()
}

// releaseSlot returns one capacity-gate token to sub.slots (non-blocking; the default
// guards a redundant call from over-releasing below zero). Called from the slot-free
// sites (drop/deliver/unattribute) so the worker's main loop can acquire a slot and
// pull its next task the instant an in-flight task resolves. Runs OUTSIDE f.mu.
func releaseSlot(sub *configSub) {
	if sub == nil || sub.slots == nil {
		return
	}
	select {
	case <-sub.slots:
	default:
	}
}

// unattributeFromStream reverses attributeToStream when a send fails before the
// task actually reached the worker (so no Complete will free the slot): free the
// slot and re-advertise the restored credit.
func (f *fanoutExecutor) unattributeFromStream(ps *pendingStage) {
	km := taskKindVer(ps.task)
	f.mu.Lock()
	var freedSub *configSub
	// Free at most once via slotHeld: a deadline drop() may have already freed this
	// stage's slot (and cleared sentBy) — reversing again would double-count.
	if ps.slotHeld {
		if ps.sentBy != nil && ps.sentBy.inflight > 0 {
			ps.sentBy.inflight--
		}
		freedSub = ps.sentBy
		ps.slotHeld = false
		ps.sentBy = nil
	}
	f.mu.Unlock()
	releaseSlot(freedSub) // return the capacity-gate token so the stream can pull again
	f.advertiseCredits([]model.KindVersion{km})
}

// drop removes a resolved token from inflight and — if the task had been sent to a
// local worker stream — frees that stream's credit slot and re-advertises the
// kind's refreshed credit to the mesh. The credit free is DERIVED from the pending
// stage's sentBy (M4: no hand-maintained per-peer counter). Idempotent: a second
// drop for the same token (deadline racing Complete) finds it gone and no-ops.
func (f *fanoutExecutor) drop(token string) {
	f.mu.Lock()
	ps, ok := f.inflight[token]
	if !ok {
		f.mu.Unlock()
		return
	}
	delete(f.inflight, token)
	// Return the capacity-gate token (once) — covers local AND foreign; independent of
	// the credit-hint slotHeld below. Clear gateSub so a racing deliver/drop can't
	// double-release.
	gateSub := ps.gateSub
	ps.gateSub = nil
	var freedKM model.KindVersion
	freed := false
	// Free the credit slot at most once, gated on slotHeld (not `sentBy != nil`):
	// a racing unattributeFromStream on the same stage must not decrement twice. We
	// clear both slotHeld AND sentBy so any later path is a no-op.
	if ps.slotHeld {
		if ps.sentBy != nil && ps.sentBy.inflight > 0 {
			ps.sentBy.inflight--
		}
		ps.slotHeld = false
		ps.sentBy = nil
		freedKM = taskKindVer(ps.task)
		freed = true
	}
	f.mu.Unlock()
	releaseSlot(gateSub) // return the capacity-gate token → stream pulls its next task
	if freed {
		f.advertiseCredits([]model.KindVersion{freedKM}) // a slot freed → that (kind, kindVersion)'s credit rose
	}
}

// deliver fulfils a lease from a worker's Complete. No-ops if the token
// is unknown — either the worker raced a deadline/release (the dispatcher already
// moved on) or the token belongs to a FORWARDED reference this broker executed for a
// peer (this broker holds no lease for it; the forwarding peer owns the row). The
// (generation, claim_epoch, manifest) AppendOutbox fence makes any duplicate harmless.
func (f *fanoutExecutor) deliver(token string, sc *workerpb.StageComplete) {
	f.mu.Lock()
	ps, ok := f.inflight[token]
	var freedKM model.KindVersion
	var gateSub *configSub
	freedSlot := false
	if ok {
		// drop the token WHILE STILL HOLDING f.mu, before releasing the resultCh, so
		// a concurrent stream-drop abandon() (which also targets this token) finds it gone
		// and no-ops instead of racing a synthetic transient failure into the buffered(1)
		// resultCh — which would make the parked dispatchStage take the failure and DISCARD
		// this genuine result. Single-writer of the channel is now guaranteed under the lock.
		delete(f.inflight, token)
		// Return the capacity-gate token (once) — covers local AND foreign.
		gateSub = ps.gateSub
		ps.gateSub = nil
		// FREE THE CREDIT SLOT HERE (mirrors drop). dispatchStage calls drop(token) after
		// it wakes on resultCh, but this delete already removed the token, so that drop
		// finds nothing and its sub.inflight-- never runs — leaking one credit slot per
		// COMPLETED task (the broker-side inflight counter climbs past max_inflight, e.g.
		// 973/100; at scale the leaked credit starves the worker and wedges the fleet).
		// So decrement on the resolving path, gated on slotHeld, clearing slotHeld+sentBy
		// so drop()'s later call stays a no-op (never double-frees).
		if ps.slotHeld {
			if ps.sentBy != nil && ps.sentBy.inflight > 0 {
				ps.sentBy.inflight--
			}
			ps.slotHeld = false
			ps.sentBy = nil
			freedKM = taskKindVer(ps.task)
			freedSlot = true
		}
	}
	f.mu.Unlock()
	releaseSlot(gateSub) // return the capacity-gate token → stream pulls its next task
	if freedSlot {
		f.advertiseCredits([]model.KindVersion{freedKM}) // a freed slot raises that (kind, kindVersion)'s credit
	}
	if !ok {
		return
	}
	// Metrics: count a resolved stage result by outcome (success | terminal |
	// transient). nil until SetMetrics wires it.
	if f.completed != nil {
		outcome := "success"
		if sc.GetErrorMessage() != "" || sc.GetTerminal() {
			if sc.GetTerminal() {
				outcome = "terminal"
			} else {
				outcome = "transient"
			}
		}
		f.completed.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", outcome)))
	}
	// A best-effort send: the parked dispatchStage receives it, or (buffer full /
	// already resolved) the result is dropped and the deadline/reaper recovers.
	select {
	case ps.resultCh <- sc:
	default:
	}
}

// dispatchStage parks a StageTask, blocks until the worker Completes it or ctx
// (task deadline / shutdown) fires, and returns the worker's StageComplete. ctx
// cancellation returns (nil, ctx.Err()) so the caller records a transient
// failure and the lease frees for re-dispatch. Shared by all five stages.
func (f *fanoutExecutor) dispatchStage(ctx context.Context, _ model.Kind, task *workerpb.StageTask) (*workerpb.StageComplete, error) {
	// Carry the task's REMAINING deadline to the worker so it bounds its own
	// reaction (handler) ctx (the broker's deadline timer below stays
	// authoritative, but the worker stopping promptly frees the slot + lease
	// without waiting it out). The deadline comes from runOne's
	// context.WithTimeout(TaskDeadline); a task with no deadline (no ctx deadline)
	// leaves TaskDeadlineMs at 0 (worker runs unbounded, the same as the in-process
	// path for a 0 TaskDeadline).
	//
	// FLOOR at 1ms: a deadline DOES exist (ctx.Deadline ok) but the remaining
	// budget can truncate to 0 ms — sub-millisecond remaining, or the tail stage of
	// a multi-stage reconcile that consumed most of a tight deadline. Sending 0
	// would make the worker run UNBOUNDED (its `ms > 0` guard treats 0 as "no
	// deadline"), silently dropping the worker-side bound for a kind that asked for
	// one. Flooring at 1ms keeps the bound (the handler is cancelled near-immediately,
	// which is the correct outcome for an already-/about-to-be-expired deadline) —
	// the broker's own timer remains the authoritative enforcer regardless.
	if dl, ok := ctx.Deadline(); ok {
		ms := time.Until(dl).Milliseconds()
		if ms < 1 {
			ms = 1
		}
		task.TaskDeadlineMs = ms
	}
	lease, _ := runtime.LeaseFromContext(ctx) // present on the broker path; zero on others
	// Stamp the durable fence onto the task so the completion is SELF-CONTAINED: the
	// strict monotonic claim_epoch and the kind_manifest content hash this row was
	// claimed under travel with the task. Any broker holding this epoch's token may do
	// the fenced result write (owner-independent), so a forwarded task carries its full
	// fence with it. On the in-process/non-broker path the lease is zero, leaving both
	// at 0 (inert).
	task.ClaimEpoch = lease.ClaimEpoch
	task.ManifestVersion = lease.ManifestVersion
	// STRICT routing key: the exact (kind, kindVersion) this task was claimed for. `kind`
	// (the pipeline's) matches task.Kind; the kindVersion rides on the task (baseTask set
	// it from the claimed lease), so every placement/gate/buffer op keys on the pair.
	km := taskKindVer(task)
	ps := &pendingStage{task: task, resultCh: make(chan *workerpb.StageComplete, 1), lease: lease}
	f.mu.Lock()
	f.inflight[task.GetLeaseToken()] = ps
	f.mu.Unlock()

	// PLACEMENT (NATS-style, local-first):
	//   - LOCAL worker for this (kind, kindVersion) → serve it off the local buffer (byKind).
	//     ZERO hop. This is the common case (workers ≥ brokers). A WorkStream fan-in
	//     goroutine pops it and sends it to a worker. Also the resolve path for a worker
	//     that connects LATER.
	//   - No local worker but the mesh is on → FORWARD the durable task reference to a
	//     peer that serves the pair (over its warm route). The reference carries its own
	//     (work_id, shard, generation, claim_epoch, manifest_version) fence, so the peer
	//     can execute it without a lease transfer. If NO peer serves it RIGHT NOW (a
	//     transient presence gap), we must NOT drop the task into our own dead local
	//     buffer — this broker has no worker, so it would strand forever. Instead RETRY
	//     the forward on a short tick until a peer appears, a local worker shows up (then
	//     serve locally), or ctx/deadline fires.
	//   STRICT: every check is on the EXACT (kind, kindVersion) — a v2 worker connecting does
	//   NOT satisfy a v1 task's local gate, and a reference is forwarded only to a peer
	//   serving that kindVersion.
	//
	// TODO(work-plane-redesign R2): the forwarding peer is meant to write the fenced
	// result ITSELF (the fence is owner-independent), which lives in the runtime result
	// path, not this package — so it is not wired here yet. Today, after forwarding, this
	// broker keeps the parked stage and rides ctx/deadline; the row's own heartbeat
	// (worker-attested — a forwarded row is attested on the PEER, not here) lapses and
	// the reaper reclaims + re-issues it under a bumped epoch. Recovery therefore unifies
	// on heartbeat-lapse → reaper, with no home-route hop.
	if f.mesh != nil && !f.HasSubscriber(km.Kind, km.Version) {
		const forwardRetry = 20 * time.Millisecond
		// Loop until a local worker connects (then fall through to serve locally below),
		// a peer accepts the forwarded reference (then park on ctx/deadline), or
		// ctx/deadline fires.
		for !f.HasSubscriber(km.Kind, km.Version) {
			if peer, forwarded := f.mesh.pushTask(km, task); forwarded {
				// The reference is with a peer whose worker runs it; that peer returns the
				// result home over its route (RouteFrame_Complete → onRouteComplete →
				// deliver). Record the peer so onPeerGone can wake this parked stage if the
				// peer departs before returning the result — otherwise the row rides the
				// RemoteFanoutCeiling. The forwarded row is attested on the PEER, so THIS
				// broker's heartbeat no longer covers it → if the peer vanishes the row also
				// goes stale and the reaper reclaims it (bumped epoch) as the backstop.
				f.mu.Lock()
				if held := f.inflight[task.GetLeaseToken()]; held != nil {
					held.pushedTo = peer
				}
				f.mu.Unlock()
				// Also park on the PEER's ctx directly, not only onPeerGone's wake: pushTask
				// (which placed the task under the mesh lock) and the pushedTo record above
				// are separate critical sections, so a peer that departs in the window
				// between them is missed by onPeerGone (it scans inflight for pushedTo==peer
				// while pushedTo is still nil, then we store an already-gone peer no future
				// onPeerGone fires for). peer.ctx is cancelled on departure regardless of
				// timing, so this arm covers the race window. We resolve it EXACTLY as
				// onPeerGone does — a synthetic TRANSIENT StageComplete (retryable, never
				// terminal) — so the peer-departed outcome is identical whether onPeerGone
				// wins or this arm does: the row re-dispatches, it does not fail.
				peerGone := peer.ctx
				if peerGone == nil {
					peerGone = context.Background() // test peers may carry no ctx → never fires
				}
				select {
				case sc := <-ps.resultCh:
					f.drop(task.GetLeaseToken())
					return sc, nil
				case <-peerGone.Done():
					f.drop(task.GetLeaseToken())
					return &workerpb.StageComplete{ErrorMessage: reasonPeerDeparted, Terminal: false}, nil
				case <-ctx.Done():
					f.drop(task.GetLeaseToken())
					return nil, ctx.Err()
				}
			}
			// No peer serves it right now — wait briefly and retry (or bail on ctx). We do
			// NOT queue to the local buffer: with no local worker it would strand.
			select {
			case <-time.After(forwardRetry):
			case <-ctx.Done():
				f.drop(task.GetLeaseToken())
				return nil, ctx.Err()
			}
		}
	}

	q := f.queueFor(km)
	select {
	case q <- ps:
	case <-ctx.Done():
		f.drop(task.GetLeaseToken())
		return nil, ctx.Err()
	}
	select {
	case sc := <-ps.resultCh:
		f.drop(task.GetLeaseToken())
		return sc, nil
	case <-ctx.Done():
		f.drop(task.GetLeaseToken())
		return nil, ctx.Err()
	}
}

func (f *fanoutExecutor) baseTask(kind model.Kind, kindVersion int, stage workerpb.Stage, reaction string, env *model.Env, resID uuid.UUID, gen int64) *workerpb.StageTask {
	var pc, pb []byte
	if env != nil {
		pc = env.ProviderConfig
		pb = env.ProviderBundle
	}
	return &workerpb.StageTask{
		Stage:          stage,
		Kind:           string(kind),
		KindVersion:    int32(kindVersion), // the explicit (kind, kindVersion) this task was claimed for — STRICT routing rides it
		Reaction:       reaction,           // the worker looks up its handler by (kind, reaction)
		ResourceId:     resID[:],
		Generation:     gen,
		ProviderConfig: pc,
		ProviderBundle: pb, // CUSTOM bundle override; empty = kind default
		LeaseToken:     f.newToken(),
	}
}

// run fans one built StageTask out to a worker and normalizes the result: it
// returns a non-nil StageComplete ONLY on a clean run. A dispatch error (ctx
// cancel / abandon) or a worker-reported stage failure both come back as
// (nil, failedStage, err) — err is transient or terminal per the wire — so every
// DispatchStage arm reduces to "build task → f.run → decode sc". This is the single
// place the 6 stages shared verbatim; keeping it one func also keeps the failed-
// stage index uniform (it was inconsistently 0 for OPERATE before).
func (f *fanoutExecutor) run(ctx context.Context, kind model.Kind, t *workerpb.StageTask) (*workerpb.StageComplete, int, error) {
	sc, err := f.dispatchStage(ctx, kind, t)
	if err != nil {
		return nil, 0, err
	}
	if msg := sc.GetErrorMessage(); msg != "" {
		return nil, int(sc.GetFailedStage()), stageErr(msg, sc.GetTerminal())
	}
	return sc, 0, nil
}

// DispatchStage is the broker's model.StageDispatcher: it maps a manifest
// ReactionDecl → the matching proto Stage, builds the TYPED StageTask (via
// wire) from the unified ReactionRequest, fans it out to a connected dumb
// worker (f.run), and decodes the typed StageComplete back into an Outcome. The
// Stage enum is the on-the-wire shape; the reaction's (Trigger, Emits) selects it.
func (f *fanoutExecutor) DispatchStage(ctx context.Context, kind model.Kind, kindVersion int, rx model.ReactionDecl, req model.ReactionRequest) (model.Outcome, int, error) {
	// The claimed task's KIND VERSION (v1, v2, …) is passed by the reaction engine
	// (from work_queue.kindVersion) and is authoritative; the broker stamps it on
	// every StageTask for STRICT (kind, kindVersion) routing so a v1 task reaches a
	// v1 worker's v1 handler. There is NO implicit v1 default: if a caller doesn't
	// thread it (0), fall back to the dispatcher's ctx-Lease kindVersion — a 0 that
	// survives both routes to (kind, 0) and finds no worker (a real miss, never a
	// silent v1).
	if kindVersion < 1 {
		if l, ok := runtime.LeaseFromContext(ctx); ok && l.KindVersion > 0 {
			kindVersion = l.KindVersion
		}
	}
	switch {
	case rx.Emits.Has(model.OutcomeChildren):
		t := f.baseTask(kind, kindVersion, workerpb.Stage_STAGE_COMPOSE, rx.Name, req.Env, req.Resource.ID, req.Resource.Generation)
		creq, err := wire.ComposeReqToProto(model.ComposeRequest{Root: req.Resource, Observed: req.Observed, Env: req.Env})
		if err != nil {
			return model.Outcome{}, 0, model.Terminal(fmt.Errorf("encode compose request: %w", err))
		}
		t.ComposeReq = creq
		sc, fs, err := f.run(ctx, kind, t)
		if err != nil {
			return model.Outcome{}, fs, err
		}
		r := wire.ComposeResultFromProto(sc.GetCompose())
		return model.Outcome{Children: r.Desired, Edges: r.Edges, Configs: r.Configs, Status: r.Status, Conditions: r.Conditions}, 0, r.Err

	case rx.Trigger == model.TriggerChildrenSettled:
		t := f.baseTask(kind, kindVersion, workerpb.Stage_STAGE_ROLLUP, rx.Name, req.Env, req.Resource.ID, req.Resource.Generation)
		t.RollupReq = wire.RollupReqToProto(model.RollupRequest{Root: req.Resource, Descendants: req.Descendants, Status: req.Status, Env: req.Env})
		sc, fs, err := f.run(ctx, kind, t)
		if err != nil {
			return model.Outcome{}, fs, err
		}
		return model.Outcome{Status: wire.RawOrNil(sc.GetStatus()), Conditions: wire.ConditionsFromProto(sc.GetConditions())}, 0, nil

	case rx.Emits.Has(model.OutcomeFinalizer):
		t := f.baseTask(kind, kindVersion, workerpb.Stage_STAGE_DELETE, rx.Name, req.Env, req.Resource.ID, req.Resource.Generation)
		t.DeleteReq = wire.DeleteReqToProto(model.DeleteRequest{Resource: req.Resource, Env: req.Env})
		_, fs, err := f.run(ctx, kind, t)
		if err != nil {
			return model.Outcome{}, fs, err
		}
		return model.Outcome{}, 0, nil

	case rx.Emits.Has(model.OutcomeOperationOutput):
		var op model.Operation
		if req.Operation != nil {
			op = *req.Operation
		}
		t := f.baseTask(kind, kindVersion, workerpb.Stage_STAGE_OPERATE, rx.Name, req.Env, req.Resource.ID, req.Resource.Generation)
		t.OperateReq = wire.OperateReqToProto(model.OperateRequest{Resource: req.Resource, Operation: op, Env: req.Env})
		sc, fs, err := f.run(ctx, kind, t)
		if err != nil {
			return model.Outcome{}, fs, err
		}
		return model.Outcome{OperationOutput: wire.RawOrNil(sc.GetOutput())}, 0, nil

	case rx.Trigger == model.TriggerReactor:
		// A reactor is a side effect on a transition — status-only on the wire, like
		// WORK. The ReactorDispatcher (running on this broker) parks it here; the
		// worker runs the reactor kind's reaction and Completes. The dispatcher acks
		// lifecycle_outbox only after this returns nil error, so a failed/undelivered
		// react leaves the row claimed for the reaper (at-least-once).
		t := f.baseTask(kind, kindVersion, workerpb.Stage_STAGE_REACT, rx.Name, req.Env, req.Resource.ID, req.Resource.Generation)
		t.ReactReq = wire.ReactReqToProto(model.ReactRequest{
			Resource: req.Resource, Transition: req.Transition, DedupToken: req.DedupToken, Env: req.Env,
		})
		sc, fs, err := f.run(ctx, kind, t)
		if err != nil {
			return model.Outcome{}, fs, err
		}
		return model.Outcome{Status: wire.RawOrNil(sc.GetStatus()), Conditions: wire.ConditionsFromProto(sc.GetConditions())}, 0, nil

	default: // SpecChange/Resync work (status only)
		t := f.baseTask(kind, kindVersion, workerpb.Stage_STAGE_WORK, rx.Name, req.Env, req.Resource.ID, req.Resource.Generation)
		t.WorkReq = wire.WorkReqToProto(model.WorkRequest{Resource: req.Resource, Status: req.Status, Env: req.Env})
		sc, fs, err := f.run(ctx, kind, t)
		if err != nil {
			return model.Outcome{}, fs, err
		}
		return model.Outcome{Status: wire.RawOrNil(sc.GetStatus()), Conditions: wire.ConditionsFromProto(sc.GetConditions())}, 0, nil
	}
}

// stageErr rebuilds a stage error from the wire, preserving terminal-ness so the
// dispatcher records the same terminal/transient failure it would in-process.
func stageErr(msg string, terminal bool) error {
	err := errors.New(msg)
	if terminal {
		return model.Terminal(err)
	}
	return err
}
