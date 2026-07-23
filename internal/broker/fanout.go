package broker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric"

	"github.com/salesforce/converge/internal/meshpb"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/sdk-go/workerpb"
)

// execAttr is one UI-attribution event: task T was dispatched to worker workerID.
// Coalesced by runAttributionBatcher into a single batched worker_id UPDATE.
type execAttr struct {
	id       uuid.UUID
	shard    int16
	workerID string
}

// Attribution batcher tuning: flush at whichever comes first. Small + frequent so
// the "running on <worker>" label appears promptly, but coalesced so a high dispatch
// rate collapses to one UPDATE per tick instead of one per task (the batching
// discipline the rest of work_queue follows). Best-effort throughout.
const (
	attrFlushEvery = 100 * time.Millisecond
	attrFlushMax   = 500
	attrChanBuf    = 2048
)

func (f *fanoutExecutor) runAttributionBatcher(ctx context.Context) {
	if f.attrCh == nil || f.markWorkerBatch == nil {
		return
	}
	t := time.NewTicker(attrFlushEvery)
	defer t.Stop()
	// Dedup by task id within a window: a task's LATEST workerID wins (a re-send
	// after a worker flap), and we never emit the same id twice in one UPDATE.
	pending := make(map[uuid.UUID]execAttr, attrFlushMax)
	flush := func() {
		if len(pending) == 0 {
			return
		}
		ids := make([]uuid.UUID, 0, len(pending))
		by := make([]string, 0, len(pending))
		shards := make([]int16, 0, len(pending))
		for _, a := range pending {
			ids = append(ids, a.id)
			by = append(by, a.workerID)
			shards = append(shards, a.shard)
		}
		clear(pending)
		wctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = f.markWorkerBatch(wctx, f.brokerID, ids, by, shards)
		cancel()
	}
	for {
		select {
		case <-ctx.Done():
			flush() // best-effort final flush of whatever's buffered
			return
		case a := <-f.attrCh:
			pending[a.id] = a
			if len(pending) >= attrFlushMax {
				flush()
			}
		case <-t.C:
			flush()
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────
// fanout executor — the model.StageDispatcher the dispatcher's Loop calls
// for EVERY reaction (compose/work/rollup/delete/operate, mapped from the
// manifest's ReactionDecl to a wire Stage). It bridges the synchronous reconcile
// pipeline to the async worker streams: build a StageTask (the reaction request
// as TYPED proto via wire — only the opaque provider spec/status/input stays
// bytes), park it in the per-kind buffer, block on the lease's result channel
// until a worker Completes it, then resume.
// The broker (lease-holder) does ALL the DB reads/writes around every stage.
// ─────────────────────────────────────────────────────────────────────────

type pendingStage struct {
	task     *workerpb.StageTask
	resultCh chan *workerpb.StageComplete
	lease    runtime.Lease // work_queue id+shard+worker for scoped release on abandonment
	// sentBy is the LOCAL worker stream a task was handed to (set by WorkStream when it
	// sends the task), so its resolution (Complete / abandon / deadline drop) frees
	// that stream's credit slot. nil = not yet sent to a local worker, or a
	// FOREIGN task (a reference forwarded by a peer; no local lease/resultCh).
	sentBy *configSub
	// pushedTo is the peer this broker FORWARDED the task reference to (nil for a
	// locally-served or not-yet-placed stage). When that peer DEPARTS the cluster, the
	// owner can no longer receive the result home, so onPeerGone wakes this parked stage
	// with a synthetic transient → its (already reaper-freed) row re-claims at once
	// instead of riding the RemoteFanoutCeiling. Set under f.mu at push time.
	pushedTo *peerClient
	// slotHeld is the single source of truth for "this stage currently occupies one
	// of sentBy's credit slots" — flipped ONLY under f.mu. It makes the slot
	// consume/free idempotent across the sites that touch it (attributeToStream
	// consumes; drop / unattributeFromStream release): keying the free on
	// `sentBy != nil` + map membership instead would risk a double-free (drop leaving
	// sentBy set for a racing unattribute) or a leak (attribute running after a drop
	// already freed the token). Consume sets it true; each release path frees at most
	// once by testing-and-clearing it. Not touched outside f.mu.
	slotHeld bool
	// gateSub is the worker stream whose CAPACITY-GATE slot (sub.slots) this stage
	// occupies while it is in flight on that worker. Set by the WorkStream loop the
	// instant it commits a task to a worker (BOTH a local task AND a foreign/mesh-
	// forwarded reference — both consume the worker's real execution capacity, unlike
	// the credit-HINT slot which slotHeld tracks for local tasks only). Released exactly
	// once when the stage leaves f.inflight (drop/deliver), returning the token so the
	// stream's main loop can pull its next task. Distinct from sentBy (credit hint):
	// gateSub is the real occupancy the maxInflight gate enforces. Set/read under f.mu.
	gateSub *configSub
}

// NOTE on token security: lease tokens are random UUIDs (newToken), which closes
// the practical exploit — a peer can no longer ENUMERATE/guess another worker's
// token to spray a forged Complete. And because a StageComplete now rides the SAME
// bidi WorkStream that delivered the task (not a separate unary RPC), a Complete is
// inherently BOUND to the stream that received the work: the broker resolves it
// against the leases IT handed to THAT stream, so a token leaked to a foreign stream
// can't Complete work it wasn't given. The UUID token + this stream binding + the
// AppendOutbox (generation, claim_epoch, manifest) fence (which rejects a
// stale/duplicate/misrouted apply) cover the realistic threat together.

// The broker keys every buffer, gate, and credit edge on model.KindVersion — the STRICT
// routing key: a task claimed at vpc/v2 is delivered ONLY to a worker/peer advertising
// vpc/v2, there is no "any"/wildcard version. The version is always the EXPLICIT web-API
// value (>= 1); there is no normalization, so an unset (0) key matches nothing (the
// manifest/worker that omitted it advertised (kind, 0) and serves NOTHING).

type fanoutExecutor struct {
	mu sync.Mutex
	// byKind buffers stage tasks claimed-but-not-yet-delivered, per (kind, kindVersion). A
	// WorkStream fan-in goroutine for a given (kind, kindVersion) pops ONLY from that
	// exact buffer — STRICT routing: a v2 worker never sees a v1 task and vice
	// versa. Bounded indirectly by the dispatcher's maxParallel (it won't claim past
	// free slots).
	byKind map[model.KindVersion]chan *pendingStage
	// inflight maps a lease token to the pending stage awaiting a worker Complete.
	inflight map[string]*pendingStage
	// foreignOwner maps a FORWARDED task's token to the peer that pushed it to us. Our
	// local worker runs the reference; when it Completes we return the StageComplete to
	// that owner over its route (a `complete` frame) so the owner does the fenced write.
	// Populated in onRouteTask, cleared when the forwarder resolves. Under f.mu.
	foreignOwner map[string]*peerClient
	// subscribers counts the live WorkStream streams advertising each (kind, kindVersion):
	// kind → kindVersion → count. The claim gate (HasSubscriber) refuses to claim a
	// (kind, kindVersion) with ZERO connected workers, so the broker never pulls work it
	// can't deliver — the rows stay worker_id IS NULL for another broker or a later
	// poll (no buffer fill, no stranding, no dispatchStage blocked forever).
	// G-BUFFER-STARVATION fix. STRICT per-kindVersion: a v2-only worker does NOT count
	// toward the v1 gate, so a (kind, kindVersion) whose exact-kindVersion worker is absent is
	// never claimed even when another kindVersion of the same kind has one. The inner map
	// is cleaned up when a kindVersion's count hits zero so subscribedKinds/breakdown
	// never report a stale phantom kindVersion.
	//
	// subscribers counts CONNECTED presence (drives the cluster view + reactor
	// readiness); readySubscribers counts only streams that are also HEALTHY for the
	// pair — a subset that shrinks when a worker sends a per-kind RS- (Interest with
	// has_worker=false: its provider's downstream is degraded) and grows back on RS+.
	// The CLAIM GATE (HasSubscriber) and the mesh presence advertisement key on
	// readySubscribers, NOT subscribers: a connected-but-degraded worker must stop the
	// broker claiming/pushing THAT kind (so it lands on a healthy pod) while still
	// listing as present. Same book-keeping discipline + per-kindVersion cleanup as
	// subscribers, so the two never drift. A worker that never sends Interest is always
	// ready → readySubscribers == subscribers (the unchanged default).
	subscribers      map[model.Kind]map[int]int
	readySubscribers map[model.Kind]map[int]int
	// configSubs is the set of live WorkStream streams, each with the kinds it
	// advertises + a buffered channel the stream loop selects on. broadcastConfig
	// pushes a changed kind's new default config to every sub advertising it — the
	// PUSH half of worker config delivery (the worker also PULLs at boot/refresh).
	configSubs map[*configSub]struct{}
	// release scoped-frees a work_queue lease (worker_id, ids, lo/hi) so an
	// abandoned task (dropped worker stream) is re-claimable in ms, not after the
	// reaper's StaleAfter. nil → rely on the reaper backstop only.
	release func(ctx context.Context, brokerID string, ids []uuid.UUID, lo, hi int16) (int64, error)
	// markWorkerBatch stamps work_queue.worker_id for a BATCH of dispatched tasks
	// (UI attribution only). The batcher goroutine coalesces per-task events off attrCh
	// and calls this once per tick — never on any fence/hot path. nil → attribution
	// disabled (UI shows the broker id).
	markWorkerBatch func(ctx context.Context, brokerID string, ids []uuid.UUID, workerIDs []string, shards []int16) error
	// brokerID is this broker's own claim identity (work_queue.worker_id), passed to
	// markWorkerBatch so the scoped UPDATE only touches rows this broker still owns.
	brokerID string
	// attest relays a worker's per-task liveness attestation (WorkHeartbeat) into the
	// dispatcher's heartbeat set: WorkStream maps the attested lease_tokens to their
	// work_ids (workIDsForTokens) and calls this so ONLY still-progressing tasks keep
	// their lease heartbeat-fresh; a silent/hung worker's tasks fall out and the reaper
	// reclaims them. Wired to dispatcher.Attest at construction; nil = no relay (a task
	// then rides its own deadline / the reaper).
	attest func([]uuid.UUID)
	// attrCh carries per-task attribution events (which worker got which task) from
	// the dispatch path to the coalescing batcher goroutine (runAttributionBatcher).
	// Buffered + non-blocking send: a full channel drops the event (the UI just keeps
	// the broker id — attribution is best-effort, never worth blocking dispatch).
	// nil when attribution is disabled.
	attrCh chan execAttr

	// mesh is the broker-to-broker MESH. nil → OFF: the executor behaves exactly as
	// a single broker (claim + serve only LOCAL work). When set, a task claimed for a
	// kind with no local worker is PUSHED (as a durable task reference) to a peer that
	// serves it. The result write is owner-independent (fenced on claim_epoch), so a
	// pushed task's completion does not route home; see relay.go + dispatchStage.
	mesh *peerMesh

	// metrics counts config-push drops (best-effort observability), off the hot path.
	metrics fanoutMetrics

	// subSeq stamps each new configSub a stable, monotonic sequence number (under
	// f.mu in addSubscriber) — the fallback identity for a legacy worker that sends
	// no worker_id, so its synthesized "worker-<n>" label is stable across beats.
	subSeq uint64

	// claimed / completed are OTel dispatch counters. They are NIL until SetMetrics
	// wires them (metrics off, or a Server built without them) — and a nil
	// metric.Int64Counter.Add PANICS, so the call sites nil-guard before Add. When
	// metrics ARE on they're non-nil no-op-or-real instruments. claimed++ when a stage
	// is dispatched to a worker; completed++ (outcome attr) when its result resolves.
	claimed   metric.Int64Counter
	completed metric.Int64Counter
}

// fanoutMetrics counts best-effort broker observability edges. Monotonic, atomic,
// read via Server accessors — never on the hot path.
type fanoutMetrics struct {
	// configPushDropped: live config/bundle PUSH frames dropped because a worker's
	// push channel was full (broadcastProviderConfig). A dropped push means that worker
	// won't see the edit until its periodic GetProviderConfig refresh (or a reconnect
	// re-pull / prime-on-subscribe) — normally ~0, so any sustained rise flags a
	// wedged/backed-up worker stream. Surfaced via Server.ConfigPushDropped.
	configPushDropped atomic.Int64
}

func newFanoutExecutor() *fanoutExecutor {
	return &fanoutExecutor{
		byKind:           make(map[model.KindVersion]chan *pendingStage),
		inflight:         make(map[string]*pendingStage),
		foreignOwner:     make(map[string]*peerClient),
		subscribers:      make(map[model.Kind]map[int]int),
		readySubscribers: make(map[model.Kind]map[int]int),
		configSubs:       make(map[*configSub]struct{}),
	}
}

// broadcastProviderConfig pushes a (kind, kindVersion)'s new default providerconfig —
// the WHOLE monolith, spec + bundle together — to every connected stream advertising
// that EXACT (kind, kindVersion). Non-blocking per stream (drop on a full buffer; the
// worker's periodic GetProviderConfig poll is the at-least-eventually backstop), logging
// a drop so a wedged stream is visible. A default is per (kind, kindVersion), so this
// matches ONLY streams serving that exact pair — the same routing-strict per-kindVersion
// distinction the WORK gate uses, so a v2 edit never lands on a v1 worker. Wired (via
// the engine) to the ProviderConfigCache's OnKindConfigChange so an operator's edit reaches the
// right-kindVersion workers at once instead of on their next poll.
func (f *fanoutExecutor) broadcastProviderConfig(kind model.Kind, kindVersion int, spec, data []byte) {
	km := model.KindVersion{Kind: kind, Version: kindVersion}
	resp := &workerpb.WorkStreamServerMsg{Body: &workerpb.WorkStreamServerMsg_Config{
		Config: &workerpb.ProviderConfigUpdate{Kind: string(kind), KindVersion: int32(kindVersion), Config: spec, Bundle: data},
	}}
	f.mu.Lock()
	subs := make([]*configSub, 0, len(f.configSubs))
	for s := range f.configSubs {
		if s.serves(km) {
			subs = append(subs, s)
		}
	}
	f.mu.Unlock()
	for _, s := range subs {
		select {
		case s.ch <- resp:
		default:
			f.metrics.configPushDropped.Add(1)
			slog.Warn("broker: providerconfig push dropped (buffer full); worker reconciles on next poll/reconnect", "kind", km.Kind, "kind_version", km.Version)
		}
	}
}

// releaseAbandoned scoped-frees the DB leases for tokens a dropped worker stream
// had SENT but not Completed, so a surviving worker re-claims them immediately.
// A token already Completed/dropped (no longer in inflight) is skipped. Best
// effort — the reaper StaleAfter is the backstop if the release errors.
func (f *fanoutExecutor) releaseAbandoned(ctx context.Context, tokens []string) {
	if f.release == nil || len(tokens) == 0 {
		return
	}
	f.mu.Lock()
	leases := make([]runtime.Lease, 0, len(tokens))
	for _, tok := range tokens {
		if ps, ok := f.inflight[tok]; ok && ps.lease.WorkID != uuid.Nil {
			leases = append(leases, ps.lease)
		}
	}
	f.mu.Unlock()
	for _, l := range leases {
		// One row at a time keeps shard bounds exact (each lease's own shard).
		_, _ = f.release(ctx, l.BrokerID, []uuid.UUID{l.WorkID}, l.ShardID, l.ShardID)
	}
}

// abandon wakes every still-parked dispatchStage whose worker stream just dropped
// before Completing its task. releaseAbandoned frees the DB lease, but the parked
// goroutine waits on its resultCh/ctx — and a no-deadline (TaskDeadline=0) task
// has no ctx timeout, so it would otherwise leak the goroutine + its pair.inFlight
// slot until pod shutdown (repeated worker flaps bleed per-kind capacity to zero).
// Deliver a synthetic TRANSIENT StageComplete (Terminal=false) per token: the
// parked dispatchStage takes its resultCh arm, drops the token, and DispatchStage
// turns the error message into a RETRYABLE failure — so dispatch records it on the
// live parent ctx, the slot frees, and the (already-released) row re-claims at
// once. The send is non-blocking (resultCh is buffered(1)); if a genuine Complete
// already won the race, this hits default and is a harmless no-op.
func (f *fanoutExecutor) abandon(tokens []string) {
	for _, tok := range tokens {
		f.mu.Lock()
		ps, ok := f.inflight[tok]
		f.mu.Unlock()
		if !ok {
			continue // already Completed/dropped
		}
		select {
		case ps.resultCh <- &workerpb.StageComplete{
			LeaseToken: tok, ErrorMessage: reasonWorkerStreamDropped, Terminal: false,
		}:
		default: // a concurrent genuine Complete already resolved it — no-op
		}
	}
}

// reasonWorkerStreamDropped is the ErrorMessage the broker stamps on a StageComplete
// it SYNTHESIZES (never from a worker) to wake a parked stage when its local worker
// stream drops before Complete. Transient (Terminal=false) so dispatch re-pends the
// still-leased row; releaseAbandoned frees the DB lease so re-claim is immediate.
const reasonWorkerStreamDropped = "converge: local worker stream dropped before Complete"

// workIDsForTokens maps a worker's attested lease_tokens to the work_queue ids of
// the LOCAL tasks (in f.inflight) those tokens hold, skipping unknown/foreign
// tokens. WorkStream calls it on a WorkHeartbeat so the dispatcher's Attest refreshes
// heartbeat_at for exactly the tasks the worker's handlers are still progressing; a
// token this broker does not own a lease for (a stale token, or a reference this
// broker forwarded to a peer) has no local work_id and is dropped.
func (f *fanoutExecutor) workIDsForTokens(tokens []string) []uuid.UUID {
	if len(tokens) == 0 {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]uuid.UUID, 0, len(tokens))
	for _, tok := range tokens {
		if ps, ok := f.inflight[tok]; ok && ps.lease.WorkID != uuid.Nil {
			ids = append(ids, ps.lease.WorkID)
		}
	}
	return ids
}

// compactSent drops the no-longer-inflight (Completed/abandoned) tokens from a
// WorkStream stream's `sent` slice, returning the compacted slice. Called only by
// the single WorkStream goroutine that owns `sent`, so it stays single-writer —
// the only shared read is f.inflight membership under f.mu. Pure memory hygiene:
// releaseAbandoned/abandon already filter by inflight, so the dropped tokens
// would never have been acted on.
func (f *fanoutExecutor) compactSent(sent []string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	kept := sent[:0]
	for _, tok := range sent {
		if _, ok := f.inflight[tok]; ok {
			kept = append(kept, tok)
		}
	}
	return kept
}

func (f *fanoutExecutor) creditForLocked(km model.KindVersion) int32 {
	var c int32
	for sub := range f.configSubs {
		// Only a READY stream contributes credit: a stream that has RS-'d km is
		// degraded for it, so advertising its slots would let a peer forward km work we'd
		// only fail. readyFor folds the serves + not-unready test.
		if !sub.readyFor(km) {
			continue
		}
		if rem := sub.maxInflight - sub.inflight; rem > 0 {
			c += int32(rem)
		}
	}
	return c
}

// advertiseCredits pushes this broker's refreshed presence+credit for each
// (kind, kindVersion) to the mesh (RS+/RS-). No-op when the mesh is off. Called on every
// edge: worker connect/drop (addSubscriber/removeSubscriber) and task
// dispatch/complete. Presence (has_worker) is the correctness signal the peer claim
// gate keys on; the credit int is a best-effort routing HINT only. STRICT: advertised
// per EXACT (kind, kindVersion), so a peer only forwards a v2 task to a v2-serving stream.
func (f *fanoutExecutor) advertiseCredits(kms []model.KindVersion) {
	if f.mesh == nil {
		return
	}
	for _, km := range kms {
		f.mu.Lock()
		c := f.creditForLocked(km)
		has := f.readySubscribers[km.Kind][km.Version] > 0 // READY presence: a healthy worker exists (maybe full)
		f.mu.Unlock()
		f.mesh.advertiseCredit(km.Kind, km.Version, c, has)
	}
}

// relayEnabled reports whether the broker-to-broker mesh is wired. When false the
// executor is byte-for-byte the mesh-off path (serve only LOCAL work).
func (f *fanoutExecutor) relayEnabled() bool { return f.mesh != nil }

// onRouteTask is the routeSink hook: peer `owner` forwarded a task REFERENCE to us
// because it has no local worker for the pair and we advertise one. We run it on a
// local worker and return the worker's StageComplete to `owner` over the SAME route
// (a `complete` frame); `owner` — which holds the parked stage + the durable write
// path + the work_queue lease — does the fenced result write. We hold NO lease for it.
//
// The foreign stage is registered in f.inflight (so our WorkStream receive loop's
// deliver resolves it when our worker Completes) with a resultCh a forwarder goroutine
// drains: on a result it sends the StageComplete home; on our ctx/mesh shutdown it just
// exits (owner's heartbeat-lapse → reaper re-claim is the backstop). No credit slot is
// consumed (we hold no lease), matching attributeToStream's foreign handling.
func (f *fanoutExecutor) onRouteTask(owner *peerClient, task *workerpb.StageTask) {
	token := task.GetLeaseToken()
	ps := &pendingStage{task: task, resultCh: make(chan *workerpb.StageComplete, 1)}
	f.mu.Lock()
	f.inflight[token] = ps
	f.foreignOwner[token] = owner
	f.mu.Unlock()

	// Forwarder: return the worker's result to the owning peer, which does the fenced
	// write. Bound to the OWNER's ctx (cancelled when that peer departs the cluster), so
	// it can't leak — if the owner is gone there is nowhere to return the result and the
	// owner's reaper re-claim is the backstop.
	ownerGone := context.Background().Done() // never-fires default when the owner carries no ctx (tests)
	if owner.ctx != nil {
		ownerGone = owner.ctx.Done()
	}
	go func() {
		select {
		case sc := <-ps.resultCh:
			// Non-blocking enqueue onto the owner's route send queue; a full/closed queue
			// drops it and the owner's reaper re-claims (at-least-once, epoch-fenced).
			select {
			case owner.out <- &meshpb.RouteFrame{Body: &meshpb.RouteFrame_Complete{Complete: sc}}:
			default:
			}
		case <-ownerGone:
		}
		f.mu.Lock()
		delete(f.foreignOwner, token)
		f.mu.Unlock()
		f.drop(token) // clear the inflight entry (foreign → frees no credit slot)
	}()

	q := f.queueFor(taskKindVer(task))
	// Non-blocking: if the local buffer is somehow full (worker gone / saturated), drop
	// the reference — never block the route receive loop (which would stall every other
	// kind's frames on this stream). The forwarding peer's reaper re-claims it.
	select {
	case q <- ps:
	default:
		f.mu.Lock()
		delete(f.inflight, token)
		delete(f.foreignOwner, token)
		f.mu.Unlock()
	}
}

// onRouteComplete resolves a task THIS broker forwarded to a peer: the peer's worker
// ran it and returned the StageComplete home over the route. We hold the parked stage
// + the work_queue lease, so deliver resolves ps.resultCh and our runtime does the
// fenced write (epoch-fenced, so a stale/dup return no-ops). An unknown token is a
// harmless map-miss.
func (f *fanoutExecutor) onRouteComplete(sc *workerpb.StageComplete) {
	f.deliver(sc.GetLeaseToken(), sc)
}

// onPeerGone wakes every parked dispatchStage this broker PUSHED to `peer` before
// that peer departed the cluster: the peer can no longer return the result home, so
// deliver a synthetic TRANSIENT StageComplete per such token. The parked dispatchStage
// takes its resultCh arm and dispatch records a retryable failure — its row (whose
// heartbeat, attested on the departed peer, has lapsed and been reaper-freed) re-claims
// at once instead of riding the RemoteFanoutCeiling. Non-blocking (resultCh is
// buffered(1)); a token a genuine RouteFrame_Complete already resolved is gone from
// inflight and skipped. Called from refreshOnce when a peer leaves.
func (f *fanoutExecutor) onPeerGone(peer *peerClient) {
	f.mu.Lock()
	var woken []chan *workerpb.StageComplete
	for _, ps := range f.inflight {
		if ps.pushedTo == peer && ps.resultCh != nil {
			woken = append(woken, ps.resultCh)
		}
	}
	f.mu.Unlock()
	for _, ch := range woken {
		select {
		case ch <- &workerpb.StageComplete{ErrorMessage: reasonPeerDeparted, Terminal: false}:
		default: // a genuine Complete already resolved it — no-op
		}
	}
}

// reasonPeerDeparted labels the synthetic transient onPeerGone delivers so a
// forwarded task whose executing peer departed re-dispatches (retryable), never fails
// terminally.
const reasonPeerDeparted = "converge-mesh: executing peer departed before returning result"

// taskKindVer is the STRICT routing key of a StageTask: its kind + its explicit
// kindVersion (>= 1, stamped by baseTask). The single place the broker reads the
// kindVersion dimension off a task, so every buffer/credit/gate lookup keys
// identically — no normalization, so a 0 keys (kind, 0) and matches no buffer.
func taskKindVer(task *workerpb.StageTask) model.KindVersion {
	return model.KindVersion{Kind: model.Kind(task.GetKind()), Version: int(task.GetKindVersion())}
}

// queueFor returns (creating if needed) the buffer channel for a (kind, kindVersion).
// STRICT routing: each (kind, kindVersion) has its OWN buffer, and only a WorkStream
// fan-in goroutine for that exact pair pops from it — a v1 task never lands in a
// buffer a v2 worker drains.
func (f *fanoutExecutor) queueFor(km model.KindVersion) chan *pendingStage {
	f.mu.Lock()
	defer f.mu.Unlock()
	q, ok := f.byKind[km]
	if !ok {
		q = make(chan *pendingStage, 1024)
		f.byKind[km] = q
	}
	return q
}

// requeue returns a still-inflight stage to its kind buffer when a worker stream
// pulled it but dropped before (or while) sending — so another worker picks it
// up. A stage whose lease already resolved is no longer inflight and is dropped.
// The send is bounded by stop (the requeuing stream's teardown signal): if the
// per-kind buffer is full at shutdown the send would otherwise block forever and
// leak the fan-in goroutine — instead we abandon the requeue and let the reaper's
// StaleAfter reclaim the lease (the same backstop releaseAbandoned documents). At
// the default per-pair fanout ceiling the buffer (sized to the in-flight ceiling)
// can't fill, so this select only matters if the ceiling is raised very high.
func (f *fanoutExecutor) requeue(ps *pendingStage, stop <-chan struct{}) {
	f.mu.Lock()
	_, live := f.inflight[ps.task.GetLeaseToken()]
	f.mu.Unlock()
	if !live {
		return
	}
	select {
	case f.queueFor(taskKindVer(ps.task)) <- ps:
	case <-stop:
	}
}
