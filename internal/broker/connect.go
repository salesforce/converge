package broker

import (
	"context"
	"errors"
	"io"

	"connectrpc.com/connect"

	"github.com/salesforce/converge/internal/meshpb"
	"github.com/salesforce/converge/internal/meshpb/meshpbconnect"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/sdk-go/workerpb"
	"github.com/salesforce/converge/sdk-go/workerpb/workerpbconnect"
)

// forward hands a pending stage to the shared merged channel; on stop (stream
// teardown before hand-off) it returns the stage to its origin — a LOCAL stage
// (has a lease/resultCh) goes back to its buffer via requeue so another worker gets
// it; a FORWARDED reference (from a peer, no local lease) is simply dropped (the
// forwarding peer keeps the row; its heartbeat lapses → the reaper re-issues it).
// Returns false when the fan-in goroutine should exit (stop fired).
func forward(ps *pendingStage, merged chan<- *pendingStage, stop <-chan struct{}, f *fanoutExecutor) bool {
	select {
	case merged <- ps:
		return true
	case <-stop:
		if ps.resultCh != nil { // local (this broker owns the lease): return it for another worker
			f.requeue(ps, stop)
		}
		return false
	}
}

// connectHandler implements BOTH broker Connect services over the fanout executor:
// the WorkerServiceHandler (WorkStream + GetProviderConfig, worker-facing) and the
// MeshServiceHandler (Route, broker↔broker). WorkStream streams claimed stage tasks to
// a worker, receives its StageCompletes (which resolve a lease and unblock the parked
// pipeline), and receives its WorkHeartbeat frames (per-task liveness the broker relays
// into the dispatcher's heartbeat set — a task whose worker stops attesting falls to
// the reaper). GetProviderConfig serves each kind's live default providerconfig so a
// dumb worker can load + refresh it. Route carries the mesh; a worker never dials
// MeshService, so it cannot reach it.
type connectHandler struct {
	fanout  *fanoutExecutor
	configs ConfigReader // live default-config source; nil-safe (serves nothing)
}

var (
	_ workerpbconnect.WorkerServiceHandler = (*connectHandler)(nil)
	_ meshpbconnect.MeshServiceHandler     = (*connectHandler)(nil)
)

// sentCompactAt is how large a WorkStream stream's `sent` token slice may grow
// before it's compacted against f.inflight (dropping Completed tokens). Above the
// per-kind in-flight ceiling so compaction is rare on a healthy stream, low
// enough to bound memory on a long-lived high-volume connection at scale.
const sentCompactAt = 4096

// GetProviderConfig returns each requested (kind, kindVersion)'s CURRENT default
// providerconfig document AND bundle, read LIVE from the broker's ProviderConfigCache so a
// worker's periodic refresh observes an operator's edit. Defaults are per
// (kind, kindVersion): a worker asking for vpc/v1 AND vpc/v2 gets each kindVersion's OWN doc.
// The request carries a PARALLEL kind_versions array (mirrors Subscribe); a SHORTER
// (or absent) one — an old client that predates versioning — defaults its missing
// entries to kindVersion 1 (v1). A (kind, kindVersion) with no default doc AND no bundle (or no
// config source wired) is omitted from the entries.
func (h *connectHandler) GetProviderConfig(_ context.Context, req *connect.Request[workerpb.GetProviderConfigRequest]) (*connect.Response[workerpb.GetProviderConfigResponse], error) {
	out := &workerpb.GetProviderConfigResponse{}
	if h.configs != nil {
		kinds := req.Msg.GetKinds()
		kindVersions := req.Msg.GetKindVersions()
		out.Entries = make([]*workerpb.ProviderConfigEntry, 0, len(kinds))
		for i, k := range kinds {
			kindVersion := 1 // default for a client that sent no (or fewer) kind versions
			if i < len(kindVersions) && kindVersions[i] > 0 {
				kindVersion = int(kindVersions[i])
			}
			kind := model.Kind(k)
			doc := h.configs.Config(kind, kindVersion)
			b := h.configs.Bundle(kind, kindVersion)
			if len(doc) == 0 && len(b) == 0 {
				continue // this (kind, kindVersion) has neither a default doc nor a bundle
			}
			out.Entries = append(out.Entries, &workerpb.ProviderConfigEntry{
				Kind: k, KindVersion: int32(kindVersion), Config: doc, Bundle: b,
			})
		}
	}
	return connect.NewResponse(out), nil
}

// parseSubscription turns a worker's Subscribe into the exact (kind, kindVersion) pair
// set it serves. kind_versions is a PARALLEL array — kinds[i] is served at
// kind_versions[i] (a worker serving vpc/v1 + vpc/v2 sends kinds=["vpc","vpc"],
// kind_versions=[1,2]). A SHORTER (or absent) kind_versions — an old client that
// predates versioning — defaults its missing entries to kindVersion 1, so a legacy
// subscriber transparently serves v1 of each kind. There is NO wildcard: a worker
// serves ONLY the exact pairs it lists. An empty kinds set is a protocol error.
func parseSubscription(subMsg *workerpb.Subscribe) ([]model.KindVersion, error) {
	kinds := subMsg.GetKinds()
	if len(kinds) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("WorkStream: no kinds advertised"))
	}
	kindVersions := subMsg.GetKindVersions()
	kms := make([]model.KindVersion, len(kinds))
	for i, k := range kinds {
		kindVersion := 1 // default for a client that sent no (or fewer) kind versions
		if i < len(kindVersions) && kindVersions[i] > 0 {
			kindVersion = int(kindVersions[i])
		}
		kms[i] = model.KindVersion{Kind: model.Kind(k), Version: kindVersion}
	}
	return kms, nil
}

// primeConfigs synchronously sends the just-subscribed worker each advertised
// (kind, kindVersion)'s CURRENT default config + bundle BEFORE the send loop can stream it
// any task. The worker's boot GetProviderConfig can race the default landing (or
// answer from a still-cold broker), and the async broadcast only carries FUTURE
// changes — so without this snapshot a worker that connects after a default is applied
// has an empty cache for that pair until its 5-min refresh, and a config/bundle-
// bootstrapped composer claimed in that window terminal-fails "no bundle yet" before
// the refresh lands. Sending here — the send loop hasn't started — makes these frames
// precede every StageTask. Best-effort: a send error drops the stream and the worker
// reconnects (and re-primes); a pair with no default is skipped. Defaults are PER
// (kind, kindVersion), so prime each advertised pair with the WHOLE providerconfig in one
// ProviderConfigUpdate (spec + data together), mirroring the live push + the pull.
func (h *connectHandler) primeConfigs(stream *connect.BidiStream[workerpb.WorkStreamClientMsg, workerpb.WorkStreamServerMsg], kms []model.KindVersion) error {
	if h.configs == nil {
		return nil
	}
	for _, km := range kms {
		spec := h.configs.Config(km.Kind, km.Version)
		data := h.configs.Bundle(km.Kind, km.Version)
		if len(spec) == 0 && len(data) == 0 {
			continue // no default for this pair — nothing to prime
		}
		if err := stream.Send(&workerpb.WorkStreamServerMsg{
			Body: &workerpb.WorkStreamServerMsg_Config{Config: &workerpb.ProviderConfigUpdate{
				Kind: string(km.Kind), KindVersion: int32(km.Version), Config: spec, Bundle: data,
			}},
		}); err != nil {
			return err
		}
	}
	return nil
}

// WorkStream is the BIDIRECTIONAL worker↔broker work channel. The worker's FIRST
// message is a Subscribe (kinds + concurrency); the broker then streams claimed
// STAGE TASKS (+ live config/bundle pushes) DOWN, while the worker sends its
// StageCompletes UP the SAME stream. Carrying the completion on this pinned stream
// — not a unary RPC — is what guarantees it reaches THIS broker (the lease holder)
// rather than being LB-misrouted to a peer that would drop it.
// On exit it returns any stage pulled-but-not-sent to its buffer so another worker
// picks it up (no work is lost when a stream drops mid-forward).
func (h *connectHandler) WorkStream(ctx context.Context, stream *connect.BidiStream[workerpb.WorkStreamClientMsg, workerpb.WorkStreamServerMsg]) error {
	// FIRST message MUST be a Subscribe — it opens the session (advertised kinds +
	// concurrency + friendly id). Anything else is a protocol error.
	first, err := stream.Receive()
	if err != nil {
		return err // client hung up before subscribing
	}
	subMsg := first.GetSubscribe()
	if subMsg == nil {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("WorkStream: first message must be Subscribe"))
	}
	kms, err := parseSubscription(subMsg)
	if err != nil {
		return err
	}

	// The worker's identity is what the broker OBSERVED about the connection (resolved
	// in spiffeGate, carried on ctx) — the mTLS client cert's SPIFFE ID, a trusted
	// service-mesh header, or the peer IP — NEVER a value the worker self-reported (the
	// Subscribe message carries no id; the broker never trusts a client-supplied one).
	// This feeds the connected-worker cluster view + the "running on <worker>"
	// attribution; the lease/fence use the broker's own id, not this.
	peer := peerIdentityFrom(ctx)

	// Mark this stream a live consumer of its (kind, kindVersion) pairs so the dispatcher's
	// ClaimGate lets the broker pull their work; unmark on disconnect so a
	// (kind, kindVersion) with no remaining worker stops being claimed (G-BUFFER-STARVATION).
	// addSubscriber also records this stream's max_inflight (its remaining slots become
	// the CREDIT the mesh advertises to peers, per exact kindVersion) and returns the sub
	// carrying its config-push channel (broadcastConfig targets it, per kind).
	sub := h.fanout.addSubscriber(kms, int(subMsg.GetMaxInflight()), peer)
	defer h.fanout.removeSubscriber(kms, sub)

	// RESUMED TASKS: a reconnecting worker lists the tasks it was still executing when
	// its previous stream dropped (Subscribe.resumed = {lease_token, claim_epoch}). Adopt
	// their liveness immediately so THIS broker's heartbeat keeps them fresh from the
	// first beat, instead of waiting for the first WorkHeartbeat frame (which could lapse
	// them into the reaper's window). Best-effort: a resumed token maps to a work_id only
	// if THIS broker now owns that lease (e.g. it re-claimed the migrated row); a token
	// this broker holds no lease for (still owned elsewhere) yields no work_id and is
	// silently ignored. The (generation, claim_epoch) fence still guards the eventual
	// result write, so a mis-adopted attestation can only extend a lease this broker
	// legitimately holds.
	if resumed := subMsg.GetResumed(); len(resumed) > 0 && h.fanout.attest != nil {
		toks := make([]string, 0, len(resumed))
		for _, rt := range resumed {
			toks = append(toks, rt.GetLeaseToken())
		}
		h.fanout.attest(h.fanout.workIDsForTokens(toks))
	}

	// PRIME the just-subscribed worker with each advertised (kind, kindVersion)'s CURRENT
	// default config + bundle, synchronously, BEFORE the fan-in loop below can stream it
	// any task. The worker's boot GetProviderConfig can race the default landing (or
	// answer from a still-cold broker), and the async broadcast only carries FUTURE
	// changes — so without this snapshot a worker that connects (or reconnects) after a
	// default is applied has an empty cache for that (kind, kindVersion) until its 5-min
	// refresh, and a config/bundle-bootstrapped composer (stdstarlark/celbom/tfpolicy/…)
	// claimed in that window terminal-fails "no bundle yet" before the refresh lands.
	// Sending here — the send loop hasn't started, so these frames precede every
	// StageTask — closes that window at the source. Best-effort: a send error drops the
	// stream and the worker reconnects (and re-primes); a (kind, kindVersion) with no default is
	// simply skipped. This is the server-side half of the worker's re-Load-on-connect
	// (belt and braces). Defaults are PER (kind, kindVersion), so prime each advertised pair —
	// a worker serving vpc/v1 + vpc/v2 is primed with BOTH kind versions' distinct docs (kms is
	// already the exact pair set, one entry per advertised (kind, kindVersion)).
	if err := h.primeConfigs(stream, kms); err != nil {
		return err
	}

	// Merge the advertised (kind, kindVersion) buffers into one receive loop. A small
	// fan-in goroutine per (kind, kindVersion) blocks on THAT exact buffer and forwards
	// into a shared channel; the loop sends each to the stream. requeue puts back a
	// stage pulled but not yet sent. STRICT routing falls out of the buffer keying: a
	// v2-only worker spawns a fan-in only for (kind, 2), so it can never pop a v1 task.
	//
	// The fan-in does NOT poll peers. A broker that claims work for a (kind, kindVersion) it
	// has no local worker for FORWARDS the durable task reference (dispatchStage →
	// mesh.pushTask) onto a serving peer's route; that peer's Route handler enqueues it
	// into ITS local `byKind` buffer for the exact (kind, kindVersion)
	// (fanoutExecutor.onRouteTask), where this same fan-in loop pops it and streams it to
	// a worker — indistinguishable from a locally-claimed task except that the executing
	// broker holds no lease for it (the forwarding peer keeps the row + the
	// owner-independent epoch fence). So the fan-in simply blocks on the local buffer and
	// forwards, whether the mesh is on or off.
	merged := make(chan *pendingStage)
	stop := make(chan struct{})
	defer close(stop)
	for _, km := range kms {
		q := h.fanout.queueFor(km)
		go func(q chan *pendingStage) {
			for {
				select {
				case ps := <-q:
					if !forward(ps, merged, stop, h.fanout) {
						return
					}
				case <-stop:
					return
				}
			}
		}(q)
	}

	// Tokens this stream SENT to the worker but hasn't seen Completed. On stream
	// exit (worker gone), those leases are abandoned — scoped-release them so a
	// surviving worker re-claims in ms instead of after the reaper's StaleAfter
	// (G-LEASE-RELEASE). A token that Completes is removed by Complete (drop), so
	// releaseAbandoned only frees the genuinely-stuck ones.
	var sent []string
	defer func() {
		// CLEAN DROP fast path: wake the parked dispatchStage goroutines FIRST (frees the
		// in-memory slot + goroutine for every no-deadline task this dropped worker held),
		// THEN free their DB leases for fast re-claim. Both are needed: releaseAbandoned
		// alone leaves the goroutine parked forever (the leak); abandon alone leaves the
		// row claimed until the reaper. Both filter by f.inflight, so a FORWARDED
		// reference (this broker holds no lease for it) is naturally skipped — its row
		// lives on the forwarding peer and rides the heartbeat-lapse → reaper path. This
		// clean-drop path stays ~1ms; the worker-attested heartbeat window is the backstop
		// only for a SILENT/hung worker whose stream never drops.
		h.fanout.abandon(sent)
		h.fanout.releaseAbandoned(context.WithoutCancel(ctx), sent)
	}()

	// RECEIVE LOOP (worker → broker): read StageCompletes off the stream and resolve
	// them LOCALLY (deliver). Because the stream is pinned to THIS
	// broker pod through the LB, a Complete arriving here is guaranteed to be for a
	// lease THIS broker holds — fixing the unary-Complete LB-misroute that dropped
	// results at the wrong broker. Runs in its own goroutine (a bidi stream's Receive
	// must have a single caller); a Receive error (worker gone / stream drop) signals
	// the send loop to exit via recvDone, which triggers the deferred abandon.
	recvDone := make(chan error, 1)
	go func() {
		for {
			msg, err := stream.Receive()
			if err != nil {
				recvDone <- err // io.EOF on a clean close, or a transport error
				return
			}
			switch b := msg.GetBody().(type) {
			case *workerpb.WorkStreamClientMsg_Complete:
				// The worker finished a stage. Resolve the parked lease on THIS broker.
				h.fanout.deliver(b.Complete.GetLeaseToken(), b.Complete)
			case *workerpb.WorkStreamClientMsg_Heartbeat:
				// WORKER-ATTESTED LIVENESS: the worker lists the lease_tokens of the tasks
				// its handlers are STILL progressing (an empty frame = none). Relay only
				// these into the dispatcher's heartbeat set (Attest), so ONLY still-running
				// tasks keep their lease fresh — a silent/hung worker's task stops being
				// attested, its heartbeat_at goes stale, and the reaper reclaims it (a live
				// TCP connection no longer masks a wedged handler). Map each attested token
				// to its LOCAL work_id (foreign/unknown tokens skipped) so this broker only
				// refreshes leases it actually holds; the claim_epoch[] echo is carried for
				// the fence but the heartbeat scopes by work_id.
				if h.fanout.attest != nil {
					h.fanout.attest(h.fanout.workIDsForTokens(b.Heartbeat.GetLeaseToken()))
				}
			case *workerpb.WorkStreamClientMsg_Interest:
				// A per-kind readiness flip: the worker's provider for this
				// (kind, kindVersion) went degraded (has_worker=false, RS-) or recovered
				// (has_worker=true, RS+). Flip this stream's ready state for the pair so the
				// broker stops (or resumes) claiming + pushing it — and, via the mesh
				// re-advertisement inside setStreamReadiness, so peers do too. credit is
				// unused on the worker path (presence-only; the broker never trusts a
				// worker's self-reported slots).
				in := b.Interest
				h.fanout.setStreamReadiness(sub, model.KindVersion{Kind: model.Kind(in.GetKind()), Version: int(in.GetKindVersion())}, in.GetHasWorker())
			case *workerpb.WorkStreamClientMsg_Subscribe:
				// A second Subscribe is a protocol error; ignore (don't re-open).
			}
		}
	}()

	// LIVENESS is worker-attested (the worker sends WorkHeartbeat frames UP, relayed
	// into the dispatcher's heartbeat set), so the broker sends no down-keepalive: a
	// task whose worker stops attesting (silent/hung, even on a live TCP connection)
	// simply stops being heartbeated and the reaper reclaims it. A CLEAN stream drop is
	// still surfaced promptly by recvDone below (→ the deferred abandon/releaseAbandoned
	// fast path), so the down direction being send-only costs nothing.
	// CAPACITY GATE: hold at most one un-consumed slot token across the loop. The main
	// loop is a SINGLE goroutine, so it acquires exactly one slot then reads exactly one
	// task from merged — never speculatively hoarding slots (the per-kind-fan-in design
	// that starved a worker serving more kinds than it has slots). While the worker is
	// full (all maxInflight slots held by in-flight tasks) the acquire blocks in the
	// select below, so the main loop stops pulling from merged — and because the per-kind
	// fan-ins hand off to merged over an UNBUFFERED channel, they park too, leaving the
	// tasks in the shared byKind buffer for another (idle) worker's stream to pop. The
	// slot is returned when the task resolves (drop/deliver via releaseSlot) or on any
	// non-send exit below.
	haveSlot := false
	defer func() {
		if haveSlot { // exiting while holding an un-consumed slot — return it
			select {
			case <-sub.slots:
			default:
			}
		}
	}()
	for {
		// Acquire a slot before we're willing to take a task. Stay responsive to
		// disconnect + config pushes while blocked on a full worker (else a full worker
		// couldn't receive a live config update or notice its stream dropped).
		if !haveSlot {
			select {
			case <-ctx.Done():
				return nil
			case err := <-recvDone:
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			case upd := <-sub.ch:
				if err := stream.Send(upd); err != nil {
					return err
				}
				continue
			case sub.slots <- struct{}{}:
				haveSlot = true
			}
		}
		select {
		case <-ctx.Done():
			return nil // worker disconnected; release the sent-but-unacked leases (deferred)
		case err := <-recvDone:
			// The worker→broker half closed (worker gone / stream drop). Treat a clean
			// EOF as a normal disconnect; either way the deferred abandon frees leases.
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case upd := <-sub.ch:
			// A pushed config or bundle update for one of this worker's kinds (the
			// channel carries the ready-to-send WorkStreamServerMsg). Best-effort: a
			// failed send drops the stream (the worker reconnects + re-pulls both).
			if err := stream.Send(upd); err != nil {
				return err
			}
		case ps := <-merged:
			// This task consumes the slot we hold; ownership transfers to the task's
			// in-flight record, freed by drop/deliver via sub.slots. Requeue/skip paths
			// below RETURN the slot (set haveSlot=false only when the task takes it).
			// READINESS GUARD: this stream may have gone per-kind RS- for the task's
			// (kind, kindVersion) AFTER the task was buffered — its provider's downstream
			// (e.g. Kafka) degraded in the race window between claim and delivery. Sending
			// it here would just fail. Bounce it back to the buffer so a healthy worker or
			// peer takes it (the lease is untouched — the task is not lost, and it is NOT
			// terminal: a health outage must never poison-pill a resource). If no ready
			// worker exists anywhere, the claim gate (HasSubscriber → readySubscribers) has
			// already stopped NEW claims, so the buffer drains down and the rows stay queued
			// for recovery.
			if !sub.readyFor(taskKindVer(ps.task)) {
				// Not sent — keep our slot (haveSlot stays true) for the next task.
				h.fanout.requeue(ps, stop)
				continue
			}
			// Attribute this task to THIS stream's credit BEFORE the send: it now
			// occupies one of the stream's slots, lowering the kind's advertised credit
			// hint so peers stop over-forwarding. Freed in fanout.drop on Complete/abandon/
			// deadline. Attribute pre-send so a concurrent credit read never sees the
			// slot as free while the task is on the wire.
			//
			// If attribution reports the LOCAL token already resolved (a deadline/drain
			// drop freed it while it sat in the buffer), the stage is dead — do NOT send it
			// to the worker (the lease is gone; a Complete would no-op and we must not
			// consume a slot we can't free). Skip to the next. A FORWARDED reference is
			// always attributed (attributeToStream returns true without touching a slot).
			if !h.fanout.attributeToStream(ps, sub) {
				// Token already resolved while buffered — not sent; keep our slot.
				continue
			}
			// The task now OWNS the capacity-gate slot we hold: record the owning stream
			// on the stage so drop/deliver return the token to sub.slots when it resolves
			// (works for a foreign/mesh reference too, which slotHeld does not cover). Stop
			// holding the slot in the loop; go acquire a fresh one for the next task.
			h.fanout.claimGateSlot(ps, sub)
			haveSlot = false
			if err := stream.Send(&workerpb.WorkStreamServerMsg{
				Body: &workerpb.WorkStreamServerMsg_Task{Task: ps.task},
			}); err != nil {
				// Send failed (stream broken): the stage was NOT delivered. Un-attribute
				// frees the slot (via sub.slots) + returns it to its buffer for another
				// worker; the deferred abandon handles the rest.
				h.fanout.unattributeFromStream(ps)
				h.fanout.requeue(ps, stop)
				return err
			}
			sent = append(sent, ps.task.GetLeaseToken())
			// Keep `sent` bounded on a long-lived high-volume stream: a Completed token
			// is gone from f.inflight, so once it grows past a cadence drop the
			// no-longer-inflight ones (releaseAbandoned/abandon both filter by inflight,
			// so pruning them changes nothing). In-goroutine only — `sent` stays
			// single-writer; the only shared read is f.inflight under f.mu in compactSent.
			if len(sent) >= sentCompactAt {
				sent = h.fanout.compactSent(sent)
			}
		}
	}
}

// Route (broker-to-broker) is the SERVER side of the persistent mesh link: a peer
// dialed us. We run the mesh's accept loop over the bidi stream — receiving the peer's
// interest/presence and any task references it FORWARDS to us to execute, and (via the
// peer's own send goroutine on our dial-side route to it) forwarding our references +
// interest back. The claiming broker keeps the row; the result write is fenced on the
// owner-independent claim_epoch, so a forwarded reference needs no home-route hop.
// No-op (stream closes) when the mesh is off. See peerMesh.AcceptRoute.
func (h *connectHandler) Route(ctx context.Context, stream *connect.BidiStream[meshpb.RouteFrame, meshpb.RouteFrame]) error {
	if h.fanout.mesh == nil {
		return nil // mesh off: nothing to serve
	}
	return h.fanout.mesh.AcceptRoute(ctx, stream)
}
