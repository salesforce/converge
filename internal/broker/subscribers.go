package broker

import (
	"fmt"
	"sort"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/sdk-go/workerpb"
)

// subscribers.go: worker-stream subscriber management for the fanoutExecutor —
// registering/removing a connected worker's (kind,version) subscriptions, the
// readiness gate (RS+/RS-), the per-(kind,version) presence + ready counts the
// claim gate reads, and the connected-worker snapshot for the cluster view. The
// dispatch tail lives in dispatch.go; mesh reference forwarding + credit in fanout.go.

// configSub is one connected WorkStream stream's config-push channel: the
// (kind, kindVersion) pairs it advertises (so a broadcast only reaches relevant streams
// and credit is accounted per exact kindVersion) + a buffered chan the stream loop
// drains onto the wire. The channel is buffered so broadcastConfig never blocks on
// a slow/backed-up stream; on a full buffer the push is dropped (the worker's
// periodic GetProviderConfig poll is the backstop — config delivery is
// at-least-eventually, never blocking the broker's config loop).
type configSub struct {
	// kinds is the exact set of (kind, kindVersion) pairs this stream serves. Keyed on the
	// full pair (not bare kind) so credit + the claim gate stay STRICT per-kindVersion; a
	// config/bundle push (which is per-KIND — one default doc per kind) matches on the
	// Kind component alone (broadcastPush).
	kinds map[model.KindVersion]struct{}
	// unready is the subset of kinds this stream has advertised a per-kind RS- for
	// (an Interest with has_worker=false: the provider serving that (kind, kindVersion)
	// is degraded — e.g. its Kafka downstream is down). A pair in unready is still in
	// kinds (still SUBSCRIBED, still config-pushed) but does NOT count toward the ready
	// gate/credit, so the broker stops claiming + pushing it to this stream until the
	// worker clears it with an RS+ (has_worker=true). Absent/empty = fully ready (the
	// default for a worker that never sends Interest). Guarded by fanoutExecutor.mu.
	unready map[model.KindVersion]struct{}
	// peer is the connected worker's OBSERVED identity, resolved from the connection
	// in spiffeGate: peer.id is the mTLS client cert's SPIFFE ID, a trusted mesh
	// header, or the peer IP — NEVER a value the worker self-reported. peer.source /
	// peer.verified describe which, for the cluster-view badge. Feeds attribution +
	// the connected-worker view; never trusted for routing or auth (the broker owns
	// the lease/fence). peer.id empty → the snapshot synthesizes a "worker-<seq>"
	// fallback so the stream still lists.
	peer peerIdentity
	// ch carries a ready-to-send WorkStreamServerMsg — a ConfigUpdate (spec axis) OR
	// a BundleUpdate (bundle axis); the two push independently. One channel keeps
	// the stream-drain side a single select case.
	ch chan *workerpb.WorkStreamServerMsg

	// maxInflight is this worker stream's local concurrency ceiling (Subscribe.
	// max_inflight). inflight is how many tasks it currently holds (SHARED across all
	// the kinds it advertises — a worker runs at most maxInflight tasks total, not
	// per-kind). Together they give this stream's remaining slots = max(0, maxInflight
	// − inflight), which the mesh advertises as CREDIT (M1: per-stream slots, NOT a
	// per-kind subscriber count). Guarded by fanoutExecutor.mu.
	maxInflight int
	inflight    int
	// slots is the per-stream CAPACITY GATE: a buffered semaphore of maxInflight tokens
	// the stream's fan-in goroutines acquire BEFORE popping a task off the shared byKind
	// channel, and release when that task resolves. A stream whose worker is full blocks
	// on its own slots (NOT on the shared channel), so it stops competing for work and
	// other streams' fan-ins win the shared byKind pop — the work spreads to idle workers
	// instead of piling on one (over-dispatch). Sized once at addSubscriber; never resized.
	slots chan struct{}
	// seq is a stable per-stream sequence number stamped once in addSubscriber
	// (under f.mu). It's the fallback identity for a legacy worker that sends no
	// worker_id: connectedWorkers synthesizes "worker-<seq>" so the SAME stream
	// keeps the SAME label across beats (map iteration order is random, so a
	// per-snapshot counter would reshuffle labels between streams beat-to-beat).
	seq uint64
}

// serves reports whether this stream advertises the EXACT (kind, kindVersion) — the
// STRICT membership test for a per-(kind, kindVersion) config/bundle push. Defaults are
// per-kindVersion now, so a vpc/v2 edit must reach ONLY vpc/v2 workers, not a vpc/v1
// worker sharing the kind. Same strictness as the WORK gate. Caller holds f.mu (the
// map is only mutated under it).
func (s *configSub) serves(km model.KindVersion) bool {
	_, ok := s.kinds[km]
	return ok
}

// readyFor reports whether this stream both SERVES the exact (kind, kindVersion) AND
// is HEALTHY for it (not in unready) — the per-stream term the claim gate + the push
// path use to skip a degraded provider. A stream that never sent an Interest RS- has
// an empty unready set, so readyFor == serves (the unchanged default). Caller holds
// f.mu (both maps are only mutated under it).
func (s *configSub) readyFor(km model.KindVersion) bool {
	if _, ok := s.kinds[km]; !ok {
		return false
	}
	_, degraded := s.unready[km]
	return !degraded
}

// bumpKindVerCount increments the per-(kind, kindVersion) count in a kind→kindVersion→count
// map (subscribers or readySubscribers), allocating the inner map on first use. Caller
// holds f.mu. Its inverse is dropKindVerCount.
func bumpKindVerCount(m map[model.Kind]map[int]int, km model.KindVersion) {
	inner := m[km.Kind]
	if inner == nil {
		inner = make(map[int]int, 1)
		m[km.Kind] = inner
	}
	inner[km.Version]++
}

// dropKindVerCount decrements the per-(kind, kindVersion) count, cleaning up an emptied
// kindVersion entry (and an emptied kind entry) so neither map ever reports a phantom
// pair to the gate / cluster view / breakdown, and neither accretes dead kinds under
// churn. Never drops below 0. Caller holds f.mu. The inverse of bumpKindVerCount.
func dropKindVerCount(m map[model.Kind]map[int]int, km model.KindVersion) {
	inner := m[km.Kind]
	if inner == nil {
		return
	}
	if inner[km.Version] > 0 {
		inner[km.Version]--
	}
	if inner[km.Version] <= 0 {
		delete(inner, km.Version)
	}
	if len(inner) == 0 {
		delete(m, km.Kind)
	}
}

// addSubscriber registers a WorkStream stream advertising the (kind, kindVersion) pairs
// `kms` with local concurrency ceiling maxInflight: it bumps the per-(kind, kindVersion)
// presence count (the STRICT local claim-gate term) and returns a configSub the
// stream loop selects on for config pushes. It then advertises this broker's
// refreshed CREDIT for each (kind, kindVersion) to the mesh (a new worker connecting
// raises that exact kindVersion's credit → peers may push it matching work). Pair with
// removeSubscriber on stream exit.
func (f *fanoutExecutor) addSubscriber(kms []model.KindVersion, maxInflight int, peer peerIdentity) *configSub {
	if maxInflight <= 0 {
		maxInflight = 1 // a worker with no advertised ceiling still has one slot
	}
	sub := &configSub{
		kinds:       make(map[model.KindVersion]struct{}, len(kms)),
		unready:     make(map[model.KindVersion]struct{}), // starts empty: a fresh stream is ready for every kind it serves
		peer:        peer,
		ch:          make(chan *workerpb.WorkStreamServerMsg, 64), // buffered: broadcast never blocks
		maxInflight: maxInflight,
		slots:       make(chan struct{}, maxInflight), // capacity gate: maxInflight free tokens
	}
	f.mu.Lock()
	f.subSeq++
	sub.seq = f.subSeq // stable fallback identity for a stream with no observed id
	for _, km := range kms {
		bumpKindVerCount(f.subscribers, km)
		// A fresh stream is READY for every kind it advertises until it sends an RS-;
		// readySubscribers therefore mirrors subscribers at connect. A worker that never
		// sends Interest keeps the two identical — the unchanged pre-readiness behavior.
		bumpKindVerCount(f.readySubscribers, km)
		sub.kinds[km] = struct{}{}
	}
	f.configSubs[sub] = struct{}{}
	f.mu.Unlock()
	f.advertiseCredits(kms)
	return sub
}

func (f *fanoutExecutor) removeSubscriber(kms []model.KindVersion, sub *configSub) {
	f.mu.Lock()
	for _, km := range kms {
		dropKindVerCount(f.subscribers, km)
		// readySubscribers only counts a pair this stream was READY for. A pair the
		// stream had already RS-'d (in sub.unready) ALREADY decremented readySubscribers
		// when the RS- arrived — decrementing again here would underflow the count for a
		// still-ready peer stream. So drop from readySubscribers only the pairs NOT in
		// unready; the unready set is discarded with the stream.
		if _, degraded := sub.unready[km]; !degraded {
			dropKindVerCount(f.readySubscribers, km)
		}
	}
	delete(f.configSubs, sub)
	f.mu.Unlock()
	f.advertiseCredits(kms) // a worker leaving lowers that (kind, kindVersion)'s credit (maybe to 0 = RS-)
}

// setStreamReadiness applies a worker's per-kind readiness flip (a WorkStream
// Interest): has_worker=false is RS- (the provider serving km is degraded — stop
// claiming/pushing it to this stream), has_worker=true is RS+ (recovered). It moves
// km in/out of the stream's unready set and adjusts readySubscribers by ±1, then
// re-advertises credit so the mesh Interest retracts/restores in lockstep (a peer's
// anyPeerServes goes false when the last ready worker for km leaves the fleet). No-op
// (idempotent) if the stream doesn't serve km or is already in the requested state —
// so a duplicate/reordered absolute frame can't double-count.
func (f *fanoutExecutor) setStreamReadiness(sub *configSub, km model.KindVersion, ready bool) {
	f.mu.Lock()
	if _, serves := sub.kinds[km]; !serves {
		f.mu.Unlock()
		return // never subscribed for this pair — nothing to gate
	}
	_, degraded := sub.unready[km]
	switch {
	case ready && degraded: // RS+: recovered → count it ready again
		delete(sub.unready, km)
		bumpKindVerCount(f.readySubscribers, km)
	case !ready && !degraded: // RS-: degraded → stop counting it ready
		sub.unready[km] = struct{}{}
		dropKindVerCount(f.readySubscribers, km)
	default:
		f.mu.Unlock() // already in the requested state — idempotent no-op
		return
	}
	f.mu.Unlock()
	f.advertiseCredits([]model.KindVersion{km})
}

// HasSubscriber reports whether any connected worker is READY to execute the EXACT
// (kind, kindVersion) — connected AND healthy for it. The dispatcher's claim gate
// uses it so the broker never claims a (kind, kindVersion) it can't deliver: a pair
// with no live worker, OR whose only workers have all sent a per-kind RS- (their
// provider's downstream is degraded), is NOT claimed — the rows stay queued for a
// healthy pod instead of being pulled off Postgres to fail. STRICT: a v2 worker does
// NOT satisfy a v1 query — there is no wildcard/any-kindVersion match here (see
// HasAnyWorkerForKind for the coarse reactor-readiness signal that spans kindVersions).
func (f *fanoutExecutor) HasSubscriber(kind model.Kind, kindVersion int) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.readySubscribers[kind][kindVersion] > 0
}

// HasAnyWorkerForKind reports whether any connected worker advertises kind at ANY
// kindVersion — the coarse presence signal for reactor-readiness (lifecycle_outbox
// carries no kindVersion, so its gate can't be per-kindVersion). NOT used for WORK routing,
// which is strictly per (kind, kindVersion) via HasSubscriber.
func (f *fanoutExecutor) HasAnyWorkerForKind(kind model.Kind) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, n := range f.subscribers[kind] {
		if n > 0 {
			return true
		}
	}
	return false
}

// subscribedKinds returns the kinds with ≥1 connected worker (open WorkStream
// stream) at ANY kindVersion right now. Order unspecified. (The empty-inner-map cleanup
// in removeSubscriber guarantees a listed kind has a live worker on some kindVersion.)
func (f *fanoutExecutor) subscribedKinds() []model.Kind {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]model.Kind, 0, len(f.subscribers))
	for k := range f.subscribers {
		out = append(out, k)
	}
	return out
}

// SubscriberBreakdown snapshots the connected-worker counts per (kind, kindVersion):
// kind → kindVersion → number of live WorkStream streams advertising it. The host reads
// it for the ZERO-WORKER-FOR-KINDVERSION observability signal — pairing it with the set
// of kind versions a kind has resources on surfaces "kind vpc has resources on kind versions
// {1,2}, workers connected for {2}", the diagnostic for a v1 tile that will never
// drain because no v1 worker connected (STRICT routing never falls back to v2).
// A deep copy so the caller can't mutate the live maps; off the hot path.
func (f *fanoutExecutor) SubscriberBreakdown() map[model.Kind]map[int]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[model.Kind]map[int]int, len(f.subscribers))
	for k, inner := range f.subscribers {
		cp := make(map[int]int, len(inner))
		for maj, n := range inner {
			cp[maj] = n
		}
		out[k] = cp
	}
	return out
}

// connectedWorkers snapshots every live WorkStream stream as a ConnectedWorker for
// the cluster view. One row per stream (a worker that reconnects appears once
// per open stream). Kinds are sorted so the view is stable across beats; an
// empty worker_id (legacy worker) gets a synthesized fallback from the stream's
// STABLE seq ("worker-<seq>"), so the same stream keeps the same label across
// beats (f.configSubs is a map — iteration order is random, so a per-snapshot
// counter would reshuffle labels between streams). Off the hot path — read on
// the reporter's 30s beat only. Order by workerID for a stable listing.
// subscriberCount is the number of live worker streams — a cheap gauge source that
// doesn't allocate the per-worker snapshot connectedWorkers builds.
func (f *fanoutExecutor) subscriberCount() int {
	f.mu.Lock()
	n := len(f.configSubs)
	f.mu.Unlock()
	return n
}

func (f *fanoutExecutor) connectedWorkers() []ConnectedWorker {
	f.mu.Lock()
	out := make([]ConnectedWorker, 0, len(f.configSubs))
	for s := range f.configSubs {
		// One entry per (kind, kindVersion) pair the worker serves, so the cluster view
		// can show "vpc/v1", "vpc/v2" — Kinds[i] served at KindVersions[i]. Sorted by
		// (kind, kindVersion) for a stable, diff-friendly view.
		pairs := make([]model.KindVersion, 0, len(s.kinds))
		for km := range s.kinds {
			pairs = append(pairs, km)
		}
		sort.Slice(pairs, func(a, b int) bool {
			if pairs[a].Kind != pairs[b].Kind {
				return pairs[a].Kind < pairs[b].Kind
			}
			return pairs[a].Version < pairs[b].Version
		})
		kinds := make([]model.Kind, len(pairs))
		kindVersions := make([]int, len(pairs))
		for i, km := range pairs {
			kinds[i] = km.Kind
			kindVersions[i] = km.Version
		}
		id := s.peer.id
		if id == "" {
			id = fmt.Sprintf("worker-%d", s.seq) // no observed id (e.g. plaintext, no RemoteAddr); stable per-stream fallback
		}
		out = append(out, ConnectedWorker{
			WorkerID:     id,
			IDSource:     s.peer.source,
			IDVerified:   s.peer.verified,
			Kinds:        kinds,
			KindVersions: kindVersions,
			InFlight:     s.inflight, // guarded by f.mu, held here
			MaxInflight:  s.maxInflight,
		})
	}
	f.mu.Unlock()
	sort.Slice(out, func(a, b int) bool { return out[a].WorkerID < out[b].WorkerID })
	return out
}

// creditForLocked computes this broker's CURRENT remaining worker credit for the
// EXACT (kind, kindVersion): the sum over every connected stream advertising that exact
// pair of its remaining slots (max(0, maxInflight − inflight)). Remaining slots
// are per-STREAM (a worker runs at most maxInflight tasks total across the pairs it
// serves), so a stream serving both v1 and v2 contributes its remaining slots to
// EACH — the mesh push weight for a pair, advertised as Interest. Caller holds f.mu.
