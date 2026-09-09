package broker

import (
	"context"
	"sync"

	"github.com/salesforce/converge/internal/meshpb"
	"github.com/salesforce/converge/internal/meshpb/meshpbconnect"
	"github.com/salesforce/converge/internal/model"
)

// peer.go: one peer broker in the mesh — its dial client + per-peer goroutine
// lifetime, and its last-advertised per-(kind,version) presence/credit hint. The
// mesh lifecycle + routing that USE a peerClient live in relay.go / route_frame.go.

// peerClient is one peer broker: its dial address + MeshService client (for the
// Route stream), its persistent Route stream's outbound frame queue, and its
// last-advertised per-kind presence/credit (the pushTask routing hint).
type peerClient struct {
	memberID string
	addr     string
	client   meshpbconnect.MeshServiceClient

	// ctx/cancel bound this peer's goroutines (route dial/pump) to the PEER's
	// lifetime, not the mesh's: refreshOnce cancels it when the peer leaves
	// cluster_members (or changes address), so a departed peer's reconnect loop stops
	// promptly instead of leaking + forever-redialing a dead address under churn (HPA
	// flap / rolling restarts). Derived from the mesh ctx.
	ctx    context.Context
	cancel context.CancelFunc

	// out is the bounded outbound queue drained by this route's single send
	// goroutine. pushTask/advertiseCredit enqueue non-blockingly.
	out chan *meshpb.RouteFrame

	// credit is the peer's last-advertised remaining FREE worker slots per
	// (kind, kindVersion) (a best-effort routing HINT — pushTask weights by it), and
	// serves is whether it has ANY worker for that exact (kind, kindVersion) (presence,
	// RS+/RS-). serves is the CORRECTNESS signal: the claim gate keys on it, so a
	// worker-less broker keeps claiming while a peer's worker for the EXACT kindVersion
	// exists even if momentarily full. Both from the peer's inbound Interest frames.
	// STRICT: keyed on (kind, kindVersion), so a peer serving only vpc/v2 never advertises
	// interest for a vpc/v1 claimer's tile. Under cmu — tiny maps, off the DB path.
	cmu    sync.Mutex
	credit map[model.KindVersion]int32
	serves map[model.KindVersion]bool
}

func (p *peerClient) getCredit(km model.KindVersion) int32 {
	p.cmu.Lock()
	defer p.cmu.Unlock()
	return p.credit[km]
}

// servesKindVersion reports whether the peer has ANY worker for the EXACT
// (kind, kindVersion) (presence) — the claim-gate signal, distinct from free
// credit. STRICT: a peer serving vpc/v2 does not satisfy a vpc/v1 query.
func (p *peerClient) servesKindVersion(km model.KindVersion) bool {
	p.cmu.Lock()
	defer p.cmu.Unlock()
	return p.serves[km]
}

// setInterest applies an inbound Interest frame for one (kind, kindVersion): credit =
// free slots (0 = full), hasWorker = presence (RS+/RS-). Keeps a 0-credit entry
// when the peer still has a worker, so the claim gate stays open while the worker
// is merely busy. Keyed on the exact (kind, kindVersion) so per-kindVersion routing stays strict.
func (p *peerClient) setInterest(km model.KindVersion, credit int32, hasWorker bool) {
	p.cmu.Lock()
	if credit <= 0 {
		delete(p.credit, km)
	} else {
		p.credit[km] = credit
	}
	if hasWorker {
		p.serves[km] = true
	} else {
		delete(p.serves, km)
	}
	p.cmu.Unlock()
}

// decCredit optimistically drops one free-credit for (kind, kindVersion) after a
// forward, so a burst of claims doesn't oversubscribe one peer's HINT before its next
// authoritative Interest frame lands. Presence (serves) is untouched. Corrected by the
// peer's next Interest.
func (p *peerClient) decCredit(km model.KindVersion) {
	p.cmu.Lock()
	if p.credit[km] > 0 {
		p.credit[km]--
		if p.credit[km] == 0 {
			delete(p.credit, km)
		}
	}
	p.cmu.Unlock()
}
