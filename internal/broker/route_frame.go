package broker

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/salesforce/converge/internal/meshpb"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/sdk-go/workerpb"
)

// route_frame.go: the broker↔broker Route bidi-stream machinery — dialing a peer
// (runRoute/serveRoute), accepting a peer's dial (AcceptRoute), the shared
// send+receive pump both sides run, and applying an inbound frame. The peer set +
// credit/push logic that DRIVE these live in relay.go; the peerClient state in
// peer.go.

// runRoute maintains the persistent outbound Route stream to peer p: dial, send
// RouteHello + a bulk Interest for every kind we currently have credit for, then
// run the send goroutine (drains p.out) and the receive loop (inbound Interest +
// pushed tasks). Reconnects with backoff on drop until the PEER's ctx is cancelled
// (peer departs cluster_members / changes address) or the mesh shuts down — so a
// dead peer's dial loop stops promptly under churn instead of redialing forever.
func (m *peerMesh) runRoute(p *peerClient) {
	backoff := routeReconnectMin
	for {
		if p.ctx.Err() != nil {
			return
		}
		err := m.serveRoute(p)
		if p.ctx.Err() != nil {
			return
		}
		slog.Debug("mesh: route dropped; reconnecting", "peer", p.memberID, "err", err, "backoff", backoff)
		select {
		case <-time.After(jitterDur(backoff)):
		case <-p.ctx.Done():
			return
		}
		if backoff < routeReconnectMax {
			backoff *= 2
		}
	}
}

// serveRoute opens one Route bidi stream to p and runs it until it drops. It sends
// Hello + the bulk interest set, then splits into a send goroutine (p.out → wire)
// and this goroutine's receive loop (wire → sink/credit). Returns the terminating
// error so runRoute can reconnect. Bound to the peer's ctx (cancelled on departure).
func (m *peerMesh) serveRoute(p *peerClient) error {
	streamCtx, cancel := context.WithCancel(p.ctx)
	defer cancel()
	stream := p.client.Route(streamCtx)
	return m.pump(streamCtx, cancel, stream, p)
}

// AcceptRoute is the SERVER side of the Route RPC: a peer dialed us. We read its
// Hello to resolve which peerClient this stream belongs to, then run the SAME pump
// as the dialer — because a Route stream is BIDIRECTIONAL: this accepted stream is
// the single link for the pair, so we both RECEIVE the dialer's frames AND SEND our
// own interest/tasks to it over the same stream (the acceptor never dials its own).
// Blocks until the stream drops. Called from connectHandler.Route.
func (m *peerMesh) AcceptRoute(ctx context.Context, stream routeBidi) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Bind the accepted (server-side) stream to the mesh lifetime as well as the
	// request lifetime: a mesh shutdown (m.ctx cancelled — the broker is stopping)
	// must tear this handler down, otherwise the receive parked in the transport read
	// blocks until the dialer half-closes and the handler never returns — pinning the
	// listener connection past shutdown. The dialer side already derives its stream ctx
	// from m.ctx (via p.ctx); this gives the acceptor the symmetric bound. A watcher
	// goroutine cancels streamCtx when either ctx fires; it exits with the stream.
	if m.ctx != nil {
		meshCtx := m.ctx
		watchDone := make(chan struct{})
		defer close(watchDone)
		go func() {
			select {
			case <-meshCtx.Done():
				cancel()
			case <-watchDone:
			}
		}()
	}
	// First frame is the dialer's Hello → resolve the peerClient (for credit + its
	// out queue). A discovery race (peer not yet in our snapshot) is tolerated by a
	// short wait: the dialer only exists in cluster_members if we'll discover it too.
	first, err := stream.Receive()
	if err != nil {
		return err
	}
	peerID := first.GetHello().GetMemberId()
	var p *peerClient
	for i := 0; i < 50 && p == nil; i++ { // ≤ ~1s for our discovery to catch up
		if p = m.loadPeers()[peerID]; p != nil {
			break
		}
		select {
		case <-time.After(20 * time.Millisecond):
		case <-streamCtx.Done():
			return streamCtx.Err()
		}
	}
	if p == nil {
		return nil // unknown/departed peer: close the stream, dialer reconnects
	}
	m.handleInbound(p, first) // in case it wasn't a Hello (tolerant)
	return m.pump(streamCtx, cancel, stream, p)
}

// pump runs BOTH directions of a Route stream for peer p until it drops: a send
// goroutine drains p.out (forwarded task references + coalesced interest), and this
// goroutine's receive loop applies inbound frames (peer presence/credit + references
// forwarded to us). Used by both the dialer (serveRoute) and the acceptor
// (AcceptRoute) — the stream is the one bidirectional link for the pair. Sends the
// bulk interest set first (NATS sendSubsToRoute) so the peer can forward us matching
// work immediately. cancel tears the stream down on either side's failure.
func (m *peerMesh) pump(streamCtx context.Context, cancel context.CancelFunc, stream routeBidi, p *peerClient) error {
	// Hello (identity) + bulk interest for every kind we currently serve, so the peer
	// can forward us matching work immediately (NATS sendSubsToRoute). We send presence
	// (has_worker) even at credit 0 so the peer's claim gate opens.
	_ = stream.Send(&meshpb.RouteFrame{Body: &meshpb.RouteFrame_Hello{Hello: &meshpb.RouteHello{MemberId: m.selfID}}})
	for km, credit := range m.snapshotSelfCredit() {
		_ = stream.Send(&meshpb.RouteFrame{Body: &meshpb.RouteFrame_Interest{Interest: &workerpb.Interest{Kind: string(km.Kind), KindVersion: int32(km.Version), Credit: credit, HasWorker: true}}})
	}

	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		for {
			select {
			case <-streamCtx.Done():
				return
			case fr := <-p.out:
				if err := stream.Send(fr); err != nil {
					cancel()
					return
				}
			}
		}
	}()

	// Receive runs in its own goroutine so the loop can also watch streamCtx: a
	// connect Receive parked in the transport read does NOT return on ctx cancel by
	// itself, so on teardown (peer departs / mesh shuts down) we select on
	// streamCtx.Done() to stop promptly instead of blocking until the peer closes. The
	// orphaned Receive goroutine unblocks and exits when the stream is torn down by the
	// deferred cancel in serveRoute/AcceptRoute.
	type recv struct {
		fr  *meshpb.RouteFrame
		err error
	}
	recvCh := make(chan recv, 1)
	go func() {
		for {
			fr, err := stream.Receive()
			// Select on streamCtx too: if the main loop already took its
			// streamCtx.Done() arm (teardown) it will NOT drain recvCh, so a bare
			// blocking send into this buffer-1 channel would park this goroutine
			// forever once a frame is already buffered — leaking it plus the stream
			// state on every teardown that races an inbound frame. Exiting on
			// streamCtx.Done() lets it unwind cleanly.
			select {
			case recvCh <- recv{fr, err}:
			case <-streamCtx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-streamCtx.Done():
			cancel()
			<-sendDone
			return streamCtx.Err()
		case r := <-recvCh:
			if r.err != nil {
				cancel()
				<-sendDone
				return r.err
			}
			m.handleInbound(p, r.fr)
		}
	}
}

// handleInbound applies one received RouteFrame from peer p: Interest updates the
// peer's advertised presence/credit; a forwarded Task reference is handed to the sink
// (executed on our local worker); Hello is an identity no-op.
func (m *peerMesh) handleInbound(p *peerClient, fr *meshpb.RouteFrame) {
	switch b := fr.GetBody().(type) {
	case *meshpb.RouteFrame_Interest:
		km := model.KindVersion{Kind: model.Kind(b.Interest.GetKind()), Version: int(b.Interest.GetKindVersion())}
		p.setInterest(km, b.Interest.GetCredit(), b.Interest.GetHasWorker())
	case *meshpb.RouteFrame_Task:
		if m.sink != nil {
			m.sink.onRouteTask(p, b.Task)
		}
	case *meshpb.RouteFrame_Complete:
		// A peer we forwarded a task to returned its worker's result. We hold the
		// parked stage + the lease, so resolve it and let our runtime do the fenced write.
		if m.sink != nil {
			m.sink.onRouteComplete(b.Complete)
		}
	case *meshpb.RouteFrame_Hello:
		// identity — no-op
	}
}

// routeBidi is the minimal bidi-stream surface AcceptRoute needs, satisfied by
// *connect.BidiStream[RouteFrame, RouteFrame] — kept as an interface for testing.
type routeBidi interface {
	Receive() (*meshpb.RouteFrame, error)
	Send(*meshpb.RouteFrame) error
}

// jitterDur applies ±25% jitter to d so reconnecting routes don't resynchronize.
func jitterDur(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	delta := (rand.Float64()*0.5 - 0.25) * float64(d)
	return time.Duration(float64(d) + delta)
}
