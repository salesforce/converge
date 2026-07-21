package broker

import (
	"context"
	"crypto/x509"
	"net"
	"net/http"
	"strings"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

// peerIdentity is the broker's OBSERVED identity for a connected client (worker or
// peer broker), resolved from the connection itself at the transport edge (the
// spiffeGate) and carried on the request context into the WorkStream handler.
//
// It is derived ONLY from what the broker can observe about the connection — the
// client's mTLS cert, a trusted mesh header, or the peer IP — NEVER from a value
// the client self-reports (a client can send garbage). This feeds the "running on
// <worker>" attribution (work_queue.worker_id, display-only — never fenced/reaped)
// and the connected-worker cluster view. Authorization is the separate SPIFFE
// allowlist (spiffeGate); the broker's lease/fence use the BROKER's own id, never
// this. So a client reconnecting with a rotated cert can't strand work — and
// because a SPIFFE ID is STABLE across cert rotation (the SVID's URI SAN outlives
// its key), the verified id doesn't even flicker.
type peerIdentity struct {
	// id is the value attributed + displayed. A full SPIFFE ID when verified
	// (spiffe://…), else the observed peer IP. Empty only when neither is available.
	id string
	// source records how id was derived, for the UI badge + audit:
	// "spiffe" | "mesh-header" | "peer-ip" | "".
	source string
	// verified is true only when id came from a cryptographically-verified source
	// (mTLS client cert, or a trusted service-mesh header) — false for a bare peer IP.
	verified bool
}

// peerIdentitySource selects how the broker resolves a connection's identity,
// from the PEER_IDENTITY_SOURCE env (see resolvePeerIdentitySource). It governs
// ONLY the VERIFIED tier; the peer-IP fallback always applies when no verified
// identity is found.
type peerIdentitySource int

const (
	// peerSourceMTLS (default): read the SPIFFE ID from the connection's own
	// verified client cert. Correct when the broker terminates mTLS itself.
	peerSourceMTLS peerIdentitySource = iota
	// peerSourceMeshHeader: a service mesh (Istio/Linkerd) terminates mTLS in a
	// sidecar, so the broker's own r.TLS is the SIDECAR's cert, not the workload's
	// — the workload identity arrives in a trusted header instead. OPT-IN only:
	// a header is forgeable by anything that can reach the port directly, so the
	// operator asserts "a mesh fronts me and sets/strips these headers" by setting
	// PEER_IDENTITY_SOURCE=mesh-header (mirrors API_RATE_LIMIT_TRUST_PROXY).
	peerSourceMeshHeader
)

// resolvePeerIdentitySource maps the PEER_IDENTITY_SOURCE env value to the enum.
// Empty / "mtls" (default) → peerSourceMTLS; "mesh-header" → peerSourceMeshHeader.
// Any other value falls back to the safe default (mTLS) — an unrecognised knob
// must never silently trust a header.
func resolvePeerIdentitySource(v string) peerIdentitySource {
	if strings.EqualFold(strings.TrimSpace(v), "mesh-header") {
		return peerSourceMeshHeader
	}
	return peerSourceMTLS
}

// istioXFCCHeader / linkerdClientIDHeader are the headers a service mesh sets to
// convey the ORIGINATING workload's identity after it terminates mTLS in a sidecar.
//   - Istio X-Forwarded-Client-Cert: a ";"-separated list of "key=value" pairs;
//     the workload SPIFFE ID is the URI=spiffe://… entry.
//   - Linkerd l5d-client-id: the workload's identity verbatim
//     (e.g. worker.converge.serviceaccount.identity.linkerd.cluster.local).
const (
	istioXFCCHeader       = "X-Forwarded-Client-Cert"
	linkerdClientIDHeader = "L5d-Client-Id"
)

// resolvePeerIdentity derives the connection's identity from r, per src, using
// ONLY observed connection properties — never a client self-report.
//
// Precedence (highest-trust first):
//  1. verified identity for src (mTLS client-cert SPIFFE ID, or a trusted mesh header),
//  2. the observed peer IP (unverified — the only thing we can vouch for sans cert),
//  3. empty (nothing available, e.g. no cert and no RemoteAddr).
func resolvePeerIdentity(r *http.Request, src peerIdentitySource) peerIdentity {
	switch src {
	case peerSourceMeshHeader:
		if id := meshHeaderIdentity(r); id != "" {
			return peerIdentity{id: id, source: "mesh-header", verified: true}
		}
	default: // peerSourceMTLS
		if id := mtlsSPIFFEIdentity(r); id != "" {
			return peerIdentity{id: id, source: "spiffe", verified: true}
		}
	}
	// No verified identity: attribute by the bare peer IP (the broker observes it;
	// nothing self-reported is trusted). Marked unverified for the UI/audit.
	if ip := peerIP(r); ip != "" {
		return peerIdentity{id: ip, source: "peer-ip", verified: false}
	}
	return peerIdentity{}
}

// mtlsSPIFFEIdentity returns the SPIFFE ID from the connection's verified client
// cert URI SAN, or "" when there's no verified client cert / no SPIFFE URI SAN.
// Reads from VerifiedChains (populated by RequireAndVerifyClientCert), the same
// already-verified leaf the spiffeGate authz uses.
func mtlsSPIFFEIdentity(r *http.Request) string {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
		return ""
	}
	return spiffeURIFromCert(r.TLS.VerifiedChains[0][0])
}

// spiffeURIFromCert returns the first URI-SAN that parses as a SPIFFE ID, or "".
func spiffeURIFromCert(leaf *x509.Certificate) string {
	for _, uri := range leaf.URIs {
		if id, err := spiffeid.FromURI(uri); err == nil {
			return id.String()
		}
	}
	return ""
}

// meshHeaderIdentity extracts the workload identity a service mesh conveys after
// terminating mTLS: Istio's XFCC (the URI=spiffe://… field) or Linkerd's
// l5d-client-id. Returns "" when neither header carries an identity. Only called
// when PEER_IDENTITY_SOURCE=mesh-header, so the operator has asserted these
// headers are set by a trusted sidecar (and not spoofable by a direct client).
func meshHeaderIdentity(r *http.Request) string {
	if xfcc := r.Header.Get(istioXFCCHeader); xfcc != "" {
		if id := spiffeFromXFCC(xfcc); id != "" {
			return id
		}
	}
	if l5d := strings.TrimSpace(r.Header.Get(linkerdClientIDHeader)); l5d != "" {
		return l5d
	}
	return ""
}

// spiffeFromXFCC pulls the URI=spiffe://… identity out of an Istio
// X-Forwarded-Client-Cert value. XFCC is a comma-separated list of proxy hops,
// each a ";"-separated list of key=value pairs; we take the first "URI=" whose
// value parses as a SPIFFE ID. Values may be quoted.
func spiffeFromXFCC(xfcc string) string {
	for hop := range strings.SplitSeq(xfcc, ",") {
		for kv := range strings.SplitSeq(hop, ";") {
			k, v, ok := strings.Cut(strings.TrimSpace(kv), "=")
			if !ok || !strings.EqualFold(k, "URI") {
				continue
			}
			v = strings.Trim(strings.TrimSpace(v), `"`)
			if id, err := spiffeid.FromString(v); err == nil {
				return id.String()
			}
		}
	}
	return ""
}

// peerIP returns the connection's remote IP (no port), or "" if unparseable.
func peerIP(r *http.Request) string {
	if r.RemoteAddr == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

// peerIdentityCtxKey is the private context key the spiffeGate uses to carry the
// resolved peerIdentity into the WorkStream handler (whose ctx derives from the
// request's — see connect-go NewBidiStreamHandler).
type peerIdentityCtxKey struct{}

// withPeerIdentity returns ctx carrying pi.
func withPeerIdentity(ctx context.Context, pi peerIdentity) context.Context {
	return context.WithValue(ctx, peerIdentityCtxKey{}, pi)
}

// peerIdentityFrom returns the peerIdentity stashed on ctx, or the zero value when
// none was resolved (e.g. a plaintext dev connection the gate didn't wrap).
func peerIdentityFrom(ctx context.Context) peerIdentity {
	pi, _ := ctx.Value(peerIdentityCtxKey{}).(peerIdentity)
	return pi
}
