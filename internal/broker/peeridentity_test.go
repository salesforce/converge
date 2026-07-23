package broker

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

// tlsReqWithSPIFFE builds a request whose r.TLS carries a verified client-cert leaf
// with the given SPIFFE ID as its URI SAN (mimicking RequireAndVerifyClientCert
// having populated VerifiedChains). spiffeID "" → a leaf with no URI SAN.
func tlsReqWithSPIFFE(t *testing.T, spiffeID, remoteAddr string) *http.Request {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "leaf"},
		NotBefore:    time.Unix(0, 0),
		NotAfter:     time.Unix(1<<31, 0),
	}
	if spiffeID != "" {
		u, err := url.Parse(spiffeID)
		if err != nil {
			t.Fatalf("parse %q: %v", spiffeID, err)
		}
		tmpl.URIs = []*url.URL{u}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}
	r := httptest.NewRequest(http.MethodPost, "/x", nil)
	r.RemoteAddr = remoteAddr
	r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}}
	return r
}

func TestResolvePeerIdentitySource(t *testing.T) {
	cases := map[string]peerIdentitySource{
		"":               peerSourceMTLS,
		"mtls":           peerSourceMTLS,
		"MTLS":           peerSourceMTLS,
		"  mesh-header ": peerSourceMeshHeader,
		"MESH-HEADER":    peerSourceMeshHeader,
		"bogus":          peerSourceMTLS, // unrecognised → safe default, never trusts a header
	}
	for in, want := range cases {
		if got := resolvePeerIdentitySource(in); got != want {
			t.Errorf("resolvePeerIdentitySource(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestResolvePeerIdentity_MTLS(t *testing.T) {
	t.Run("verified SPIFFE from client cert wins", func(t *testing.T) {
		r := tlsReqWithSPIFFE(t, "spiffe://ex.org/ns/converge/sa/worker", "10.1.2.3:5555")
		pi := resolvePeerIdentity(r, peerSourceMTLS)
		if pi.id != "spiffe://ex.org/ns/converge/sa/worker" || pi.source != "spiffe" || !pi.verified {
			t.Fatalf("got %+v, want verified spiffe id", pi)
		}
	})
	t.Run("no SPIFFE SAN falls back to peer IP (unverified)", func(t *testing.T) {
		r := tlsReqWithSPIFFE(t, "", "10.1.2.3:5555") // cert but no URI SAN
		pi := resolvePeerIdentity(r, peerSourceMTLS)
		if pi.id != "10.1.2.3" || pi.source != "peer-ip" || pi.verified {
			t.Fatalf("got %+v, want unverified peer-ip 10.1.2.3", pi)
		}
	})
	t.Run("plaintext (no TLS) falls back to peer IP", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/x", nil)
		r.RemoteAddr = "192.168.0.9:40001"
		pi := resolvePeerIdentity(r, peerSourceMTLS)
		if pi.id != "192.168.0.9" || pi.source != "peer-ip" || pi.verified {
			t.Fatalf("got %+v, want unverified peer-ip", pi)
		}
	})
	t.Run("a mesh header is IGNORED in mtls mode (not trusted)", func(t *testing.T) {
		r := tlsReqWithSPIFFE(t, "", "10.0.0.1:1")
		r.Header.Set(istioXFCCHeader, `URI=spiffe://evil.org/impersonator`)
		pi := resolvePeerIdentity(r, peerSourceMTLS)
		if pi.id == "spiffe://evil.org/impersonator" {
			t.Fatal("mtls mode must NOT trust a mesh header — a direct client could forge it")
		}
		if pi.source != "peer-ip" {
			t.Fatalf("got %+v, want peer-ip (header ignored)", pi)
		}
	})
	t.Run("nothing available → empty", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/x", nil)
		r.RemoteAddr = ""
		pi := resolvePeerIdentity(r, peerSourceMTLS)
		if pi.id != "" || pi.verified {
			t.Fatalf("got %+v, want empty identity", pi)
		}
	})
}

func TestResolvePeerIdentity_MeshHeader(t *testing.T) {
	t.Run("Istio XFCC URI field", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/x", nil)
		r.RemoteAddr = "127.0.0.1:1" // the sidecar; ignored when a header resolves
		r.Header.Set(istioXFCCHeader, `By=spiffe://cluster/ns/gw;Hash=abc;URI=spiffe://ex.org/ns/converge/sa/worker`)
		pi := resolvePeerIdentity(r, peerSourceMeshHeader)
		if pi.id != "spiffe://ex.org/ns/converge/sa/worker" || pi.source != "mesh-header" || !pi.verified {
			t.Fatalf("got %+v, want verified mesh-header spiffe id", pi)
		}
	})
	t.Run("Linkerd l5d-client-id", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/x", nil)
		r.RemoteAddr = "127.0.0.1:1"
		r.Header.Set(linkerdClientIDHeader, "worker.converge.serviceaccount.identity.linkerd.cluster.local")
		pi := resolvePeerIdentity(r, peerSourceMeshHeader)
		if pi.id != "worker.converge.serviceaccount.identity.linkerd.cluster.local" || pi.source != "mesh-header" || !pi.verified {
			t.Fatalf("got %+v, want verified mesh-header l5d id", pi)
		}
	})
	t.Run("no mesh header falls back to peer IP", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/x", nil)
		r.RemoteAddr = "10.9.9.9:2"
		pi := resolvePeerIdentity(r, peerSourceMeshHeader)
		if pi.id != "10.9.9.9" || pi.source != "peer-ip" || pi.verified {
			t.Fatalf("got %+v, want unverified peer-ip", pi)
		}
	})
	t.Run("XFCC without a valid URI field → peer IP", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/x", nil)
		r.RemoteAddr = "10.9.9.9:2"
		r.Header.Set(istioXFCCHeader, `By=spiffe://x/y;Hash=abc`) // no URI= field
		pi := resolvePeerIdentity(r, peerSourceMeshHeader)
		if pi.source != "peer-ip" {
			t.Fatalf("got %+v, want peer-ip", pi)
		}
	})
}

// TestSpiffeGateStashesIdentity: the gate resolves the connection's identity and
// stashes it on the request context, so the (bidi) WorkStream handler — whose ctx
// derives from the request's — reads it via peerIdentityFrom without ever seeing a
// self-report. Verified for a worker-service path with a SPIFFE cert.
func TestSpiffeGateStashesIdentity(t *testing.T) {
	s := &Server{peerSource: peerSourceMTLS} // no allowlist → authz skipped, identity still resolved
	var got peerIdentity
	sink := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = peerIdentityFrom(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	gate := s.spiffeGate(sink)

	r := tlsReqWithSPIFFE(t, "spiffe://ex.org/ns/converge/sa/worker", "10.1.2.3:5555")
	r.URL.Path = workerServicePrefix + "WorkStream"
	rec := httptest.NewRecorder()
	gate.ServeHTTP(rec, r)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got.id != "spiffe://ex.org/ns/converge/sa/worker" || got.source != "spiffe" || !got.verified {
		t.Fatalf("stashed identity = %+v, want verified spiffe id", got)
	}
}
