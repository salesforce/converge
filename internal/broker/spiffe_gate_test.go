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

	"github.com/salesforce/converge/internal/meshpb/meshpbconnect"
	"github.com/salesforce/converge/pkg/spiffeauthz"
	"github.com/salesforce/converge/sdk-go/workerpb/workerpbconnect"
)

// leafWithSPIFFE builds a self-signed leaf carrying spiffeID as its URI SAN, for
// stuffing into a fake tls.ConnectionState's VerifiedChains (what spiffeGate reads).
func leafWithSPIFFE(t *testing.T, spiffeID string) *x509.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	u, err := url.Parse(spiffeID)
	if err != nil {
		t.Fatalf("parse %q: %v", spiffeID, err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "leaf"},
		NotBefore:    time.Unix(0, 0),
		NotAfter:     time.Unix(1<<31, 0),
		URIs:         []*url.URL{u},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}
	return cert
}

// TestSpiffeGatePerServiceAuthz is the core proof of the worker/broker SPIFFE split:
// the SAME listener admits both audiences (the union at the TLS handshake), but a
// WorkerService RPC is allowed ONLY for a worker-allowlist identity and a MeshService
// RPC ONLY for a mesh-allowlist identity — so a worker cert can't reach the mesh (and
// a peer-broker cert can't pull work). spiffeGate is exercised directly with a faked
// verified client cert, since Connect's Peer doesn't surface the TLS chain.
func TestSpiffeGatePerServiceAuthz(t *testing.T) {
	const workerID = "spiffe://ex.org/converge/worker/pod-a"
	const brokerID = "spiffe://ex.org/converge/broker/pod-b"

	workerAuthz, err := spiffeauthz.Parse(workerID)
	if err != nil {
		t.Fatal(err)
	}
	meshAuthz, err := spiffeauthz.Parse(brokerID)
	if err != nil {
		t.Fatal(err)
	}

	s := &Server{workerAuthz: workerAuthz, meshAuthz: meshAuthz}
	// A stub next handler: any request that reaches it "passed" the gate.
	reached := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	gate := s.spiffeGate(reached)

	workerPath := "/" + workerpbconnect.WorkerServiceName + "/WorkStream"
	meshPath := "/" + meshpbconnect.MeshServiceName + "/Route"

	// do drives the gate handler DIRECTLY with a request whose r.TLS carries a faked
	// verified client cert (spiffeID in its URI SAN), returning the status the gate
	// produced. Calling ServeHTTP directly — not over a real socket — is what lets us
	// set r.TLS: a plain httptest server would overwrite it from the (cert-less)
	// connection, and standing up a full mTLS handshake per case is unnecessary to
	// exercise the gate's path/identity routing.
	do := func(t *testing.T, path, spiffeID string) int {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, path, nil)
		leaf := leafWithSPIFFE(t, spiffeID)
		req.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}}
		rec := httptest.NewRecorder()
		gate.ServeHTTP(rec, req)
		return rec.Code
	}

	// The four-quadrant matrix: each audience may reach ONLY its own service.
	t.Run("worker id on worker path is allowed", func(t *testing.T) {
		if got := do(t, workerPath, workerID); got != http.StatusOK {
			t.Errorf("worker→worker: status = %d, want 200", got)
		}
	})
	t.Run("worker id on mesh path is forbidden", func(t *testing.T) {
		if got := do(t, meshPath, workerID); got != http.StatusForbidden {
			t.Errorf("worker→mesh: status = %d, want 403", got)
		}
	})
	t.Run("broker id on mesh path is allowed", func(t *testing.T) {
		if got := do(t, meshPath, brokerID); got != http.StatusOK {
			t.Errorf("broker→mesh: status = %d, want 200", got)
		}
	})
	t.Run("broker id on worker path is forbidden", func(t *testing.T) {
		if got := do(t, workerPath, brokerID); got != http.StatusForbidden {
			t.Errorf("broker→worker: status = %d, want 403", got)
		}
	})
	t.Run("unknown path is not found", func(t *testing.T) {
		if got := do(t, "/nope", workerID); got != http.StatusNotFound {
			t.Errorf("unknown path: status = %d, want 404", got)
		}
	})
}

// TestSpiffeGateDisabledSkipsAuthz: with no allowlist on either service (both matchers
// nil — the plain/dev path), the gate does NOT enforce authz, so a request to a known
// service path passes without a client cert. It still routes by path (the gate always
// runs now, to resolve+stash the peer identity for attribution), so an UNKNOWN path is
// 404 and a known-service request reaches the handler.
func TestSpiffeGateDisabledSkipsAuthz(t *testing.T) {
	s := &Server{peerSource: peerSourceMTLS} // nil matchers → no authz
	reached := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	gate := s.spiffeGate(reached)

	t.Run("known service path passes without a cert", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, workerServicePrefix+"WorkStream", nil)
		rec := httptest.NewRecorder()
		gate.ServeHTTP(rec, r)
		if rec.Code != http.StatusOK {
			t.Errorf("disabled-authz known path: status = %d, want 200", rec.Code)
		}
	})
	t.Run("unknown path is 404", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/anything", nil)
		rec := httptest.NewRecorder()
		gate.ServeHTTP(rec, r)
		if rec.Code != http.StatusNotFound {
			t.Errorf("unknown path: status = %d, want 404", rec.Code)
		}
	})
}
