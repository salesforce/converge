package spiffeauthz

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net/url"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

// mustID parses s into a spiffeid.ID or fails the test.
func mustID(t *testing.T, s string) spiffeid.ID {
	t.Helper()
	id, err := spiffeid.FromString(s)
	if err != nil {
		t.Fatalf("spiffeid %q: %v", s, err)
	}
	return id
}

func TestParse(t *testing.T) {
	t.Run("empty disables authz", func(t *testing.T) {
		m, err := Parse("")
		if err != nil || m != nil {
			t.Fatalf("empty allowlist should yield (nil, nil); got (%v, %v)", m, err)
		}
	})
	t.Run("valid ids build a matcher", func(t *testing.T) {
		m, err := Parse(" spiffe://ex.org/a , spiffe://ex.org/b/ ")
		if err != nil || m == nil {
			t.Fatalf("valid ids should build a matcher; got (%v, %v)", m, err)
		}
		if !m(mustID(t, "spiffe://ex.org/a")) {
			t.Error("spiffe://ex.org/a should match")
		}
		// trailing slash was trimmed, so the bare form matches
		if !m(mustID(t, "spiffe://ex.org/b")) {
			t.Error("spiffe://ex.org/b should match (trailing slash trimmed)")
		}
		if m(mustID(t, "spiffe://ex.org/c")) {
			t.Error("spiffe://ex.org/c must NOT match")
		}
	})
	t.Run("malformed id is a hard error", func(t *testing.T) {
		if _, err := Parse("not-a-spiffe-id"); err == nil {
			t.Fatal("a malformed SPIFFE ID must fail fast, not be silently dropped")
		}
	})
}

func TestUnion(t *testing.T) {
	worker, _ := Parse("spiffe://ex.org/worker")
	mesh, _ := Parse("spiffe://ex.org/broker")

	t.Run("accepts either side", func(t *testing.T) {
		u := Union(worker, mesh)
		if u == nil {
			t.Fatal("union of two non-nil matchers must be non-nil")
		}
		if !u(mustID(t, "spiffe://ex.org/worker")) {
			t.Error("union must accept a worker id")
		}
		if !u(mustID(t, "spiffe://ex.org/broker")) {
			t.Error("union must accept a broker id")
		}
		if u(mustID(t, "spiffe://ex.org/stranger")) {
			t.Error("union must reject an id in neither list")
		}
	})
	t.Run("nils collapse", func(t *testing.T) {
		if Union(nil, nil) != nil {
			t.Error("union of only nils is nil (no allowlist)")
		}
		u := Union(nil, worker)
		if u == nil || !u(mustID(t, "spiffe://ex.org/worker")) {
			t.Error("union with a nil element keeps the non-nil side")
		}
	})
}

// leafWithURI builds a self-signed leaf carrying spiffeID as its URI SAN (or no
// URI SAN when spiffeID is ""), for CheckLeaf assertions.
func leafWithURI(t *testing.T, spiffeID string) *x509.Certificate {
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
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}
	return cert
}

func TestCheckLeaf(t *testing.T) {
	m, _ := Parse("spiffe://ex.org/ns/prod/sa/caller")

	t.Run("allowed identity passes", func(t *testing.T) {
		if err := CheckLeaf(m, leafWithURI(t, "spiffe://ex.org/ns/prod/sa/caller")); err != nil {
			t.Errorf("allowed SPIFFE id should pass: %v", err)
		}
	})
	t.Run("other identity is rejected", func(t *testing.T) {
		if err := CheckLeaf(m, leafWithURI(t, "spiffe://ex.org/ns/dev/sa/other")); err == nil {
			t.Error("a SPIFFE id not in the allowlist must be rejected")
		}
	})
	t.Run("cert with no URI SAN is rejected", func(t *testing.T) {
		if err := CheckLeaf(m, leafWithURI(t, "")); err == nil {
			t.Error("a leaf with no URI SAN must be rejected when authz is on")
		}
	})
	t.Run("nil leaf is rejected", func(t *testing.T) {
		if err := CheckLeaf(m, nil); err == nil {
			t.Error("a nil leaf must be rejected when authz is on")
		}
	})
	t.Run("nil matcher is a no-op", func(t *testing.T) {
		if err := CheckLeaf(nil, nil); err != nil {
			t.Errorf("nil matcher should accept everything: %v", err)
		}
	})
}
