package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/salesforce/converge/pkg/spiffeauthz"
)

// genKeyPair returns a fresh self-signed leaf cert + key as PEM. cn is stamped
// into the subject CN so two generated pairs are distinguishable by content.
func genKeyPair(t *testing.T, cn string) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Unix(0, 0),
		NotAfter:              time.Unix(1<<31, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}

// writeFile writes b to dir/name and returns the path.
func writeFile(t *testing.T, dir, name string, b []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return p
}

// TestNewCertReloaderFailFast: a bad path, an unparseable keypair, or an
// unparseable client CA must error at construction (boot), not on a later
// handshake.
func TestNewCertReloaderFailFast(t *testing.T) {
	dir := t.TempDir()
	certPEM, keyPEM := genKeyPair(t, "boot")
	certPath := writeFile(t, dir, "tls.crt", certPEM)
	keyPath := writeFile(t, dir, "tls.key", keyPEM)

	t.Run("missing cert file", func(t *testing.T) {
		if _, err := newCertReloader(filepath.Join(dir, "nope.crt"), keyPath, "", nil); err == nil {
			t.Fatal("want error for missing cert file")
		}
	})
	t.Run("garbage keypair", func(t *testing.T) {
		bad := writeFile(t, dir, "bad.key", []byte("not a key"))
		if _, err := newCertReloader(certPath, bad, "", nil); err == nil {
			t.Fatal("want error for unparseable key")
		}
	})
	t.Run("garbage client CA", func(t *testing.T) {
		badCA := writeFile(t, dir, "bad-ca.crt", []byte("not a cert"))
		if _, err := newCertReloader(certPath, keyPath, badCA, nil); err == nil {
			t.Fatal("want error for unparseable client CA")
		}
	})
	t.Run("valid one-way", func(t *testing.T) {
		if _, err := newCertReloader(certPath, keyPath, "", nil); err != nil {
			t.Fatalf("valid material should load: %v", err)
		}
	})
}

// TestCertReloaderHotSwap: rewriting the cert/key files and calling reload
// swaps the served keypair; an unchanged file is a no-op (pointer is reused).
func TestCertReloaderHotSwap(t *testing.T) {
	dir := t.TempDir()
	certPEM, keyPEM := genKeyPair(t, "v1")
	certPath := writeFile(t, dir, "tls.crt", certPEM)
	keyPath := writeFile(t, dir, "tls.key", keyPEM)

	r, err := newCertReloader(certPath, keyPath, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	first, _ := r.r.GetCertificate(nil)

	// No-op reload: file unchanged → same cached pointer, no churn.
	if _, err := r.r.Reload(false); err != nil {
		t.Fatalf("no-op reload: %v", err)
	}
	if got, _ := r.r.GetCertificate(nil); got != first {
		t.Error("unchanged files should not swap the cached cert pointer")
	}

	// Rotate: write a new keypair, reload → served cert changes.
	cert2, key2 := genKeyPair(t, "v2")
	if err := os.WriteFile(certPath, cert2, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, key2, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.r.Reload(false); err != nil {
		t.Fatalf("rotate reload: %v", err)
	}
	got, _ := r.r.GetCertificate(nil)
	if got == first {
		t.Fatal("rotated files should swap the cached cert pointer")
	}
	// The new served leaf must be the v2 subject.
	leaf, err := x509.ParseCertificate(got.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if leaf.Subject.CommonName != "v2" {
		t.Errorf("served CN = %q, want v2", leaf.Subject.CommonName)
	}
}

// TestCertReloaderBadReloadKeepsLastGood: a half-written / corrupt file
// mid-rotation returns an error from reload but leaves the last-good keypair in
// place, so TLS keeps working until the write completes.
func TestCertReloaderBadReloadKeepsLastGood(t *testing.T) {
	dir := t.TempDir()
	certPEM, keyPEM := genKeyPair(t, "good")
	certPath := writeFile(t, dir, "tls.crt", certPEM)
	keyPath := writeFile(t, dir, "tls.key", keyPEM)

	r, err := newCertReloader(certPath, keyPath, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	good, _ := r.r.GetCertificate(nil)

	// Truncate the cert to a half-written PEM.
	if err := os.WriteFile(certPath, []byte("-----BEGIN CERTIFICATE-----\nhalf"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.r.Reload(false); err == nil {
		t.Fatal("want error reloading a corrupt cert")
	}
	if got, _ := r.r.GetCertificate(nil); got != good {
		t.Error("a failed reload must keep the last-good cert")
	}
}

// TestCertReloaderMTLSConfig: with a client CA the served config requires and
// verifies client certs against the configured pool; without one it stays
// one-way (no client auth).
func TestCertReloaderMTLSConfig(t *testing.T) {
	dir := t.TempDir()
	certPEM, keyPEM := genKeyPair(t, "srv")
	certPath := writeFile(t, dir, "tls.crt", certPEM)
	keyPath := writeFile(t, dir, "tls.key", keyPEM)
	caPEM, _ := genKeyPair(t, "client-ca")
	caPath := writeFile(t, dir, "client-ca.crt", caPEM)

	t.Run("one-way TLS leaves client auth off", func(t *testing.T) {
		r, err := newCertReloader(certPath, keyPath, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		cfg := r.tlsConfig()
		if cfg.GetCertificate == nil {
			t.Fatal("GetCertificate must be set (hot reload)")
		}
		if cfg.GetConfigForClient != nil {
			t.Error("one-way TLS must not install a client-auth callback")
		}
		if cfg.ClientAuth != tls.NoClientCert {
			t.Errorf("ClientAuth = %v, want NoClientCert", cfg.ClientAuth)
		}
	})

	t.Run("mTLS requires and verifies the client cert", func(t *testing.T) {
		r, err := newCertReloader(certPath, keyPath, caPath, nil)
		if err != nil {
			t.Fatal(err)
		}
		cfg := r.tlsConfig()
		if cfg.GetConfigForClient == nil {
			t.Fatal("mTLS must install a per-handshake client-auth callback")
		}
		perConn, err := cfg.GetConfigForClient(nil)
		if err != nil {
			t.Fatal(err)
		}
		if perConn.ClientAuth != tls.RequireAndVerifyClientCert {
			t.Errorf("ClientAuth = %v, want RequireAndVerifyClientCert", perConn.ClientAuth)
		}
		if perConn.ClientCAs == nil {
			t.Error("ClientCAs pool must be populated for mTLS")
		}
	})
}

// ca is a tiny self-signed CA that can issue leaf certs, for the live-handshake
// test below.
type ca struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte
}

func newCA(t *testing.T) *ca {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(100),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Unix(0, 0),
		NotAfter:              time.Unix(1<<31, 0),
		KeyUsage:              x509.KeyUsageCertSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &ca{cert: cert, key: key, certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// issue signs a leaf cert+key for cn. server=true sets the serverAuth EKU + a
// loopback SAN (so a client verifying the hostname is satisfied); otherwise it's
// a clientAuth leaf.
func (c *ca) issue(t *testing.T, cn string, server bool) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Unix(2, 0).UnixNano() + int64(len(cn))),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Unix(0, 0),
		NotAfter:     time.Unix(1<<31, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	if server {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		tmpl.DNSNames = []string{"localhost"}
		// httptest serves on 127.0.0.1, so the leaf needs the loopback IP SAN
		// for the client's hostname verification to pass.
		tmpl.IPAddresses = []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}
	} else {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		t.Fatalf("issue %s: %v", cn, err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}

// TestCertReloaderLiveMTLSHandshake is the end-to-end proof: a real TLS server
// using the reloader's *tls.Config rejects a client with no cert (and one with
// an untrusted cert) and accepts a client whose cert the configured CA signed —
// the RequireAndVerifyClientCert contract, exercised through a real handshake
// rather than by inspecting the config.
func TestCertReloaderLiveMTLSHandshake(t *testing.T) {
	dir := t.TempDir()
	authority := newCA(t)
	srvCert, srvKey := authority.issue(t, "server", true)
	certPath := writeFile(t, dir, "tls.crt", srvCert)
	keyPath := writeFile(t, dir, "tls.key", srvKey)
	caPath := writeFile(t, dir, "client-ca.crt", authority.certPEM)

	r, err := newCertReloader(certPath, keyPath, caPath, nil)
	if err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	srv.TLS = r.tlsConfig()
	srv.StartTLS()
	defer srv.Close()

	// A pool that trusts the server's CA, so the only variable under test is the
	// CLIENT side of the mTLS handshake.
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(authority.certPEM)

	doGet := func(clientCerts []tls.Certificate) (int, error) {
		c := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
			RootCAs:      roots,
			Certificates: clientCerts,
		}}}
		defer c.CloseIdleConnections()
		resp, err := c.Get(srv.URL)
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode, nil
	}

	t.Run("no client cert is rejected", func(t *testing.T) {
		if _, err := doGet(nil); err == nil {
			t.Fatal("server must reject a client presenting no cert")
		}
	})

	t.Run("untrusted client cert is rejected", func(t *testing.T) {
		otherCA := newCA(t)
		cPEM, kPEM := otherCA.issue(t, "intruder", false)
		pair, err := tls.X509KeyPair(cPEM, kPEM)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := doGet([]tls.Certificate{pair}); err == nil {
			t.Fatal("server must reject a client cert signed by an untrusted CA")
		}
	})

	t.Run("CA-signed client cert is accepted", func(t *testing.T) {
		cPEM, kPEM := authority.issue(t, "client", false)
		pair, err := tls.X509KeyPair(cPEM, kPEM)
		if err != nil {
			t.Fatal(err)
		}
		code, err := doGet([]tls.Certificate{pair})
		if err != nil {
			t.Fatalf("trusted client cert should be accepted: %v", err)
		}
		if code != http.StatusOK {
			t.Errorf("status = %d, want 200", code)
		}
	})
}

// issueSPIFFE signs a clientAuth leaf carrying the given SPIFFE ID as a URI SAN
// — the shape a SPIRE/cert-manager-issued workload SVID has.
func (c *ca) issueSPIFFE(t *testing.T, spiffeID string) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	uri, err := url.Parse(spiffeID)
	if err != nil {
		t.Fatalf("parse spiffe id: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Unix(3, 0).UnixNano() + int64(len(spiffeID))),
		Subject:      pkix.Name{CommonName: "workload"},
		NotBefore:    time.Unix(0, 0),
		NotAfter:     time.Unix(1<<31, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:         []*url.URL{uri},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		t.Fatalf("issue spiffe leaf: %v", err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}

// (SPIFFE allowlist PARSING + matching moved to pkg/spiffeauthz; its own test
// covers Parse/Union/CheckLeaf. Here we cover the certReloader's WIRING of a
// parsed matcher: the mTLS-requirement guard and the verifyPeerSPIFFE hook.)

func TestNewCertReloaderAuthzRequiresMTLS(t *testing.T) {
	dir := t.TempDir()
	certPEM, keyPEM := genKeyPair(t, "boot")
	certPath := writeFile(t, dir, "tls.crt", certPEM)
	keyPath := writeFile(t, dir, "tls.key", keyPEM)
	m, err := spiffeauthz.Parse("spiffe://ex.org/a")
	if err != nil {
		t.Fatal(err)
	}
	// allowlist set but caFile empty → must fail fast (nothing to enforce against).
	if _, err := newCertReloader(certPath, keyPath, "", m); err == nil {
		t.Fatal("SPIFFE allowlist without mTLS (no client CA) must be a boot error")
	}
}

func TestVerifyPeerSPIFFE(t *testing.T) {
	authority := newCA(t)
	allowed := "spiffe://ex.org/ns/prod/sa/caller"
	r := &certReloader{}
	m, err := spiffeauthz.Parse(allowed)
	if err != nil {
		t.Fatal(err)
	}
	r.authorizer = m

	leafFromCert := func(certPEM []byte) [][]*x509.Certificate {
		block, _ := pem.Decode(certPEM)
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		return [][]*x509.Certificate{{cert}}
	}

	t.Run("allowed identity passes", func(t *testing.T) {
		cPEM, _ := authority.issueSPIFFE(t, allowed)
		if err := r.verifyPeerSPIFFE(nil, leafFromCert(cPEM)); err != nil {
			t.Errorf("allowed SPIFFE id should pass: %v", err)
		}
	})
	t.Run("other identity is rejected", func(t *testing.T) {
		cPEM, _ := authority.issueSPIFFE(t, "spiffe://ex.org/ns/dev/sa/other")
		if err := r.verifyPeerSPIFFE(nil, leafFromCert(cPEM)); err == nil {
			t.Error("a SPIFFE id not in the allowlist must be rejected")
		}
	})
	t.Run("cert with no URI SAN is rejected", func(t *testing.T) {
		cPEM, _ := authority.issue(t, "no-uri", false)
		if err := r.verifyPeerSPIFFE(nil, leafFromCert(cPEM)); err == nil {
			t.Error("a client cert with no URI SAN must be rejected when authz is on")
		}
	})
	t.Run("nil authorizer is a no-op", func(t *testing.T) {
		open := &certReloader{}
		if err := open.verifyPeerSPIFFE(nil, nil); err != nil {
			t.Errorf("nil authorizer should accept everything: %v", err)
		}
	})
}
