// Package tlsreload hot-reloads TLS material — a keypair and an optional CA pool —
// from files that are AUTO-INJECTED and rotated underneath a running process
// (cert-manager, a SPIFFE sidecar, a mounted k8s Secret). It is the SHARED core
// used by both the server side (control API + broker listeners, cmd/converge) and
// the client side (the worker dialing the broker, sdk-go/converge), so a rotation
// of short-lived certs never needs a restart and never drops a connection — on
// EITHER side of the mTLS handshake.
//
// Why poll rather than fsnotify: a k8s Secret/ConfigMap mount is rotated by an
// ATOMIC SYMLINK SWAP of the "..data" directory, not an in-place write of the leaf
// file. A watch bound to the leaf inode misses that swap (the inode is unlinked,
// not modified). The robust, dependency-free approach the k8s ecosystem settled on
// is to re-read on a timer and compare content — one stat+read of a few KB per
// interval, trailing a rotation by at most one interval (fine for hours-to-days
// certs).
//
// Concurrency: the hooks (GetCertificate / GetClientCertificate / CAPool) run on
// the TLS handshake goroutine for every connection while Watch swaps the cached
// pointers. An RWMutex guards them; handshakes take the read lock (never blocking
// each other), the rare swap takes the write lock. Cached objects are immutable
// once stored — a swap replaces the pointer, never mutates a live *tls.Certificate
// or *x509.CertPool — so a handshake holding the old pointer keeps a consistent
// snapshot.
package tlsreload

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"time"
)

// Reloader serves a keypair (+ optional CA pool) from files, hot-swapping the
// in-memory material when the files change. Construct with New (fails fast on a
// bad path / mismatched pair / unparseable CA at boot), install the relevant hook
// into a tls.Config, and run Watch to pick up rotations.
type Reloader struct {
	certFile string
	keyFile  string
	// caFile is optional: the peer-verification CA bundle. On the CLIENT it is the
	// server (broker) CA → RootCAs; on the SERVER it is the client CA → ClientCAs.
	// Empty disables CA reloading (that side uses system roots or no client auth).
	caFile string

	mu     sync.RWMutex
	cert   *tls.Certificate
	caPool *x509.CertPool

	// Content fingerprints of the last successfully-loaded files: Watch swaps (and
	// the caller logs) only on an ACTUAL change, and a transient read/parse error
	// mid-rotation leaves the last-good material in place instead of tearing TLS down.
	certSum [sha256.Size]byte
	keySum  [sha256.Size]byte
	caSum   [sha256.Size]byte
}

// New loads the initial keypair (and CA pool when caFile is set) so a bad path,
// unreadable key, mismatched pair, or unparseable CA fails fast at BOOT rather
// than on the first handshake. caFile may be "" (no CA reloading).
func New(certFile, keyFile, caFile string) (*Reloader, error) {
	r := &Reloader{certFile: certFile, keyFile: keyFile, caFile: caFile}
	if _, err := r.Reload(true); err != nil {
		return nil, err
	}
	return r, nil
}

// Reload re-reads the files and atomically swaps any that changed. initial=true
// (boot) forces a load and treats every read error as fatal. On the polling path
// (initial=false) it returns changed=false after a no-op when nothing changed, and
// a transient read/parse error is returned (the caller logs and keeps last-good
// material) rather than dropping TLS. Returns whether the material actually changed
// so the caller can log a rotation exactly once.
func (r *Reloader) Reload(initial bool) (changed bool, err error) {
	certPEM, err := os.ReadFile(r.certFile)
	if err != nil {
		return false, fmt.Errorf("read TLS cert %q: %w", r.certFile, err)
	}
	keyPEM, err := os.ReadFile(r.keyFile)
	if err != nil {
		return false, fmt.Errorf("read TLS key %q: %w", r.keyFile, err)
	}
	var caPEM []byte
	if r.caFile != "" {
		caPEM, err = os.ReadFile(r.caFile)
		if err != nil {
			return false, fmt.Errorf("read TLS CA %q: %w", r.caFile, err)
		}
	}

	certSum := sha256.Sum256(certPEM)
	keySum := sha256.Sum256(keyPEM)
	caSum := sha256.Sum256(caPEM)

	// Nothing changed since the last good load — skip the parse + swap. On boot the
	// zero fingerprints force a fall-through so the material loads.
	if !initial && certSum == r.certSum && keySum == r.keySum && caSum == r.caSum {
		return false, nil
	}

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return false, fmt.Errorf("parse TLS keypair (%q,%q): %w", r.certFile, r.keyFile, err)
	}
	var caPool *x509.CertPool
	if r.caFile != "" {
		caPool = x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caPEM) {
			return false, fmt.Errorf("parse TLS CA %q: no valid certificate found", r.caFile)
		}
	}

	r.mu.Lock()
	r.cert = &cert
	r.caPool = caPool
	r.certSum, r.keySum, r.caSum = certSum, keySum, caSum
	r.mu.Unlock()
	return true, nil
}

// GetCertificate is the tls.Config.GetCertificate hook (SERVER side): returns the
// current keypair on every handshake, so a rotated cert is served with no restart.
func (r *Reloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cert, nil
}

// GetClientCertificate is the tls.Config.GetClientCertificate hook (CLIENT side):
// returns the current keypair for the worker to PRESENT on every (re)dial, so a
// rotated client cert is used with no restart. This is the hook the worker was
// missing — a static Certificates slice never re-reads the file.
func (r *Reloader) GetClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cert, nil
}

// CAPool returns the current CA pool (nil when no caFile). Read per handshake so a
// rotated CA takes effect with no restart: on the client wire it into a
// GetClientCertificate-based config's RootCAs via a fresh read; on the server it
// feeds ClientCAs. Callers that cache tls.Config must re-read this per handshake
// (e.g. via GetConfigForClient / DialTLSContext) rather than snapshotting it once.
func (r *Reloader) CAPool() *x509.CertPool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.caPool
}

// HasCA reports whether a CA file is configured (so the caller knows to wire the
// pool at all).
func (r *Reloader) HasCA() bool { return r.caFile != "" }

// Files returns the configured paths for logging.
func (r *Reloader) Files() (cert, key, ca string) { return r.certFile, r.keyFile, r.caFile }

// Watch runs the poll loop until ctx is cancelled, re-reading the files every
// interval and hot-swapping on change. onReload (optional) is called after each
// tick with (changed, err) so the caller logs a rotation / a transient failure in
// its own logger — this package stays logging-agnostic. A failed reload keeps the
// last-good material in place (a half-written file mid-rotation must not break TLS;
// the next tick picks up the completed write). interval <= 0 disables the loop
// (returns immediately) so a caller can opt out of reloading.
func (r *Reloader) Watch(ctx context.Context, interval time.Duration, onReload func(changed bool, err error)) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			changed, err := r.Reload(false)
			if onReload != nil {
				onReload(changed, err)
			}
		}
	}
}
