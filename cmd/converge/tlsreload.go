package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"time"

	"github.com/salesforce/converge/pkg/spiffeauthz"
	"github.com/salesforce/converge/pkg/tlsreload"
)

// certReloader serves the API server's TLS material — the server keypair and,
// for mTLS, the client-CA pool — from files that are AUTO-INJECTED and rotated
// underneath a running process (cert-manager, a SPIFFE sidecar, a mounted k8s
// secret). It hot-swaps the in-memory material when the files change so a cert
// rotation never needs a restart and never drops a connection.
//
// Why poll rather than fsnotify: a k8s secret/configmap mount is rotated by an
// ATOMIC SYMLINK SWAP of the "..data" directory, not by an in-place write of the
// leaf file. Filesystem watches on the leaf file miss that swap (the inode the
// watch is bound to is unlinked, not modified), so the robust, dependency-free
// approach the k8s ecosystem settled on is to re-read on a timer and compare
// content. A 3m default trails a rotation by at most one interval — fine for
// certs with hours-to-days validity — and costs one stat+read of a few KB.
//
// Concurrency: GetCertificate / GetConfigForClient run on the TLS handshake
// goroutine for EVERY incoming connection, while the poll loop swaps the cached
// values. An RWMutex guards the pointers; handshakes take the read lock (so they
// never block each other) and the rare swap takes the write lock. The cached
// objects themselves are treated as immutable once stored — a swap replaces the
// pointer, it never mutates a live *tls.Certificate or *x509.CertPool — so a
// handshake that grabbed the old pointer keeps using a consistent snapshot.
// certReloader adapts the SHARED tlsreload.Reloader (file poll + fingerprint-diff
// + atomic keypair/CA swap — the same core the worker's client dial uses) to the
// SERVER side: it builds the *tls.Config the API + broker listeners serve, adding
// the server-specific bits the shared core is agnostic to — RequireAndVerifyClientCert
// against the reloaded client-CA pool, and the optional SPIFFE-ID allowlist.
type certReloader struct {
	// r is the shared hot-reloader: caFile here is the CLIENT-CA bundle for mTLS
	// (empty = one-way TLS, clients unauthenticated). GetCertificate serves the
	// server keypair; CAPool serves the client-CA pool — both hot per handshake.
	r *tlsreload.Reloader

	// authorizer, when non-nil, is the SPIFFE-ID allowlist enforced on top of mTLS
	// chain validation: a verified client cert must also carry a URI SAN whose
	// SPIFFE ID matches. nil = chain trust only. Immutable after construction (the
	// allowlist is config, not a rotated file). For a listener that serves several
	// audiences (the broker's WorkerService + MeshService), this is the UNION of
	// their allowlists — the handshake admits any legitimate peer, and per-service
	// Connect interceptors narrow each RPC to its own audience.
	authorizer spiffeauthz.Matcher
	hasCA      bool
}

// newCertReloader loads the initial keypair (and CA pool, when caFile is set) via
// the shared reloader — a bad path / mismatched pair / unparseable CA fails fast
// at BOOT. caFile may be "" for one-way TLS. matcher is the optional SPIFFE-ID
// allowlist (nil = chain trust only), already parsed by the caller (so a listener
// serving several audiences can pass their union); an allowlist without a client
// CA to enforce it against fails fast.
func newCertReloader(certFile, keyFile, caFile string, matcher spiffeauthz.Matcher) (*certReloader, error) {
	if matcher != nil && caFile == "" {
		return nil, fmt.Errorf("a SPIFFE-ID allowlist requires mTLS: set a client CA (caFile) so client certs are presented and chain-verified before identity is checked")
	}
	rl, err := tlsreload.New(certFile, keyFile, caFile)
	if err != nil {
		return nil, err
	}
	return &certReloader{r: rl, authorizer: matcher, hasCA: caFile != ""}, nil
}

// verifyPeerSPIFFE is the tls.Config.VerifyPeerCertificate hook enforcing the
// SPIFFE-ID allowlist AFTER the standard chain verification mTLS already did. The
// leaf comes from verifiedChains (non-empty here because RequireAndVerifyClientCert
// runs chain validation first); spiffeauthz.CheckLeaf applies the allowlist.
func (r *certReloader) verifyPeerSPIFFE(_ [][]byte, verifiedChains [][]*x509.Certificate) error {
	if r.authorizer == nil {
		return nil
	}
	if len(verifiedChains) == 0 || len(verifiedChains[0]) == 0 {
		return fmt.Errorf("spiffe authz: no verified client certificate")
	}
	return spiffeauthz.CheckLeaf(r.authorizer, verifiedChains[0][0])
}

// tlsConfig builds the *tls.Config the server serves with. The server keypair
// always comes from the shared reloader's GetCertificate (hot-reloadable). When a
// client-CA is configured it additionally requires and verifies a client cert
// (mTLS); the CA pool is read fresh per handshake via GetConfigForClient so a
// rotated CA also takes effect with no restart.
func (r *certReloader) tlsConfig() *tls.Config {
	base := &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: r.r.GetCertificate,
	}
	if !r.hasCA {
		return base
	}
	// Re-derive the client-auth fields per handshake so a rotated CA pool is picked
	// up without a restart. Clone the base config and stamp in the CURRENT pool
	// (r.r.CAPool, hot-reloaded); the returned config is used for THIS handshake only.
	base.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		c := base.Clone()
		c.GetConfigForClient = nil // avoid recursion on the per-conn config
		c.ClientAuth = tls.RequireAndVerifyClientCert
		c.ClientCAs = r.r.CAPool()
		// SPIFFE-ID allowlist (if configured) runs AFTER chain verification —
		// RequireAndVerifyClientCert validates the chain, then this rejects any
		// verified peer whose identity isn't allowed. nil authorizer = no-op.
		if r.authorizer != nil {
			c.VerifyPeerCertificate = r.verifyPeerSPIFFE
		}
		return c, nil
	}
	return base
}

// watch runs the shared reloader's poll loop until ctx is cancelled, logging a
// rotation / a transient failure through this package's slog (the shared core is
// logging-agnostic). Last-good material stays in place on a failed reload.
func (r *certReloader) watch(ctx context.Context, interval time.Duration) {
	r.r.Watch(ctx, interval, func(changed bool, err error) {
		c, k, ca := r.r.Files()
		switch {
		case err != nil:
			slog.Warn("TLS reload failed; keeping last-good material", "cert_file", c, "error", err)
		case changed:
			slog.Info("reloaded TLS material", "cert_file", c, "key_file", k, "ca_file", ca, "client_auth", r.hasCA)
		}
	})
}
