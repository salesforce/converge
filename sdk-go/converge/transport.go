package converge

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"time"

	"connectrpc.com/connect"

	"github.com/salesforce/converge/pkg/tlsreload"
	"github.com/salesforce/converge/pkg/wirelimits"
	"github.com/salesforce/converge/sdk-go/workerpb/workerpbconnect"
)

// transport.go builds the broker Connect client from the Worker's address + TLS
// options. This is the plumbing the CLIENT SDK hides from a provider author: they pass
// a broker address (and, for prod, withTLS files) and never touch connectrpc, the h2c
// handshake, the HTTP transport pool, or cert hot-reload.

// withTransport is the ADVANCED escape hatch: supply an already-built broker transport
// (the generated workerpbconnect client, or any implementation) instead of letting the
// worker dial from an address + TLS. The worker then skips its own dialing entirely and
// uses this client. For custom transports — a test's in-process fake broker, a bespoke
// HTTP client, an alternate auth scheme — mirroring the WithHTTPClient/endpoint escape
// hatches cloud SDKs expose. Most workers never need it: the env (BROKER_ADDR + TLS_*) is
// the normal path.
func withTransport(client transport) option {
	return func(c *config) { c.client = client }
}

// dial builds the broker Connect client from cfg.brokerAddr + the TLS options. It
// starts the cert-reload watcher (bound to ctx) when TLS is set. Called once, on the first
// run — so construction can't fail and the dev sees no transport types.
func (w *worker) dial(ctx context.Context) (transport, error) {
	httpClient, err := w.brokerHTTPClient(ctx)
	if err != nil {
		return nil, err
	}
	// WithSendGzip compresses the worker's OUTBOUND messages — the big one being a
	// COMPOSE result Complete (a large fan-out gzips to a small fraction). The broker
	// gzips its stream replies too (the client advertises Accept-Encoding: gzip by
	// default), so both directions are compressed; the MaxBytes caps bound the
	// DECOMPRESSED size.
	return workerpbconnect.NewWorkerServiceClient(httpClient, w.cfg.brokerAddr,
		connect.WithSendGzip(),
		connect.WithReadMaxBytes(wirelimits.MaxMessageBytes),
		connect.WithSendMaxBytes(wirelimits.MaxMessageBytes)), nil
}

// brokerHTTPClient builds the HTTP client the broker is dialed over. Without withTLS
// it's prior-knowledge cleartext h2c; with withTLS it's mTLS with hot-reloaded client
// cert + broker CA.
//
// The transport is POOL-TUNED (not http.DefaultTransport). A worker holds ONE
// long-lived bidi WorkStream AND makes unary GetProviderConfig calls (boot, every
// reconnect re-pull, the periodic refresh). On HTTP/1.1 the default
// MaxIdleConnsPerHost=2 can't reuse a warm connection under a correlated burst (a
// broker restart reconnecting 50 workers at once); raising the per-host budget lets
// them reuse a warm pool instead of storming fresh connects, bounded so a worker can't
// exhaust the broker's fds.
func (w *worker) brokerHTTPClient(ctx context.Context) (connect.HTTPClient, error) {
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		ForceAttemptHTTP2:     true, // multiplex over one conn when the broker offers h2 (TLS path)
		MaxIdleConns:          512,
		MaxIdleConnsPerHost:   256, // default is 2 — the connection-storm fix on HTTP/1.1
		MaxConnsPerHost:       512, // cap so a worker can't exhaust the broker's fds
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		// HTTP/1.1 backstop: OS TCP keepalive probes a half-open connection so a
		// silently-partitioned worker's WorkStream read eventually errors instead of
		// blocking forever. The h2 ping is the fast path on the TLS/prod route.
		DialContext: (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
	}
	if w.cfg.tlsCertFile == "" || w.cfg.tlsKeyFile == "" {
		// Cleartext path: PRIOR-KNOWLEDGE cleartext HTTP/2 (h2c), NOT HTTP/1.1. The
		// WorkStream RPC is a BIDIRECTIONAL stream, which requires end-to-end HTTP/2 — a
		// plain HTTP/1.1 connection makes the broker reject it ("505 HTTP Version Not
		// Supported"). ForceAttemptHTTP2 only upgrades over TLS (ALPN); on cleartext we
		// must ask for h2c explicitly via the stdlib Protocols. HTTP/1 is left off so
		// every dial is h2c.
		var protos http.Protocols
		protos.SetUnencryptedHTTP2(true)
		tr.Protocols = &protos
		tr.ForceAttemptHTTP2 = false // Protocols governs; avoid the h1+ForceHTTP2 ambiguity
		return &http.Client{Transport: tr}, nil
	}
	// mTLS: the worker's SHORT-LIVED client cert (and the broker CA) can be rotated
	// under a running worker (cert-manager / a SPIFFE sidecar / a mounted Secret). A
	// static Certificates slice loaded once would keep presenting the EXPIRED cert after
	// rotation and the broker's RequireAndVerifyClientCert would start rejecting it,
	// silently stranding the worker. So drive the keypair from a hot-reloading Reloader
	// via GetClientCertificate (read fresh per handshake) and re-derive RootCAs per
	// handshake too so a rotated broker CA also takes effect with no restart.
	reloader, err := tlsreload.New(w.cfg.tlsCertFile, w.cfg.tlsKeyFile, w.cfg.tlsCAFile)
	if err != nil {
		return nil, fmt.Errorf("converge: worker TLS: %w", err)
	}
	go reloader.Watch(ctx, w.cfg.tlsReloadInterval, func(changed bool, rerr error) {
		switch {
		case rerr != nil:
			w.cfg.logger.Warn("converge: worker TLS reload failed; keeping last-good material", "error", rerr)
		case changed:
			c, k, ca := reloader.Files()
			w.cfg.logger.Info("converge: worker reloaded TLS material", "cert_file", c, "key_file", k, "broker_ca", ca)
		}
	})
	tr.TLSClientConfig = &tls.Config{
		MinVersion:           tls.VersionTLS12,
		GetClientCertificate: reloader.GetClientCertificate, // hot: presented per (re)dial
	}
	// RootCAs is a plain field the transport caches, so it can't hot-reload in place.
	// Re-derive it per handshake via a DialTLSContext that clones the config and stamps
	// the CURRENT CA pool. Only when a broker CA is pinned; without it the worker trusts
	// system roots (unchanged).
	if reloader.HasCA() {
		base := tr.TLSClientConfig
		tr.DialTLSContext = func(dctx context.Context, network, addr string) (net.Conn, error) {
			cfgPerConn := base.Clone()
			cfgPerConn.RootCAs = reloader.CAPool() // fresh per dial → rotated CA picked up
			d := &tls.Dialer{Config: cfgPerConn}
			return d.DialContext(dctx, network, addr)
		}
	}
	return &http.Client{Transport: tr}, nil
}
