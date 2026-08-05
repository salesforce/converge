package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"time"
)

// routemesh.go builds the transport for the broker-to-broker MESH (the persistent
// bidi Route streams). Unlike the unary relay it replaced, the mesh REQUIRES
// end-to-end HTTP/2: Connect bidi streaming does not work over HTTP/1.1. The plain
// (non-TLS) dev/local path therefore needs h2c (HTTP/2 cleartext) on BOTH ends —
// the bare http.Server + http.DefaultClient the unary relay silently rode on
// HTTP/1.1 would break bidi at runtime.
//
// We use the Go 1.24+ STANDARD LIBRARY http.Protocols to enable cleartext HTTP/2
// (SetUnencryptedHTTP2) — no third-party dependency (no x/net/http2/h2c, which is
// now deprecated in favor of this). Connect rides plain net/http, so a Protocols
// with UnencryptedHTTP2 on the Server/Transport is all that's needed:
//   - routeHTTPClient: the mesh client (h2c for plaintext peers, ALPN-h2 for TLS).
//   - configureBrokerH2C: flips a plaintext broker server to accept cleartext HTTP/2.
//   - assertRouteH2: a startup fail-fast probe that a mesh dial negotiates HTTP/2.

// routeHTTPClient builds the HTTP/2 client the mesh dials peer brokers with.
//
//   - TLS peers (certFile/keyFile set): standard HTTP/2 over the worker's mTLS
//     keypair (ALPN "h2"), verifying the peer against caFile (or system roots).
//   - plaintext peers: an http.Transport with Protocols=UnencryptedHTTP2 (h2c) and
//     HTTP/1 disabled, so every dial is prior-knowledge cleartext HTTP/2. This is
//     what makes the dev/local + demo multi-broker mesh work at all.
//
// The transport pools a warm connection per host and multiplexes streams over it
// (the "one persistent conn per peer" the design wants).
func routeHTTPClient(certFile, keyFile, caFile string) (*http.Client, error) {
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("mesh: load client keypair: %w", err)
		}
		tlsCfg := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
			NextProtos:   []string{"h2"}, // prefer HTTP/2 over TLS (ALPN)
		}
		if caFile != "" {
			pem, err := os.ReadFile(caFile)
			if err != nil {
				return nil, fmt.Errorf("mesh: read peer CA %q: %w", caFile, err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("mesh: peer CA %q: no certs parsed", caFile)
			}
			tlsCfg.RootCAs = pool
		}
		var protos http.Protocols
		protos.SetHTTP2(true)
		tr := &http.Transport{TLSClientConfig: tlsCfg, Protocols: &protos, ForceAttemptHTTP2: true}
		return &http.Client{Transport: tr}, nil
	}
	// Plaintext: cleartext HTTP/2 (h2c) via stdlib Protocols, HTTP/1 off so every
	// dial is prior-knowledge h2c.
	var protos http.Protocols
	protos.SetUnencryptedHTTP2(true)
	tr := &http.Transport{Protocols: &protos}
	return &http.Client{Transport: tr}, nil
}

// configureBrokerH2C makes a plaintext broker server accept cleartext HTTP/2 (h2c)
// so the bidi Route stream works — the bare http.Server would speak only HTTP/1.1
// and break bidi. Keeps HTTP/1 enabled too so nothing else regresses. No-op shape
// on the TLS path (that server negotiates h2 via ALPN from its cert config).
func configureBrokerH2C(srv *http.Server) {
	var protos http.Protocols
	protos.SetHTTP1(true)
	protos.SetHTTP2(true)
	protos.SetUnencryptedHTTP2(true)
	srv.Protocols = &protos
}

// assertRouteH2 fails fast at startup if the mesh transport does not negotiate
// HTTP/2 to its own broker listener — turning a silent HTTP/1.1 downgrade (which
// breaks bidi Route at runtime) into a visible error. selfAddr is this broker's own
// dial-able address. Best-effort: a not-yet-listening self address (boot race) is
// tolerated — the probe only FAILS on a definitive non-H2 negotiation.
func assertRouteH2(ctx context.Context, client *http.Client, selfAddr string) error {
	if selfAddr == "" {
		return nil // no advertise address (single-broker / mesh off): nothing to assert
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, selfAddr+"/", nil)
	if err != nil {
		return fmt.Errorf("mesh H2 self-probe: build request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil // connection refused / not listening yet: a boot race, not a protocol failure
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.ProtoMajor != 2 {
		return fmt.Errorf("mesh transport negotiated HTTP/%d.%d to %s, not HTTP/2 — bidi Route requires HTTP/2 (check h2c/ALPN wiring)",
			resp.ProtoMajor, resp.ProtoMinor, selfAddr)
	}
	return nil
}
