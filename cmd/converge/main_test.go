package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/salesforce/converge/internal/engine"
	"github.com/salesforce/converge/internal/runtime"
)

// TestApplyPoolHealthDefaultsFloorsMaxConns proves the pool never runs below
// minPoolMaxConns when the operator left PG_POOL_MAX_CONNS unset — the fix for the
// silent pool-exhaustion wedge, where pgx's own max(4, NumCPU) default on a small node
// fell at/below the count of long-lived LISTEN subscribers a pod pins, so every working
// query blocked forever in Acquire. An explicit env override always wins, even if lower.
func TestApplyPoolHealthDefaultsFloorsMaxConns(t *testing.T) {
	parse := func(t *testing.T) *pgxpool.Config {
		t.Helper()
		p, err := pgxpool.ParseConfig("postgres://u:p@localhost:5432/db?sslmode=disable")
		if err != nil {
			t.Fatalf("ParseConfig: %v", err)
		}
		return p
	}

	// Env unset + a low resolved MaxConns (simulating pgx's max(4,NumCPU) on a small
	// node) → floored up to minPoolMaxConns.
	low := parse(t)
	low.MaxConns = 4
	applyPoolHealthDefaults(low, Config{}, nil)
	if low.MaxConns != minPoolMaxConns {
		t.Errorf("env unset, MaxConns=4 → got %d, want floor %d", low.MaxConns, minPoolMaxConns)
	}

	// Env unset but the resolved value already clears the floor → left untouched.
	high := parse(t)
	high.MaxConns = 50
	applyPoolHealthDefaults(high, Config{}, nil)
	if high.MaxConns != 50 {
		t.Errorf("env unset, MaxConns=50 → got %d, want 50 (floor must not lower it)", high.MaxConns)
	}

	// An explicit env override wins even below the floor — the operator owns the sizing.
	override := parse(t)
	override.MaxConns = 4
	applyPoolHealthDefaults(override, Config{PgPoolMaxConns: 8}, nil)
	if override.MaxConns != 8 {
		t.Errorf("PG_POOL_MAX_CONNS=8 → got %d, want 8 (explicit override must win)", override.MaxConns)
	}
}

// TestApplyDefaultsFillsZeroKnobs proves an operator who sets no env var gets the
// SAME effective default the driver would apply — sourced from the owning
// package's exported constant — so the reported cluster config shows the real
// running value, never "0s". An explicit (non-zero) value is preserved.
func TestApplyDefaultsFillsZeroKnobs(t *testing.T) {
	// All-zero (nothing overridden) → every knob filled from its constant.
	var c Config
	c.applyDefaults()
	cases := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"HeartbeatEvery", c.HeartbeatEvery, runtime.DefaultHeartbeatEvery},
		{"WorkerPollMin", c.WorkerPollMin, runtime.DefaultPollMin},
		{"WorkerPollMax", c.WorkerPollMax, runtime.DefaultPollMax},
		{"SweeperStaleAfter", c.SweeperStaleAfter, runtime.DefaultSweeperStaleAfter},
		{"SweeperUnclaimedDeleteAfter", c.SweeperUnclaimedDeleteAfter, runtime.DefaultSweeperUnclaimedDeleteAfter},
		{"RetryAfter", c.RetryAfter, engine.DefaultRetryAfter},
		{"SweeperInterval", c.SweeperInterval, engine.DefaultSweeperInterval},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("applyDefaults left %s = %v, want %v", tc.name, tc.got, tc.want)
		}
		if tc.got == 0 {
			t.Errorf("applyDefaults left %s at zero — the report would show 0s", tc.name)
		}
	}

	// An explicit override survives applyDefaults (not clobbered by the default).
	override := Config{HeartbeatEvery: 12 * time.Second}
	override.applyDefaults()
	if override.HeartbeatEvery != 12*time.Second {
		t.Errorf("applyDefaults clobbered an explicit HeartbeatEvery: got %v, want 12s", override.HeartbeatEvery)
	}
}

// TestMemberConfigJSONReportsRealValues proves the cluster-view config blob a
// broker publishes carries the RESOLVED knobs (human durations, not "0s") once
// applyDefaults has run — the bug this fixed was reporting the un-defaulted zero
// struct. It also confirms connect_addr is advertised alongside.
func TestMemberConfigJSONReportsRealValues(t *testing.T) {
	c := Config{RelayAdvertiseAddr: "http://broker-2:9090", PgPoolMaxConns: 20, PgPoolMinConns: 2}
	c.applyDefaults()

	var m map[string]string
	if err := json.Unmarshal(memberConfigJSON(c), &m); err != nil {
		t.Fatalf("memberConfigJSON not valid JSON: %v", err)
	}
	// No duration knob may serialize as the un-defaulted "0s".
	for _, k := range []string{
		"heartbeat_every", "worker_poll_min", "worker_poll_max",
		"sweeper_stale_after", "sweeper_interval", "sweeper_unclaimed_delete_after", "retry_after",
	} {
		if m[k] == "" {
			t.Errorf("config key %q missing", k)
		}
		if m[k] == "0s" {
			t.Errorf("config key %q still reports 0s (un-defaulted)", k)
		}
	}
	// Spot-check a couple of the real values.
	if m["heartbeat_every"] != runtime.DefaultHeartbeatEvery.String() {
		t.Errorf("heartbeat_every = %q, want %q", m["heartbeat_every"], runtime.DefaultHeartbeatEvery.String())
	}
	if m["retry_after"] != engine.DefaultRetryAfter.String() {
		t.Errorf("retry_after = %q, want %q", m["retry_after"], engine.DefaultRetryAfter.String())
	}
	// Pool sizes reflect the values set on cfg (resolved off the pool in run()).
	if m["pg_pool_max_conns"] != "20" || m["pg_pool_min_conns"] != "2" {
		t.Errorf("pool sizes = %q/%q, want 20/2", m["pg_pool_max_conns"], m["pg_pool_min_conns"])
	}
	// connect_addr is advertised for the mesh.
	if m["connect_addr"] != "http://broker-2:9090" {
		t.Errorf("connect_addr = %q, want the advertised URL", m["connect_addr"])
	}
}

// TestParseRole covers ROLE resolution into (runControl, runBroker). converge is
// ONLY control and/or broker — never a worker — so it carries no provider code
// and a broker always claims every manifested kind. "all" = control + broker;
// "control" = sweepers only (no claiming); "broker" = claim every kind; the
// removed worker roles and any unknown role fail fast at boot.
func TestParseRole(t *testing.T) {
	tests := []struct {
		name        string
		role        string
		wantControl bool
		wantBroker  bool
		wantErr     bool
	}{
		{name: "empty defaults to all", role: "", wantControl: true, wantBroker: true},
		{name: "all", role: "all", wantControl: true, wantBroker: true},
		{name: "control claims nothing", role: "control", wantControl: true, wantBroker: false},
		{name: "broker claims all kinds", role: "broker", wantControl: false, wantBroker: true},
		{name: "worker role removed", role: "worker", wantErr: true},
		{name: "worker-kind role removed", role: "worker-account", wantErr: true},
		{name: "broker-kind role removed", role: "broker-account", wantErr: true},
		{name: "unknown role rejected", role: "controller", wantErr: true},
		{name: "bare kind rejected", role: "account", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseRole(tc.role)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.runControl != tc.wantControl {
				t.Errorf("runControl = %v, want %v", got.runControl, tc.wantControl)
			}
			if got.runBroker != tc.wantBroker {
				t.Errorf("runBroker = %v, want %v", got.runBroker, tc.wantBroker)
			}
		})
	}
}

// TestParseListenAddr covers LISTEN_ADDR resolution: bare host:port stays
// plain HTTP (backward compatible), the scheme picks the protocol, and the
// scheme is validated against TLS_CERT_FILE/TLS_KEY_FILE so a
// misconfiguration fails fast instead of silently serving the wrong one.
func TestParseListenAddr(t *testing.T) {
	const cert, key, ca = "/tls/server.crt", "/tls/server.key", "/tls/client-ca.crt"

	tests := []struct {
		name       string
		raw        string
		certFile   string
		keyFile    string
		caFile     string
		wantAddr   string
		wantTLS    bool
		wantCAFile string
		wantErr    bool
	}{
		// Bare host:port — historical form, plain HTTP.
		{name: "bare port", raw: ":8080", wantAddr: ":8080"},
		{name: "bare host port", raw: "0.0.0.0:8080", wantAddr: "0.0.0.0:8080"},
		{name: "bare ipv4 host port", raw: "127.0.0.1:9000", wantAddr: "127.0.0.1:9000"},
		// Explicit http:// scheme.
		{name: "http scheme", raw: "http://0.0.0.0:8080", wantAddr: "0.0.0.0:8080"},
		{name: "http scheme bare port", raw: "http://:8080", wantAddr: ":8080"},
		// https:// with both TLS files → TLS.
		{name: "https with cert+key", raw: "https://0.0.0.0:8443", certFile: cert, keyFile: key, wantAddr: "0.0.0.0:8443", wantTLS: true},
		// https:// with cert+key+CA → mTLS (CA carried through).
		{name: "https with client CA (mTLS)", raw: "https://0.0.0.0:8443", certFile: cert, keyFile: key, caFile: ca, wantAddr: "0.0.0.0:8443", wantTLS: true, wantCAFile: ca},
		// https:// missing cert/key → error.
		{name: "https missing both", raw: "https://0.0.0.0:8443", wantErr: true},
		{name: "https missing key", raw: "https://0.0.0.0:8443", certFile: cert, wantErr: true},
		{name: "https missing cert", raw: "https://0.0.0.0:8443", keyFile: key, wantErr: true},
		// http/bare WITH TLS files → error (misconfig).
		{name: "http with tls files", raw: "http://0.0.0.0:8080", certFile: cert, keyFile: key, wantErr: true},
		{name: "bare with tls files", raw: ":8080", certFile: cert, keyFile: key, wantErr: true},
		// Client CA without https → error (mTLS would silently not apply).
		{name: "http with client CA", raw: "http://0.0.0.0:8080", caFile: ca, wantErr: true},
		{name: "bare with client CA", raw: ":8080", caFile: ca, wantErr: true},
		// Malformed / unsupported.
		{name: "empty", raw: "", wantErr: true},
		{name: "unknown scheme", raw: "ftp://0.0.0.0:21", wantErr: true},
		{name: "scheme no host", raw: "https://", certFile: cert, keyFile: key, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseListenAddr(tc.raw, tc.certFile, tc.keyFile, tc.caFile)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.addr != tc.wantAddr {
				t.Errorf("addr = %q, want %q", got.addr, tc.wantAddr)
			}
			if got.tls != tc.wantTLS {
				t.Errorf("tls = %v, want %v", got.tls, tc.wantTLS)
			}
			if tc.wantTLS && (got.certFile != tc.certFile || got.keyFile != tc.keyFile) {
				t.Errorf("cert/key = %q/%q, want %q/%q", got.certFile, got.keyFile, tc.certFile, tc.keyFile)
			}
			if got.clientCAFile != tc.wantCAFile {
				t.Errorf("clientCAFile = %q, want %q", got.clientCAFile, tc.wantCAFile)
			}
		})
	}
}
