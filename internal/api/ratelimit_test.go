package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
)

// TestRateLimiterAllow: a burst-N bucket admits N immediate requests then denies
// the next (per client IP), and a DIFFERENT client is unaffected — the core
// per-IP fairness property.
func TestRateLimiterAllow(t *testing.T) {
	rl := newRateLimiter(1, 3, false) // 1 rps, burst 3
	defer rl.close()
	now := time.Now()

	for i := 0; i < 3; i++ {
		if !rl.allow("1.1.1.1", now) {
			t.Fatalf("request %d within burst must be allowed", i+1)
		}
	}
	if rl.allow("1.1.1.1", now) {
		t.Fatal("4th request past the burst must be denied")
	}
	// A different client has its own bucket.
	if !rl.allow("2.2.2.2", now) {
		t.Fatal("a distinct client IP must not be limited by another's usage")
	}
}

// TestRateLimiterClientIP: the socket peer is used by default; X-Forwarded-For /
// X-Real-IP are honored ONLY with trustProxy set (else forgeable).
func TestRateLimiterClientIP(t *testing.T) {
	withXFF := func(rl *rateLimiter) string {
		r := httptest.NewRequest(http.MethodGet, "/api/v1/resources", nil)
		r.RemoteAddr = "10.0.0.5:5432"
		r.Header.Set("X-Forwarded-For", "203.0.113.9, 10.0.0.1")
		return rl.clientIP(r)
	}

	untrusted := newRateLimiter(1, 1, false)
	defer untrusted.close()
	if got := withXFF(untrusted); got != "10.0.0.5" {
		t.Errorf("without trustProxy, key must be the socket peer host; got %q", got)
	}

	trusted := newRateLimiter(1, 1, true)
	defer trusted.close()
	if got := withXFF(trusted); got != "203.0.113.9" {
		t.Errorf("with trustProxy, key must be the first X-Forwarded-For hop; got %q", got)
	}
}

// TestRateLimitMiddleware429 wires the middleware onto a real (routeless) Huma
// API and drives it through httptest: the first request passes, the second
// (burst 1) gets 429 + Retry-After + an application/problem+json body — the
// exact shape a generated client's error path expects.
func TestRateLimitMiddleware429(t *testing.T) {
	mux := http.NewServeMux()
	api := humago.New(mux, humaConfig())
	rl := newRateLimiter(1, 1, false) // 1 rps, burst 1 → 2nd request denied
	defer rl.close()

	// Register the middleware BEFORE the route — huma captures the chain at
	// Handle time (see buildHandler), so it only applies to later operations.
	api.UseMiddleware(rl.middleware(api))
	// A trivial operation to route to; we only assert the middleware's gate.
	type pingOut struct{}
	huma.Get(api, "/ping", func(context.Context, *struct{}) (*pingOut, error) {
		return &pingOut{}, nil
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	do := func() *http.Response {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/ping", nil)
		req.RemoteAddr = "9.9.9.9:1111"
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		return resp
	}

	first := do()
	first.Body.Close()
	if first.StatusCode == http.StatusTooManyRequests {
		t.Fatal("first request within burst must not be rate limited")
	}

	second := do()
	defer second.Body.Close()
	if second.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second request past the burst must be 429; got %d", second.StatusCode)
	}
	if ra := second.Header.Get("Retry-After"); ra == "" {
		t.Error("a 429 must carry a Retry-After header")
	}
	if ct := second.Header.Get("Content-Type"); ct != "application/problem+json" {
		t.Errorf("a 429 body must be application/problem+json; got %q", ct)
	}
}
