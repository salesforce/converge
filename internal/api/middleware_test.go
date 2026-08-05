package api

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCORSMiddleware(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	do := func(allow []string, origin, method string) *httptest.ResponseRecorder {
		h := corsMiddleware(allow, next)
		req := httptest.NewRequest(method, "/api/resources", nil)
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	t.Run("no allowlist sends no CORS header", func(t *testing.T) {
		rec := do(nil, "https://evil.example", http.MethodGet)
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
			t.Errorf("default (no allowlist) must not set ACAO; got %q", got)
		}
	})

	t.Run("matching origin is reflected, not wildcarded", func(t *testing.T) {
		rec := do([]string{"https://ui.example"}, "https://ui.example", http.MethodGet)
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://ui.example" {
			t.Errorf("ACAO = %q, want the exact origin reflected", got)
		}
		if rec.Header().Get("Vary") != "Origin" {
			t.Error("a reflected origin must set Vary: Origin")
		}
	})

	t.Run("non-matching origin gets no CORS header", func(t *testing.T) {
		rec := do([]string{"https://ui.example"}, "https://evil.example", http.MethodGet)
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
			t.Errorf("an origin not in the allowlist must not be allowed; got %q", got)
		}
	})

	t.Run("literal * allows any", func(t *testing.T) {
		rec := do([]string{"*"}, "https://anything.example", http.MethodGet)
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
			t.Errorf("ACAO = %q, want * for the dev allow-any mode", got)
		}
	})

	t.Run("credentials are never allowed", func(t *testing.T) {
		rec := do([]string{"https://ui.example"}, "https://ui.example", http.MethodGet)
		if rec.Header().Get("Access-Control-Allow-Credentials") != "" {
			t.Error("Allow-Credentials must never be set (keeps cookie CSRF off)")
		}
	})

	t.Run("preflight short-circuits 200", func(t *testing.T) {
		rec := do([]string{"https://ui.example"}, "https://ui.example", http.MethodOptions)
		if rec.Code != http.StatusOK {
			t.Errorf("OPTIONS preflight = %d, want 200", rec.Code)
		}
	})
}

// TestRecoverMiddleware: a panicking handler yields a 500 (not a dropped
// connection / crashed goroutine), and a normal handler passes through
// untouched. http.ErrAbortHandler must propagate (net/http's sanctioned quiet
// abort), not be swallowed as a 500.
func TestRecoverMiddleware(t *testing.T) {
	t.Run("panic becomes 500", func(t *testing.T) {
		h := recoverMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			panic("boom")
		}))
		rec := httptest.NewRecorder()
		// Must not itself panic out of ServeHTTP.
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/resources", nil))
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("panic recovery status = %d, want 500", rec.Code)
		}
	})

	t.Run("normal handler passes through", func(t *testing.T) {
		h := recoverMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusTeapot)
		}))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
		if rec.Code != http.StatusTeapot {
			t.Errorf("passthrough status = %d, want 418", rec.Code)
		}
	})

	t.Run("ErrAbortHandler propagates", func(t *testing.T) {
		h := recoverMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			panic(http.ErrAbortHandler)
		}))
		defer func() {
			if v := recover(); v != http.ErrAbortHandler {
				t.Errorf("ErrAbortHandler must propagate; recovered %v", v)
			}
		}()
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x", nil))
		t.Fatal("expected ErrAbortHandler to propagate")
	})
}

// TestSecurityHeadersMiddleware: baseline hardening headers are set on every
// response, the CSP restricts scripts/connections to same-origin, and HSTS is
// sent ONLY over TLS.
func TestSecurityHeadersMiddleware(t *testing.T) {
	h := securityHeadersMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	t.Run("baseline headers over plain HTTP", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		hdr := rec.Header()
		if got := hdr.Get("X-Content-Type-Options"); got != "nosniff" {
			t.Errorf("X-Content-Type-Options = %q, want nosniff", got)
		}
		if got := hdr.Get("X-Frame-Options"); got != "DENY" {
			t.Errorf("X-Frame-Options = %q, want DENY", got)
		}
		if got := hdr.Get("Referrer-Policy"); got != "no-referrer" {
			t.Errorf("Referrer-Policy = %q, want no-referrer", got)
		}
		csp := hdr.Get("Content-Security-Policy")
		for _, want := range []string{"default-src 'self'", "script-src 'self'", "frame-ancestors 'none'", "object-src 'none'"} {
			if !strings.Contains(csp, want) {
				t.Errorf("CSP missing %q; got %q", want, csp)
			}
		}
		// script-src must NOT allow inline/eval (the bundle is external files).
		if strings.Contains(csp, "script-src 'self' 'unsafe-inline'") || strings.Contains(csp, "'unsafe-eval'") {
			t.Errorf("script-src must stay strict (no unsafe-inline/eval); got %q", csp)
		}
		// No HSTS on plain HTTP (browsers ignore it there; sending it is noise).
		if got := hdr.Get("Strict-Transport-Security"); got != "" {
			t.Errorf("HSTS must not be set over plain HTTP; got %q", got)
		}
	})

	t.Run("HSTS only over TLS", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.TLS = &tls.ConnectionState{} // simulate a TLS-terminated request
		h.ServeHTTP(rec, req)
		if got := rec.Header().Get("Strict-Transport-Security"); got == "" {
			t.Error("HSTS must be set over TLS")
		}
	})
}
