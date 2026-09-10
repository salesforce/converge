package api

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"time"
)

// HTTP middleware applied around every handler in Server.Handler():
// request-context deadline, gzip request decompression, gzip response
// compression, and (opt-in) CORS.

// requestTimeout bounds how long any single API request may run. It deadlines
// the request CONTEXT, so every context-aware DB call / provider roundtrip a
// handler makes is cancelled at the ceiling — preventing an expensive or
// pathological query (e.g. a topology aggregation over a huge subtree) from
// pinning a DB connection indefinitely. It is a context cancel, not an
// http.TimeoutHandler write-deadline, so it composes with gzip streaming and
// doesn't buffer responses.
const requestTimeout = 30 * time.Second

func timeoutMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// apiVersionMiddleware stamps X-API-Version on every response so a client can
// detect which API version the server speaks (and alert on a mismatch
// after an upgrade) without parsing the URL. The version lives in the path
// (apiPrefix); this header just advertises it on the wire.
func apiVersionMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-API-Version", APIVersion)
		next.ServeHTTP(w, r)
	})
}

// recoverMiddleware turns a panic in any inner middleware or handler into a
// clean 500 instead of crashing the request goroutine (and dropping the
// connection with no response). It logs the panic + stack once. Placed
// OUTERMOST so it catches panics from every layer below it. If the response
// headers were already written when the panic fired, we can't change the status
// — we just log and let the (now-truncated) response close; before that point
// we send a bare 500.
func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				// http.ErrAbortHandler is the sanctioned "abort this request
				// quietly" panic (e.g. a hijacked/streamed conn) — re-panic so
				// net/http handles it as intended, don't log it as a crash.
				if err, ok := v.(error); ok && errors.Is(err, http.ErrAbortHandler) {
					panic(v)
				}
				slog.Error("panic in HTTP handler; recovered",
					"method", r.Method, "path", r.URL.Path, "panic", v,
					"stack", string(debug.Stack()))
				// Best-effort 500. WriteHeader is a no-op (with a warning) if the
				// handler already wrote a status, which is the correct fallback.
				w.WriteHeader(http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// securityHeadersMiddleware sets baseline hardening headers on every response.
// The API is JSON + an embedded same-origin SPA (React/Vite build) served from
// "/", so:
//
//   - X-Content-Type-Options: nosniff — never MIME-sniff a response body.
//   - X-Frame-Options: DENY + frame-ancestors 'none' — the UI can't be framed
//     (clickjacking defense); it's a full-page control-plane console, never an
//     embed.
//   - Referrer-Policy: no-referrer — resource ids in the path never leak to a
//     third party via the Referer header.
//   - Content-Security-Policy tuned for the built SPA: scripts + connections are
//     same-origin only (the bundle is /assets/*.js, the API is same-origin);
//     styles allow 'unsafe-inline' (React/Tailwind set element styles at
//     runtime — scripts do NOT need it, so script-src stays strict); img/font
//     allow data: (the CSS inlines small assets as data URIs). object-src 'none'
//     and base-uri 'self' close the classic injection vectors.
//
// HSTS is added ONLY over TLS (an HSTS header on plain HTTP is ignored by
// browsers and is meaningless), gated by the request being served over TLS.
func securityHeadersMiddleware(next http.Handler) http.Handler {
	const csp = "default-src 'self'; " +
		"script-src 'self'; " +
		"style-src 'self' 'unsafe-inline'; " +
		"img-src 'self' data:; " +
		"font-src 'self' data:; " +
		"connect-src 'self'; " +
		"object-src 'none'; " +
		"base-uri 'self'; " +
		"frame-ancestors 'none'"
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Content-Security-Policy", csp)
		if r.TLS != nil {
			// 1 year, include subdomains; only meaningful (and only sent) over TLS.
			h.Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		}
		next.ServeHTTP(w, r)
	})
}

type gzipResponseWriter struct {
	http.ResponseWriter
	gz *gzip.Writer
}

func (g *gzipResponseWriter) WriteHeader(code int) {
	g.ResponseWriter.Header().Del("Content-Length")
	g.ResponseWriter.WriteHeader(code)
}

func (g *gzipResponseWriter) Write(p []byte) (int, error) { return g.gz.Write(p) }

func decompressRequestMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
			gz, err := gzip.NewReader(r.Body)
			if err != nil {
				http.Error(w, "bad gzip body", http.StatusBadRequest)
				return
			}
			r.Body = io.NopCloser(gz)
			r.Header.Del("Content-Encoding")
			r.Header.Del("Content-Length")
		}
		next.ServeHTTP(w, r)
	})
}

func gzipMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		// Flush-close the gzip stream; the response is already streaming to the
		// client, so a close error here is unrecoverable and is deliberately discarded.
		defer func() { _ = gz.Close() }()
		next.ServeHTTP(&gzipResponseWriter{ResponseWriter: w, gz: gz}, r)
	})
}

// corsMiddleware applies CORS ONLY when an allowlist is configured. With an
// empty allowlist (the default) it sends no CORS headers at all, so the API is
// same-origin only — the embedded SPA is served from this same origin and needs
// none, and a permissive default would let any web page drive the (currently
// unauthenticated) state-changing endpoints. A non-empty allowlist reflects the
// request's Origin back ONLY on an exact match (never a blanket "*" with the
// request echoed), or honors the single literal "*" entry for dev/no-credential
// use. Either way Allow-Credentials is never set, so cookie-based CSRF stays off.
func corsMiddleware(allowedOrigins []string, next http.Handler) http.Handler {
	allowAny := len(allowedOrigins) == 1 && allowedOrigins[0] == "*"
	allowed := make(map[string]struct{}, len(allowedOrigins))
	for _, o := range allowedOrigins {
		allowed[o] = struct{}{}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		// No allowlist, or a request with no Origin → no CORS headers; pass through.
		if len(allowedOrigins) == 0 || origin == "" {
			if r.Method == http.MethodOptions && len(allowedOrigins) == 0 {
				// No CORS configured: still answer a bare preflight so a same-origin
				// OPTIONS doesn't fall through to a handler that rejects it.
				w.WriteHeader(http.StatusOK)
				return
			}
			next.ServeHTTP(w, r)
			return
		}
		if _, ok := allowed[origin]; ok || allowAny {
			if allowAny {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			} else {
				// Reflect the matched origin (not "*") and Vary so caches don't
				// serve one origin's response to another.
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Add("Vary", "Origin")
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Encoding")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}
