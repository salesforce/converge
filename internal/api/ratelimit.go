package api

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"golang.org/x/time/rate"
)

// Per-client API rate limiting (token bucket, golang.org/x/time/rate).
//
// It is a HUMA middleware, so it runs on every registered API operation
// (/api/v1/*), INCLUDING the raw-bytes downloads (which are huma StreamResponse
// operations — a large manifest pull is a heavy DB read, worth a token). The
// k8s probes (/livez /readyz /healthz), the OpenAPI docs, and the SPA are plain
// mux handlers that bypass Huma, so they are exempt by construction with no
// path-prefix checks — exactly the scope we want (limit the real API surface;
// never throttle a kubelet probe).
//
// One token bucket PER CLIENT IP so a single noisy client can't starve others.
// Buckets live in a bounded map behind a mutex, swept by an idle-eviction
// janitor so the map can't grow without limit under IP churn (the 10M-scale
// discipline: no unbounded per-entity state). Disabled by default (rps <= 0):
// the middleware is simply never registered, so there is zero cost unless opted
// in — same discipline as CORS.

// rateLimiter holds a token bucket per client IP + the shared rate/burst.
type rateLimiter struct {
	rps   rate.Limit
	burst int
	// trustProxy: when true, derive the client IP from X-Forwarded-For /
	// X-Real-IP (set by a fronting LB/ingress); otherwise use the socket peer.
	// Off by default because a direct client can forge those headers to dodge
	// the limit — only trust them when a proxy is known to overwrite them.
	trustProxy bool

	mu       sync.Mutex
	buckets  map[string]*bucket
	stop     chan struct{}
	stopOnce sync.Once
}

// bucket is one client's limiter plus its last-seen time for idle eviction.
type bucket struct {
	lim  *rate.Limiter
	seen time.Time
}

// rateLimiterIdleTTL is how long a client's bucket survives with no requests
// before the janitor evicts it. Generous relative to any burst window; its only
// job is to bound the map under IP churn, not to reset limits.
const rateLimiterIdleTTL = 10 * time.Minute

// rateLimiterSweepEvery is the janitor cadence.
const rateLimiterSweepEvery = time.Minute

// newRateLimiter builds a limiter and starts its idle-eviction janitor. Caller
// must call stop() (wired to the server lifecycle) to end the janitor goroutine.
func newRateLimiter(rps float64, burst int, trustProxy bool) *rateLimiter {
	if burst <= 0 {
		burst = 1
	}
	rl := &rateLimiter{
		rps:        rate.Limit(rps),
		burst:      burst,
		trustProxy: trustProxy,
		buckets:    make(map[string]*bucket),
		stop:       make(chan struct{}),
	}
	go rl.janitor()
	return rl
}

// close ends the janitor goroutine. Idempotent.
func (rl *rateLimiter) close() { rl.stopOnce.Do(func() { close(rl.stop) }) }

// janitor periodically evicts buckets untouched for longer than the idle TTL, so
// the map stays bounded even under a churn of distinct client IPs.
func (rl *rateLimiter) janitor() {
	t := time.NewTicker(rateLimiterSweepEvery)
	defer t.Stop()
	for {
		select {
		case <-rl.stop:
			return
		case <-t.C:
			cutoff := time.Now().Add(-rateLimiterIdleTTL)
			rl.mu.Lock()
			for ip, b := range rl.buckets {
				if b.seen.Before(cutoff) {
					delete(rl.buckets, ip)
				}
			}
			rl.mu.Unlock()
		}
	}
}

// allow reports whether a request from clientIP may proceed, consuming one token
// from that client's bucket (creating it on first sight).
func (rl *rateLimiter) allow(clientIP string, now time.Time) bool {
	rl.mu.Lock()
	b := rl.buckets[clientIP]
	if b == nil {
		b = &bucket{lim: rate.NewLimiter(rl.rps, rl.burst)}
		rl.buckets[clientIP] = b
	}
	b.seen = now
	lim := b.lim
	rl.mu.Unlock()
	// Reserve outside the map lock; rate.Limiter is itself concurrency-safe.
	return lim.Allow()
}

// clientIP extracts the rate-limit key for a request. With trustProxy set it
// honors the first X-Forwarded-For hop (else X-Real-IP); otherwise it uses the
// socket peer address. Falls back to the raw value if host:port splitting fails.
func (rl *rateLimiter) clientIP(r *http.Request) string {
	if rl.trustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			// First hop is the original client (LB appends downstream).
			first, _, _ := strings.Cut(xff, ",")
			return strings.TrimSpace(first)
		}
		if xr := r.Header.Get("X-Real-IP"); xr != "" {
			return strings.TrimSpace(xr)
		}
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

// middleware returns the Huma middleware that enforces the limit. api is the
// huma.API the server registers routes on — needed to emit an RFC7807
// problem+json 429 via huma.WriteErr (so the body matches every other API error
// and a generated client renders it cleanly).
func (rl *rateLimiter) middleware(api huma.API) func(huma.Context, func(huma.Context)) {
	return func(ctx huma.Context, next func(huma.Context)) {
		r, _ := humago.Unwrap(ctx)
		if rl.allow(rl.clientIP(r), time.Now()) {
			next(ctx)
			return
		}
		ctx.SetHeader("Retry-After", "1")
		_ = huma.WriteErr(api, ctx, http.StatusTooManyRequests,
			"API rate limit exceeded; retry after a moment")
	}
}
