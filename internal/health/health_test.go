package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// fakePinger drives the /readyz path without a real database.
type fakePinger struct{ err error }

func (f fakePinger) Ping(context.Context) error { return f.err }

func get(t *testing.T, h http.Handler, path string) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

func TestLivenessIsDBFree(t *testing.T) {
	// Liveness must stay 200 even when the DB is down — a DB blip must not
	// trigger a liveness restart.
	h := Handler(fakePinger{err: errors.New("db down")}, nil)
	for _, path := range []string{"/livez", "/healthz"} {
		if code := get(t, h, path); code != http.StatusOK {
			t.Errorf("%s: got %d, want 200 (liveness must ignore DB)", path, code)
		}
	}
}

func TestReadyzReflectsDB(t *testing.T) {
	if code := get(t, Handler(fakePinger{}, nil), "/readyz"); code != http.StatusOK {
		t.Errorf("/readyz healthy: got %d, want 200", code)
	}
	if code := get(t, Handler(fakePinger{err: errors.New("db down")}, nil), "/readyz"); code != http.StatusServiceUnavailable {
		t.Errorf("/readyz with DB down: got %d, want 503", code)
	}
}

func TestReadyzNilPingerIsLiveness(t *testing.T) {
	// A nil pool degrades /readyz to a pure liveness check (always ready).
	if code := get(t, Handler(nil, nil), "/readyz"); code != http.StatusOK {
		t.Errorf("/readyz with nil pinger: got %d, want 200", code)
	}
}

// TestReadyzDrainingIsNotReady proves SetDraining flips /readyz to 503 (so k8s
// pulls the pod from endpoints during shutdown) WITHOUT touching liveness (so the
// kubelet doesn't SIGKILL the draining process). The 503 wins even over a healthy
// DB ping — it's checked first.
func TestReadyzDrainingIsNotReady(t *testing.T) {
	draining.Store(false) // isolate from any other test that set it
	t.Cleanup(func() { draining.Store(false) })

	h := Handler(fakePinger{}, nil) // DB is healthy
	if code := get(t, h, "/readyz"); code != http.StatusOK {
		t.Fatalf("/readyz before drain: got %d, want 200", code)
	}
	SetDraining()
	if code := get(t, h, "/readyz"); code != http.StatusServiceUnavailable {
		t.Errorf("/readyz while draining: got %d, want 503", code)
	}
	// Liveness must STAY 200 so the kubelet keeps the process alive to drain.
	if code := get(t, h, "/livez"); code != http.StatusOK {
		t.Errorf("/livez while draining: got %d, want 200 (must not SIGKILL a draining pod)", code)
	}
}
