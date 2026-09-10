package runtime

import (
	"context"
	"sync"
	"testing"

	"github.com/salesforce/converge/internal/model"
)

// fakeManifestRepo is an in-memory kindManifestRepo for cache tests (no DB).
type fakeManifestRepo struct {
	mu     sync.Mutex
	byKind map[model.Kind]model.KindManifest
	listN  int // count ListKindManifests calls
	getN   int // count GetKindManifest calls (miss-fills)
}

func newFakeRepo(ms ...model.KindManifest) *fakeManifestRepo {
	r := &fakeManifestRepo{byKind: map[model.Kind]model.KindManifest{}}
	for _, m := range ms {
		r.byKind[m.Kind] = m
	}
	return r
}

func (r *fakeManifestRepo) set(m model.KindManifest) {
	r.mu.Lock()
	r.byKind[m.Kind] = m
	r.mu.Unlock()
}
func (r *fakeManifestRepo) del(kind model.Kind) {
	r.mu.Lock()
	delete(r.byKind, kind)
	r.mu.Unlock()
}

func (r *fakeManifestRepo) ListKindManifests(_ context.Context) ([]model.KindManifest, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listN++
	out := make([]model.KindManifest, 0, len(r.byKind))
	for _, m := range r.byKind {
		out = append(out, m)
	}
	return out, nil
}

// GetKindManifest ignores kindVersion — these are single-kindVersion (v1) cache tests keyed
// by kind, matching the strict-kindVersion default.
func (r *fakeManifestRepo) GetKindManifest(_ context.Context, kind model.Kind, _ int) (model.KindManifest, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.getN++
	m, ok := r.byKind[kind]
	return m, ok, nil
}

func cacheWith(repo kindManifestRepo) *KindManifestCache {
	c := &KindManifestCache{repo: repo}
	c.snap.Store(&kindManifestSnapshot{
		byKind:  map[model.KindVersion]model.KindManifest{},
		checked: map[model.KindVersion]bool{},
	})
	return c
}

// TestKindManifestCacheLoadAndGet: Load populates; Get is a cache hit (no extra DB read).
func TestKindManifestCacheLoadAndGet(t *testing.T) {
	repo := newFakeRepo(model.KindManifest{Kind: "vpc", KindVersion: 1, FinalizerName: "f"})
	c := cacheWith(repo)
	if err := c.Load(context.Background()); err != nil {
		t.Fatalf("load: %v", err)
	}
	m, ok := c.Get(context.Background(), "vpc", 1)
	if !ok || m.FinalizerName != "f" {
		t.Fatalf("Get(vpc) = %+v, %v", m, ok)
	}
	if repo.getN != 0 {
		t.Fatalf("cache hit should not call GetKindManifest; got %d", repo.getN)
	}
}

// TestKindManifestCacheMissFill: a kind not loaded yet is read-through once and cached.
func TestKindManifestCacheMissFill(t *testing.T) {
	repo := newFakeRepo(model.KindManifest{Kind: "account", KindVersion: 1})
	c := cacheWith(repo) // NOT Loaded
	if _, ok := c.Get(context.Background(), "account", 1); !ok {
		t.Fatal("miss-fill should find an existing manifest")
	}
	if repo.getN != 1 {
		t.Fatalf("first miss = 1 DB read; got %d", repo.getN)
	}
	// Second Get is now a cache hit (no further DB read).
	c.Get(context.Background(), "account", 1)
	if repo.getN != 1 {
		t.Fatalf("second Get should be cached; getN=%d", repo.getN)
	}
	// A genuinely-absent kind: miss-fill caches the absence (one read, then cached).
	if _, ok := c.Get(context.Background(), "ghost", 1); ok {
		t.Fatal("absent kind should report ok=false")
	}
	c.Get(context.Background(), "ghost", 1)
	if repo.getN != 2 {
		t.Fatalf("absent kind cached after one read; getN=%d", repo.getN)
	}
}

// TestKindManifestCacheReloadDropsRemoved: reload replaces the map, so a deleted
// manifest disappears and a new one appears.
func TestKindManifestCacheReloadDropsRemoved(t *testing.T) {
	repo := newFakeRepo(model.KindManifest{Kind: "a", KindVersion: 1}, model.KindManifest{Kind: "b", KindVersion: 1})
	c := cacheWith(repo)
	_ = c.Load(context.Background())

	repo.del("a")
	repo.set(model.KindManifest{Kind: "c", KindVersion: 1})
	if err := c.reload(context.Background()); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if _, ok := c.Get(context.Background(), "a", 1); ok {
		t.Fatal("removed manifest 'a' should be gone after reload")
	}
	if _, ok := c.Get(context.Background(), "c", 1); !ok {
		t.Fatal("new manifest 'c' should appear after reload")
	}
}
