package runtime

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// fakeConfigRepo is the narrow configRepo seam: it serves one doc per (kind, kindVersion),
// set by the test, so reload can run with no database. The tests exercise v1, so the
// docs map is keyed by kind and every lookup is at kindVersion 1.
type fakeConfigRepo struct {
	docs map[model.Kind]json.RawMessage
}

func (f fakeConfigRepo) GetDefaultProviderConfig(_ context.Context, k model.Kind, _ int) (store.DefaultProviderConfig, bool, error) {
	d, ok := f.docs[k]
	return store.DefaultProviderConfig{Spec: d}, ok, nil
}

func (f fakeConfigRepo) ConfiguredKindVersions(context.Context) ([]model.KindVersion, error) {
	out := make([]model.KindVersion, 0, len(f.docs))
	for k := range f.docs {
		out = append(out, model.KindVersion{Kind: k, Version: 1})
	}
	return out, nil
}

// newTestPlane builds a ProviderConfigCache backed by a fake repo — the shape
// NewProviderConfigCache builds, but with no pool/DB so reload can be exercised in
// isolation. The fixed kinds are tracked at v1 (static mode).
func newTestPlane(repo configRepo, kinds ...model.Kind) *ProviderConfigCache {
	return &ProviderConfigCache{
		repo:                   repo,
		kinds:                  kinds,
		providerConfigSnapshot: providerConfigSnapshot{current: make(map[model.KindVersion]store.DefaultProviderConfig, len(kinds))},
	}
}

// TestConfigReadsLiveAfterReload is the regression for the original cold-start bug:
// a read of Config(kind, kindVersion) BEFORE the config exists returns nil, and must
// observe the document the instant a reload picks it up — the snapshot reads the plane's
// `current` through to whatever the latest reload stored, no push/channel/re-delivery.
func TestConfigReadsLiveAfterReload(t *testing.T) {
	docs := map[model.Kind]json.RawMessage{} // empty: no config yet (cold start)
	repo := fakeConfigRepo{docs: docs}
	cp := newTestPlane(repo, "k")

	// Before any config exists, a read is nil.
	cp.reload(context.Background())
	if cp.Config("k", 1) != nil {
		t.Fatalf("Config must read nil while no config exists, got %s", cp.Config("k", 1))
	}

	// Operator applies the default config; the next reload picks it up and a read
	// now returns it — live, with nothing pushed.
	doc := json.RawMessage(`{"state_bucket":"b","state_region":"r"}`)
	docs["k"] = doc
	cp.reload(context.Background())
	if string(cp.Config("k", 1)) != string(doc) {
		t.Fatalf("Config must read the live doc after reload; got %s", cp.Config("k", 1))
	}

	// And an edit is observed on the next reload — never stale.
	doc2 := json.RawMessage(`{"state_bucket":"b2","state_region":"r"}`)
	docs["k"] = doc2
	cp.reload(context.Background())
	if string(cp.Config("k", 1)) != string(doc2) {
		t.Fatalf("Config must reflect a later edit; got %s", cp.Config("k", 1))
	}
}

// TestReloadNudgesRetrierOnlyOnChange: reloadAndSignal must fire the
// OnDefaultChange retrier nudge ONCE per actual edit, not on every sweep —
// otherwise a steady tick would needlessly kick every pending Setup.
func TestReloadNudgesRetrierOnlyOnChange(t *testing.T) {
	docs := map[model.Kind]json.RawMessage{"k": json.RawMessage(`{"v":1}`)}
	repo := fakeConfigRepo{docs: docs}
	cp := newTestPlane(repo, "k")

	var nudges int
	cp.OnDefaultChange = func() { nudges++ }

	cp.reloadAndSignal(context.Background()) // first observe → change → 1 nudge
	cp.reloadAndSignal(context.Background()) // unchanged → no nudge
	cp.reloadAndSignal(context.Background()) // unchanged → no nudge
	if nudges != 1 {
		t.Fatalf("retrier nudge must fire once on the initial change, got %d", nudges)
	}

	docs["k"] = json.RawMessage(`{"v":2}`) // an edit
	cp.reloadAndSignal(context.Background())
	if nudges != 2 {
		t.Fatalf("an edit must fire one more nudge, got %d", nudges)
	}
}

// TestOnKindConfigChangeFiresPerChangedKind: the per-(kind, kindVersion) push hook fires
// ONCE per pair whose default changed (with the new doc), and NOT for unchanged
// pairs — this is what the broker wires to broadcast a config update to its
// connected workers, so it must be precise (no spurious pushes, no misses).
func TestOnKindConfigChangeFiresPerChangedKind(t *testing.T) {
	docs := map[model.Kind]json.RawMessage{
		"a": json.RawMessage(`{"v":1}`),
		"b": json.RawMessage(`{"v":1}`),
	}
	repo := fakeConfigRepo{docs: docs}
	cp := newTestPlane(repo, "a", "b")

	got := map[model.Kind]string{}
	cp.OnKindConfigChange = func(k model.Kind, _ int, spec, _ []byte) { got[k] = string(spec) }

	cp.reload(context.Background()) // both first-observed → both fire
	if got["a"] != `{"v":1}` || got["b"] != `{"v":1}` {
		t.Fatalf("both kinds should fire on first observe, got %v", got)
	}

	got = map[model.Kind]string{}
	docs["a"] = json.RawMessage(`{"v":2}`) // only a edited
	cp.reload(context.Background())
	if got["a"] != `{"v":2}` {
		t.Fatalf("edited kind a must fire with the new doc, got %q", got["a"])
	}
	if _, fired := got["b"]; fired {
		t.Fatalf("unchanged kind b must NOT fire a push, got %v", got)
	}
}

// TestDynamicReloadFiresDeletionOnKindRemoval: in DYNAMIC mode, when a (kind, kindVersion)'s
// default is deleted it drops out of the re-discovered ConfiguredKindVersions set —
// reload must fire a DELETION push (empty doc + empty bundle) and drop it from
// `current`, so workers clear it instead of serving the stale config/bundle forever.
func TestDynamicReloadFiresDeletionOnKindRemoval(t *testing.T) {
	docs := map[model.Kind]json.RawMessage{
		"keep": json.RawMessage(`{"v":1}`),
		"drop": json.RawMessage(`{"v":1}`),
	}
	repo := fakeConfigRepo{docs: docs}
	cp := newTestPlane(repo) // no fixed kinds…
	cp.dynamic = true        // …discover them live from ConfiguredKindVersions each reload

	del := map[model.Kind]int{}
	cp.OnKindConfigChange = func(k model.Kind, _ int, spec, data []byte) {
		if len(spec) == 0 && len(data) == 0 { // deletion = both axes empty
			del[k]++
		}
	}

	cp.reload(context.Background()) // both first-observed
	if cp.Config("drop", 1) == nil {
		t.Fatal("drop should be served after the first reload")
	}

	delete(docs, "drop")            // operator deletes the "drop" kind's default
	cp.reload(context.Background()) // re-discovery no longer includes "drop"

	if del["drop"] != 1 {
		t.Fatalf("removed kind must fire ONE deletion push (spec+data both empty); got %d", del["drop"])
	}
	if cp.Config("drop", 1) != nil {
		t.Fatal("removed kind must be dropped from current (no longer served); still present")
	}
	if cp.Config("keep", 1) == nil {
		t.Fatal("surviving kind must still be served")
	}
	if del["keep"] != 0 {
		t.Fatal("surviving kind must NOT get a deletion push")
	}
}

// TestReloadKeepsPriorSnapshotOnError: a per-(kind, kindVersion) load error must not wipe
// the last-known document (a transient DB blip never blanks a worker's config).
func TestReloadKeepsPriorSnapshotOnError(t *testing.T) {
	repo := errOnceRepo{doc: json.RawMessage(`{"v":1}`)}
	cp := newTestPlane(&repo, "k")

	cp.reload(context.Background()) // succeeds → current = {"v":1}
	if string(cp.Config("k", 1)) != `{"v":1}` {
		t.Fatalf("first reload must store the doc, got %s", cp.Config("k", 1))
	}
	repo.fail = true
	cp.reload(context.Background()) // errors → must keep the prior snapshot
	if string(cp.Config("k", 1)) != `{"v":1}` {
		t.Fatalf("a load error must keep the prior snapshot, got %s", cp.Config("k", 1))
	}
}

// errOnceRepo serves a doc, or an error when fail is set.
type errOnceRepo struct {
	doc  json.RawMessage
	fail bool
}

func (r *errOnceRepo) GetDefaultProviderConfig(_ context.Context, _ model.Kind, _ int) (store.DefaultProviderConfig, bool, error) {
	if r.fail {
		return store.DefaultProviderConfig{}, false, context.DeadlineExceeded
	}
	return store.DefaultProviderConfig{Spec: r.doc}, true, nil
}

func (r *errOnceRepo) ConfiguredKindVersions(context.Context) ([]model.KindVersion, error) {
	return nil, nil
}
