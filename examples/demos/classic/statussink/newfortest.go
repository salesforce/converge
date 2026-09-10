package statussink

import (
	"encoding/json"

	"github.com/salesforce/converge/sdk-go/converge"
)

// NewForTest builds a statussink Provider backed by a fresh in-process store at
// endpoint, with the given default config doc, PRE-DIALED (bypassing the lazy Ready
// dial) — for tests. It returns the Provider (a converge.Provider) plus a getObject
// hook to assert what was uploaded. Hermetic: no env reads, no real client.
// Construction lives here (not in the test-only demoruntime bridge) because it sets
// unexported fields; demoruntime just forwards the returned Provider to the in-process
// engine.
func NewForTest(endpoint, defaultDoc string) (converge.Provider, func(key string) ([]byte, bool)) {
	p, get, _ := NewForTestWithStats(endpoint, defaultDoc)
	return p, get
}

// StoreStats is the object-store PUT accounting a test asserts against: Distinct is the
// number of unique keys that received an object (unique side effects), TotalPuts is every
// put call (> Distinct ⇒ some deliveries redelivered), MaxPutsPerKey is the worst-case
// redelivery count for one key. Distinct == the number of (resource, generation) that
// crossed the watched transition proves exactly-once EFFECT even when TotalPuts is higher.
type StoreStats struct {
	Distinct      int
	TotalPuts     int
	MaxPutsPerKey int
}

// NewForTestWithStats is NewForTest plus a stats hook, so a chaos/scale test can prove the
// reactor's at-least-once delivery yields an exactly-once EFFECT: even if a delivery
// redelivers (TotalPuts > Distinct) the object key embeds the generation, so the store
// ends with exactly one object per (resource, generation) — no duplicate, no divergence.
func NewForTestWithStats(endpoint, defaultDoc string) (converge.Provider, func(key string) ([]byte, bool), func() StoreStats) {
	store := &fakeStore{endpoint: endpoint, objects: map[string][]byte{}, putsByKey: map[string]int{}}
	var cfg StatusSinkConfig
	if defaultDoc != "" {
		_ = json.Unmarshal([]byte(defaultDoc), &cfg)
	}
	p := &Provider{cfg: cfg, haveCfg: true, store: store}
	stats := func() StoreStats {
		d, t, m := store.stats()
		return StoreStats{Distinct: d, TotalPuts: t, MaxPutsPerKey: m}
	}
	return p, store.Get, stats
}
