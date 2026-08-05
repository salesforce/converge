package runtime

import (
	"encoding/json"
	"sync"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// providerConfigSnapshot is the READ side of the config plane: the current default
// provider config (spec + bundle) per (kind, kindVersion), served lock-free to
// workers. It owns exactly one responsibility — hand out the current document —
// and knows nothing about how the document is refreshed. The ProviderConfigCache embeds
// it and swaps its contents from the refresh loop (upsert/prune under the same
// RWMutex); a worker captures ConfigSource/BundleSource at Setup and reads
// through per task, so an operator edit is picked up live.
//
// The RWMutex guards the map: reads take RLock (the docs are tiny and a task is
// seconds-to-minutes of provider work, so the lock is off any hot path), the
// rare refresh takes Lock.
type providerConfigSnapshot struct {
	mu      sync.RWMutex
	current map[model.KindVersion]store.DefaultProviderConfig // latest {spec,data} per (kind, kindVersion) (zero = absent)
}

// get returns the current entry for a pair (zero value when absent). kindVersion
// is the task's explicit web-API version (>= 1); no normalization — a 0 looks up
// (kind, 0), which no config occupies, so the caller gets nil rather than a
// silent v1 config.
func (s *providerConfigSnapshot) get(kind model.Kind, kindVersion int) store.DefaultProviderConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current[model.KindVersion{Kind: kind, Version: kindVersion}]
}

// Config returns (kind, kindVersion)'s current default config SPEC document, or
// nil if that pair has no providerconfig. Live: a later call reflects an
// operator's edit. The broker reads it to serve GetProviderConfig / build a push.
func (s *providerConfigSnapshot) Config(kind model.Kind, kindVersion int) json.RawMessage {
	return s.get(kind, kindVersion).Spec
}

// Bundle returns (kind, kindVersion)'s current default BUNDLE bytes (the opaque
// provider data — e.g. a zip of Starlark .star files), or nil if absent. Live,
// like Config.
func (s *providerConfigSnapshot) Bundle(kind model.Kind, kindVersion int) []byte {
	return s.get(kind, kindVersion).Data
}
