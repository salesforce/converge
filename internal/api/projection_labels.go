package api

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/salesforce/converge/internal/store"
)

// projection_labels.go: label decode + synthesis (owner_kind/owner_name), the
// ?label= / ?path= query parsers, and the synth-label→native-filter routing.

func decodeJSONMap(raw []byte) map[string]string {
	if len(raw) == 0 {
		return map[string]string{}
	}
	out := map[string]string{}
	_ = json.Unmarshal(raw, &out)
	return out
}

// synthLabels merges resource.labels with synthesized owner fields: owner_kind
// and owner_name (both PUBLIC identity, never an id). Roots have no owner so
// neither key is set. The owner id is used ONLY as the cache lookup key here.
func synthLabels(stored map[string]string, ownerID *uuid.UUID, ownerCache map[uuid.UUID]ownerRef) map[string]string {
	if ownerID == nil {
		return stored
	}
	ref, ok := ownerCache[*ownerID]
	if !ok {
		return stored
	}
	out := make(map[string]string, len(stored)+2)
	for k, v := range stored {
		out[k] = v
	}
	out["owner_kind"] = ref.Kind
	out["owner_name"] = ref.Name
	return out
}

func parseLabels(raw []string) []byte {
	if len(raw) == 0 {
		return nil
	}
	m := map[string]string{}
	for _, kv := range raw {
		i := strings.Index(kv, ":")
		if i <= 0 {
			continue
		}
		m[kv[:i]] = kv[i+1:]
	}
	if len(m) == 0 {
		return nil
	}
	b, _ := json.Marshal(m)
	return b
}

// extractSynthLabelFilters pulls the synthesized owner_kind chip out of the
// ?label= list and routes it to the native kinds filter, returning the residual
// (real) labels + the extracted kinds. The owner_id chip is GONE — owner scoping
// is by (kind, name) via ?owner=, resolved to ids upstream, never a label.
func extractSynthLabelFilters(labels []string) (cleaned []string, kinds []string) {
	cleaned = labels[:0:0]
	for _, kv := range labels {
		i := strings.Index(kv, ":")
		if i <= 0 {
			cleaned = append(cleaned, kv)
			continue
		}
		key, val := kv[:i], kv[i+1:]
		switch key {
		case "owner_kind":
			kinds = append(kinds, val)
		default:
			cleaned = append(cleaned, kv)
		}
	}
	return cleaned, kinds
}

func parsePathSegments(raw []string) ([]store.TopologyPathSegment, error) {
	out := make([]store.TopologyPathSegment, 0, len(raw))
	for _, s := range raw {
		i := strings.Index(s, ":")
		if i < 0 {
			return nil, fmt.Errorf("path segment must be key:value, got %q", s)
		}
		out = append(out, store.TopologyPathSegment{Key: s[:i], Value: s[i+1:]})
	}
	return out, nil
}
