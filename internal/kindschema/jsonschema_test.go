package kindschema_test

import (
	"encoding/json"
	"testing"

	"github.com/salesforce/converge/internal/kindschema"
)

type sampleSpec struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// TestOfGeneratesSchema: Of[T] produces a non-empty JSON Schema for a real
// struct, and nil (opt-out) for the empty struct.
func TestOfGeneratesSchema(t *testing.T) {
	s := kindschema.Of[sampleSpec]()
	if len(s) == 0 {
		t.Fatal("Of[sampleSpec]() returned empty schema")
	}
	var doc map[string]any
	if err := json.Unmarshal(s, &doc); err != nil {
		t.Fatalf("schema is not valid JSON: %v", err)
	}
	if doc["type"] != "object" {
		t.Fatalf("schema type = %v, want object", doc["type"])
	}
	props, ok := doc["properties"].(map[string]any)
	if !ok || props["name"] == nil || props["count"] == nil {
		t.Fatalf("schema missing properties: %v", doc["properties"])
	}

	// Empty struct opts out (untyped axis) → nil.
	if got := kindschema.Of[struct{}](); got != nil {
		t.Fatalf("Of[struct{}]() = %s, want nil (opt-out)", got)
	}
}
