package kindschema

import (
	"encoding/json"
	"testing"

	"github.com/salesforce/converge/internal/model"
)

// TestManifestSchemaCompat exercises the additive-vs-breaking linter across the
// input (spec/config) and output (status) direction semantics. The invariant
// under test: a change is Compatible ONLY when every doc valid under old stays
// valid under new (inputs) / new promises at least what old did (outputs). The
// linter must be SOUND — it may over-report breaking, never under-report.
func TestManifestSchemaCompat(t *testing.T) {
	raw := func(s string) json.RawMessage { return json.RawMessage(s) }
	spec := func(s string) model.KindManifest { return model.KindManifest{SpecSchema: raw(s)} }
	status := func(s string) model.KindManifest { return model.KindManifest{StatusSchema: raw(s)} }

	obj := func(props, required string) string {
		r := ""
		if required != "" {
			r = `,"required":[` + required + `]`
		}
		return `{"type":"object","properties":{` + props + `}` + r + `}`
	}

	cases := []struct {
		name       string
		old, newM  model.KindManifest
		compatible bool
	}{
		// ── spec (INPUT) ──
		{"spec: identical", spec(obj(`"a":{"type":"string"}`, "")), spec(obj(`"a":{"type":"string"}`, "")), true},
		{"spec: add optional field", spec(obj(`"a":{"type":"string"}`, "")), spec(obj(`"a":{"type":"string"},"b":{"type":"string"}`, "")), true},
		{"spec: add REQUIRED field", spec(obj(`"a":{"type":"string"}`, "")), spec(obj(`"a":{"type":"string"},"b":{"type":"string"}`, `"b"`)), false},
		{"spec: remove field", spec(obj(`"a":{"type":"string"},"b":{"type":"string"}`, "")), spec(obj(`"a":{"type":"string"}`, "")), false},
		{"spec: drop a requirement", spec(obj(`"a":{"type":"string"}`, `"a"`)), spec(obj(`"a":{"type":"string"}`, "")), true},
		{"spec: type change", spec(obj(`"a":{"type":"string"}`, "")), spec(obj(`"a":{"type":"integer"}`, "")), false},
		{"spec: widen maximum", spec(obj(`"a":{"type":"integer","maximum":10}`, "")), spec(obj(`"a":{"type":"integer","maximum":20}`, "")), true},
		{"spec: narrow maximum", spec(obj(`"a":{"type":"integer","maximum":20}`, "")), spec(obj(`"a":{"type":"integer","maximum":10}`, "")), false},
		{"spec: add minLength", spec(obj(`"a":{"type":"string"}`, "")), spec(obj(`"a":{"type":"string","minLength":3}`, "")), false},
		{"spec: remove minLength", spec(obj(`"a":{"type":"string","minLength":3}`, "")), spec(obj(`"a":{"type":"string"}`, "")), true},
		{"spec: enum add value", spec(obj(`"a":{"type":"string","enum":["x"]}`, "")), spec(obj(`"a":{"type":"string","enum":["x","y"]}`, "")), true},
		{"spec: enum remove value", spec(obj(`"a":{"type":"string","enum":["x","y"]}`, "")), spec(obj(`"a":{"type":"string","enum":["x"]}`, "")), false},
		{"spec: add enum constraint", spec(obj(`"a":{"type":"string"}`, "")), spec(obj(`"a":{"type":"string","enum":["x"]}`, "")), false},
		{"spec: add schema to untyped", model.KindManifest{}, spec(obj(`"a":{"type":"string"}`, `"a"`)), false},
		{"spec: drop schema (anything-goes)", spec(obj(`"a":{"type":"string"}`, `"a"`)), model.KindManifest{}, true},

		// ── status (OUTPUT — direction inverted) ──
		{"status: add required field (safe)", status(obj(`"a":{"type":"string"}`, `"a"`)), status(obj(`"a":{"type":"string"},"b":{"type":"string"}`, `"a","b"`)), true},
		{"status: remove promised required field", status(obj(`"a":{"type":"string"},"b":{"type":"string"}`, `"a","b"`)), status(obj(`"a":{"type":"string"}`, `"a"`)), false},
		{"status: remove a property readers use", status(obj(`"a":{"type":"string"},"b":{"type":"string"}`, "")), status(obj(`"a":{"type":"string"}`, "")), false},
		{"status: drop schema breaks readers", status(obj(`"a":{"type":"string"}`, "")), model.KindManifest{}, false},
		{"status: add schema (new promise)", model.KindManifest{}, status(obj(`"a":{"type":"string"}`, "")), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ManifestSchemaCompat(tc.old, tc.newM)
			if got.Compatible != tc.compatible {
				t.Fatalf("ManifestSchemaCompat = compatible:%v (reasons=%v), want compatible:%v",
					got.Compatible, got.Reasons, tc.compatible)
			}
			if !got.Compatible && len(got.Reasons) == 0 {
				t.Fatalf("breaking verdict must carry at least one reason")
			}
		})
	}
}
