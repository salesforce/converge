package api

import (
	"reflect"
	"testing"
)

// TestApplyTypeEnumsMatch pins each apply body's optional `type` field to the canonical
// applyType* constant via its huma `enum:"…"` struct tag. The four endpoints route by
// URL, not by this field, so a drifted tag would silently accept the wrong self-describing
// value from a directory-sync client (conctl sync) — this guard fails the build first.
func TestApplyTypeEnumsMatch(t *testing.T) {
	cases := []struct {
		name     string
		body     any
		wantType string
	}{
		{"resource", applyResourceManifestInput{}.Body, applyTypeResource},
		{"providerconfig", applyProviderConfigInput{}.Body, applyTypeProviderConfig},
		{"reactorbinding", applyReactorBindingInput{}.Body, applyTypeReactorBinding},
		{"manifest", putManifestBody{}, applyTypeManifest},
	}
	// Every expected value must be one of the declared set (belt-and-suspenders that the
	// applyTypeValues list stays complete).
	valid := map[string]bool{}
	for _, v := range applyTypeValues {
		valid[v] = true
	}
	for _, tc := range cases {
		if !valid[tc.wantType] {
			t.Errorf("%s: expected type %q is not in applyTypeValues", tc.name, tc.wantType)
		}
		f, ok := reflect.TypeOf(tc.body).FieldByName("Type")
		if !ok {
			t.Errorf("%s: apply body has no Type field", tc.name)
			continue
		}
		if got := f.Tag.Get("enum"); got != tc.wantType {
			t.Errorf("%s: Type enum tag = %q, want %q (keep the tag in sync with the applyType* constant)", tc.name, got, tc.wantType)
		}
		if got := f.Tag.Get("json"); got != "type,omitempty" {
			t.Errorf("%s: Type json tag = %q, want \"type,omitempty\" (optional, self-describing)", tc.name, got)
		}
	}
}
