package store

import (
	"encoding/json"
	"slices"
	"testing"
)

// TestDecodeFlowEntries pins the native flows decode used by the edge
// diff: existing DB JSON → []flowEntry, compared with slices.Equal
// against the composer's native []flowEntry (no proposed-side JSON
// round-trip). Empty/absent/"[]" all decode to nil so they equal an empty
// proposed slice; order is significant (jsonb preserves array order).
func TestDecodeFlowEntries(t *testing.T) {
	eq := func(raw string, proposed []flowEntry) bool {
		return slices.Equal(decodeFlowEntries(json.RawMessage(raw)), proposed)
	}
	if !eq(``, nil) {
		t.Error(`empty existing must equal nil proposed`)
	}
	if !eq(`[]`, nil) {
		t.Error(`"[]" existing must equal nil proposed`)
	}
	if !eq(`[]`, []flowEntry{}) {
		t.Error(`"[]" existing must equal empty proposed`)
	}
	if !eq(`[{"dep_field":"/x","src_field":"/x"}]`, []flowEntry{{DepField: "/x", SrcField: "/x"}}) {
		t.Error(`one flow must match`)
	}
	if eq(`[{"dep_field":"/x","src_field":"/x"}]`, []flowEntry{{DepField: "/y", SrcField: "/y"}}) {
		t.Error(`different flow must NOT match`)
	}
	// order significant
	a := `[{"dep_field":"/x","src_field":"/x"},{"dep_field":"/y","src_field":"/y"}]`
	if eq(a, []flowEntry{{DepField: "/y", SrcField: "/y"}, {DepField: "/x", SrcField: "/x"}}) {
		t.Error(`flow order is significant; reversed must NOT match`)
	}
}

// TestSpecsMatchIgnoringFlowed pins the compose no-churn guard. The
// composer emits a spec WITHOUT the flowed field; the runtime fills it
// from upstream status. If the diff treated that as "changed" it would
// re-upsert → bump generation → re-substitute → infinite loop. The guard
// must report a flowed child as UNCHANGED. Also pins that the
// DeepEqual-over-decoded-maps compare is order/format insensitive.
func TestSpecsMatchIgnoringFlowed(t *testing.T) {
	flowed := [][]string{{"account_id"}} // /account_id
	none := [][]string(nil)

	cases := []struct {
		name     string
		existing string
		proposed string
		paths    [][]string
		want     bool
	}{
		{
			name:     "flowed field present in existing, absent in proposed → match",
			existing: `{"cidr":"10.0.0.0/16","account_id":"acct-123"}`,
			proposed: `{"cidr":"10.0.0.0/16"}`,
			paths:    flowed, want: true,
		},
		{
			name:     "flowed field differs but is ignored → match",
			existing: `{"cidr":"10.0.0.0/16","account_id":"old"}`,
			proposed: `{"cidr":"10.0.0.0/16","account_id":"new"}`,
			paths:    flowed, want: true,
		},
		{
			name:     "non-flowed field changed → no match (real spec change)",
			existing: `{"cidr":"10.0.0.0/16","account_id":"acct-123"}`,
			proposed: `{"cidr":"10.1.0.0/16"}`,
			paths:    flowed, want: false,
		},
		{
			name:     "key order differs, same content → match (JSON-normalized)",
			existing: `{"b":2,"a":1}`,
			proposed: `{"a":1,"b":2}`,
			paths:    none, want: true,
		},
		{
			name:     "number formatting differs, same value → match",
			existing: `{"n":10}`,
			proposed: `{"n":1e1}`,
			paths:    none, want: true,
		},
		{
			name:     "no flows, genuinely different → no match",
			existing: `{"x":1}`,
			proposed: `{"x":2}`,
			paths:    none, want: false,
		},
		{
			name:     "empty existing (never written) vs empty proposed → match",
			existing: `{}`,
			proposed: `{}`,
			paths:    none, want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := specsMatchIgnoringFlowed(
				json.RawMessage(tc.existing), json.RawMessage(tc.proposed), tc.paths)
			if got != tc.want {
				t.Errorf("specsMatchIgnoringFlowed = %v, want %v\n  existing=%s\n  proposed=%s",
					got, tc.want, tc.existing, tc.proposed)
			}
		})
	}
}

// TestLabelsMatch pins the native labels compare, especially the
// empty-vs-nil case (a no-labels child must not churn every recompose).
func TestLabelsMatch(t *testing.T) {
	cases := []struct {
		name     string
		existing string
		proposed map[string]string
		want     bool
	}{
		{"both empty: '{}' vs nil", `{}`, nil, true},
		{"both empty: '' vs nil", ``, nil, true},
		{"both empty: '{}' vs empty map", `{}`, map[string]string{}, true},
		{"equal entries", `{"k":"v"}`, map[string]string{"k": "v"}, true},
		{"value differs", `{"k":"v"}`, map[string]string{"k": "w"}, false},
		{"key added", `{"k":"v"}`, map[string]string{"k": "v", "x": "y"}, false},
		{"key removed", `{"k":"v","x":"y"}`, map[string]string{"k": "v"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := labelsMatch(json.RawMessage(tc.existing), tc.proposed); got != tc.want {
				t.Errorf("labelsMatch(%q, %v) = %v, want %v", tc.existing, tc.proposed, got, tc.want)
			}
		})
	}
}
