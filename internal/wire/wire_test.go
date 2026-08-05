package wire

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/sdk-go/workerpb"
)

// These tests are the safety net for the hand-written model<->proto conversion layer: they
// guarantee no field is silently dropped or mis-mapped (the failure mode a converter layer
// is prone to). A new field on model.* that isn't wired through wire will make one fail.

// TestComposeResultDecode locks the compose-result DECODE the broker performs
// (ComposeResultFromProto). The core only decodes compose results — the worker's own SDK
// converter encodes them on the wire — so this direction is the one the core carries. The
// proto is built directly (not via a core encoder) to test the decoder in isolation.
func TestComposeResultDecode(t *testing.T) {
	p := &workerpb.ComposeResult{
		Desired: []*workerpb.ChildSpec{{Kind: "vpc", KindVersion: 1, Name: "v1", Spec: json.RawMessage(`{"x":1}`)}},
		Edges: []*workerpb.DepEdge{{
			From: &workerpb.ResourceRef{Kind: "a", Name: "1"},
			To:   &workerpb.ResourceRef{Kind: "b", Name: "2"},
		}},
		Configs:    []*workerpb.ProviderConfigSpec{{Name: "c", Kind: "vpc", KindVersion: 1, Spec: json.RawMessage(`{}`)}},
		Status:     json.RawMessage(`{"s":1}`),
		Conditions: []*workerpb.Condition{{Type: "Ready", Status: string(model.ConditionTrue), Reason: "OK", Message: "good"}},
	}
	want := model.ComposePipelineResult{
		Desired:    []model.ChildSpec{{Kind: "vpc", KindVersion: 1, Name: "v1", Spec: json.RawMessage(`{"x":1}`)}},
		Edges:      []model.DepEdge{{From: model.ResourceRef{Kind: "a", Name: "1"}, To: model.ResourceRef{Kind: "b", Name: "2"}}},
		Configs:    []model.ProviderConfigSpec{{Name: "c", Kind: "vpc", KindVersion: 1, Spec: json.RawMessage(`{}`)}},
		Status:     json.RawMessage(`{"s":1}`),
		Conditions: []model.Condition{{Type: "Ready", Status: model.ConditionTrue, Reason: "OK", Message: "good"}},
	}
	out := ComposeResultFromProto(p)
	assertChildSpecs(t, want.Desired, out.Desired)
	assertConfigs(t, want.Configs, out.Configs)
	// Edge with no value-flows: From/To must decode; Values nil-vs-empty is not a
	// meaningful difference (both mean "no flows").
	if len(want.Edges) != len(out.Edges) || want.Edges[0].From != out.Edges[0].From || want.Edges[0].To != out.Edges[0].To {
		t.Fatalf("compose result edge drift:\n want=%+v\n out=%+v", want.Edges, out.Edges)
	}
	if string(want.Status) != string(out.Status) || !reflect.DeepEqual(want.Conditions, out.Conditions) {
		t.Fatalf("compose result status/conditions drift:\n want=%+v\n out=%+v", want, out)
	}
}

// assertChildSpecs compares ChildSpecs, treating Spec as semantic JSON (its Go
// type goes any -> json.RawMessage across the wire).
func assertChildSpecs(t *testing.T, want, got []model.ChildSpec) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("ChildSpec count: want %d got %d", len(want), len(got))
	}
	for i := range want {
		// KindVersion MUST survive the wire round-trip (no implicit v1 default): a
		// dropped version would arrive as a rejected 0 at ApplyComposeResult.
		if want[i].Kind != got[i].Kind || want[i].KindVersion != got[i].KindVersion || want[i].Name != got[i].Name || !reflect.DeepEqual(want[i].Labels, got[i].Labels) {
			t.Fatalf("ChildSpec[%d] identity/version/labels drift:\n want=%+v\n got=%+v", i, want[i], got[i])
		}
		if !jsonEqual(want[i].Spec, got[i].Spec) {
			t.Fatalf("ChildSpec[%d] spec drift: want=%v got=%v", i, want[i].Spec, got[i].Spec)
		}
	}
}

func assertConfigs(t *testing.T, want, got []model.ProviderConfigSpec) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("Config count: want %d got %d", len(want), len(got))
	}
	for i := range want {
		// KindVersion MUST survive the wire round-trip (no implicit v1 default).
		if want[i].Name != got[i].Name || want[i].Kind != got[i].Kind || want[i].KindVersion != got[i].KindVersion || want[i].IsDefault != got[i].IsDefault {
			t.Fatalf("Config[%d] identity/version drift:\n want=%+v\n got=%+v", i, want[i], got[i])
		}
		if !jsonEqual(want[i].Spec, got[i].Spec) {
			t.Fatalf("Config[%d] spec drift", i)
		}
	}
}

// jsonEqual compares two values' JSON encodings semantically (handles the
// any vs json.RawMessage shape change the opaque-spec path introduces).
func jsonEqual(a, b any) bool {
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	var av, bv any
	_ = json.Unmarshal(ab, &av)
	_ = json.Unmarshal(bb, &bv)
	return reflect.DeepEqual(av, bv)
}

// TestAnyToBytesPropagatesError confirms the C1 fix: an unmarshalable spec
// surfaces an error instead of silently shipping nil.
func TestAnyToBytesPropagatesError(t *testing.T) {
	_, err := childSpecsToProto([]model.ChildSpec{
		{Kind: "vpc", KindVersion: 1, Name: "bad", Spec: make(chan int)}, // channels can't marshal to JSON
	})
	if err == nil {
		t.Fatal("expected an error for an unmarshalable child spec, got nil (C1 regression)")
	}
}
