package faketerraform

import (
	"encoding/json"
	"testing"

	"github.com/salesforce/converge/sdk-go/converge"
)

// TestAppliesBundleToOutputs proves a valid s3://.../*.tar produces deterministic
// apply outputs — including the image_tag a downstream fakek8sjob consumes.
func TestAppliesBundleToOutputs(t *testing.T) {
	out, err := worker{}.React(t.Context(), converge.ReactionRequest{
		Resource: converge.Resource{Kind: Kind, Name: "tf-x", Spec: json.RawMessage(`{"tar_url":"s3://team-bundles/base.tar"}`)},
	})
	if err != nil {
		t.Fatalf("React errored on a valid bundle: %v", err)
	}
	var st Status
	if err := json.Unmarshal(out.Status, &st); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if st.ApplyID != "apply-base" {
		t.Fatalf("apply_id = %q, want apply-base", st.ApplyID)
	}
	if st.ImageTag != "registry.internal/base:built" {
		t.Fatalf("image_tag = %q, want registry.internal/base:built", st.ImageTag)
	}
}

// TestDeterministic proves the same bundle yields the same outputs across runs (the
// producer contract a value-flow consumer relies on).
func TestDeterministic(t *testing.T) {
	req := converge.ReactionRequest{Resource: converge.Resource{Kind: Kind, Name: "tf-x", Spec: json.RawMessage(`{"tar_url":"s3://b/app.tar"}`)}}
	a, _ := worker{}.React(t.Context(), req)
	b, _ := worker{}.React(t.Context(), req)
	if string(a.Status) != string(b.Status) {
		t.Fatalf("non-deterministic: %s vs %s", a.Status, b.Status)
	}
}

// TestRejectsBadBundleRef proves the input validation is TERMINAL (a config error a
// retry can't fix): missing, non-s3, or non-*.tar tar_url each fail terminally with
// no status emitted.
func TestRejectsBadBundleRef(t *testing.T) {
	for _, tc := range []struct {
		name, spec string
	}{
		{"empty", `{}`},
		{"missing tar_url", `{"other":"x"}`},
		{"not s3", `{"tar_url":"https://host/base.tar"}`},
		{"not tar", `{"tar_url":"s3://b/base.zip"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := worker{}.React(t.Context(), converge.ReactionRequest{
				Resource: converge.Resource{Kind: Kind, Name: "tf-x", Spec: json.RawMessage(tc.spec)},
			})
			if err == nil {
				t.Fatal("expected an error")
			}
			if !converge.IsTerminal(err) {
				t.Fatalf("bad bundle ref must be TERMINAL, got: %v", err)
			}
			if out.Status != nil {
				t.Fatalf("a rejected apply must emit no status, got %s", out.Status)
			}
		})
	}
}
