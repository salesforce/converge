package fakek8sjob

import (
	"encoding/json"
	"testing"

	"github.com/salesforce/converge/sdk-go/converge"
)

// TestRunsLiteralImage proves a job with only its literal spec.image runs that image
// (the manual / no-upstream case).
func TestRunsLiteralImage(t *testing.T) {
	out, err := worker{}.React(t.Context(), converge.ReactionRequest{
		Resource: converge.Resource{Kind: Kind, Name: "job-x", Spec: json.RawMessage(`{"image":"nginx:1.27"}`)},
	})
	if err != nil {
		t.Fatalf("React errored: %v", err)
	}
	var st Status
	_ = json.Unmarshal(out.Status, &st)
	if st.RanImage != "nginx:1.27" {
		t.Fatalf("ran_image = %q, want nginx:1.27", st.RanImage)
	}
	if st.Job != "job-nginx-1.27" {
		t.Fatalf("job = %q, want job-nginx-1.27", st.Job)
	}
}

// TestBuiltImageWinsOverLiteral proves the flowed built_image (from an upstream
// faketerraform apply) is preferred over the literal image — the Job runs what the
// apply just built.
func TestBuiltImageWinsOverLiteral(t *testing.T) {
	out, err := worker{}.React(t.Context(), converge.ReactionRequest{
		Resource: converge.Resource{Kind: Kind, Name: "job-x",
			Spec: json.RawMessage(`{"image":"nginx:1.27","built_image":"registry.internal/base:built"}`)},
	})
	if err != nil {
		t.Fatalf("React errored: %v", err)
	}
	var st Status
	_ = json.Unmarshal(out.Status, &st)
	if st.RanImage != "registry.internal/base:built" {
		t.Fatalf("ran_image = %q, want the flowed built_image (built wins)", st.RanImage)
	}
}

// TestNoImageIsTerminal proves a job with neither a literal image nor a flowed
// built_image fails TERMINAL with no status (a config error a retry can't fix).
func TestNoImageIsTerminal(t *testing.T) {
	out, err := worker{}.React(t.Context(), converge.ReactionRequest{
		Resource: converge.Resource{Kind: Kind, Name: "job-x", Spec: json.RawMessage(`{}`)},
	})
	if err == nil {
		t.Fatal("expected an error with no image")
	}
	if !converge.IsTerminal(err) {
		t.Fatalf("no-image must be TERMINAL, got: %v", err)
	}
	if out.Status != nil {
		t.Fatalf("a rejected job must emit no status, got %s", out.Status)
	}
}
