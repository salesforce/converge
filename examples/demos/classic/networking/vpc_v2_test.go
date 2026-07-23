package networking

import (
	"encoding/json"
	"testing"

	"github.com/salesforce/converge/examples/demos/classic/fault"
	"github.com/salesforce/converge/sdk-go/converge"
)

// TestVPCv2RuntimeDeclaresKindVersion2 proves the vpc/v2 runtime advertises KindVersion=2
// (what the worker sends to the broker so the core routes vpc/v2 tasks here) while
// staying the bare kind "vpc" — the version is the kindVersion, never in the kind string.
func TestVPCv2RuntimeDeclaresKindVersion2(t *testing.T) {
	v1 := VPCRuntime(0, 0, fault.Injector{}).Kind()
	v2 := VPCv2Runtime(0, 0, fault.Injector{}).Kind()
	if v1.Kind != KindVPC || v2.Kind != KindVPC {
		t.Fatalf("both kind versions must share the bare kind %q; got v1=%q v2=%q", KindVPC, v1.Kind, v2.Kind)
	}
	if v1.Version != 1 {
		t.Fatalf("VPCRuntime must declare KindVersion=1, got %d", v1.Version)
	}
	if v2.Version != 2 {
		t.Fatalf("VPCv2Runtime must declare KindVersion=2, got %d", v2.Version)
	}
}

// TestVPCv2WorkerEchoesRegion proves the v2 worker reads the REQUIRED region and
// echoes it into v2 status — the observable difference from v1 (whose status is
// just vpc_id). This is how a demo operator SEES that the v2 worker ran a vpc/v2
// resource.
func TestVPCv2WorkerEchoesRegion(t *testing.T) {
	out, err := vpcV2Worker{}.React(t.Context(), converge.ReactionRequest{
		Resource: converge.Resource{
			Name: "sample-vpc-v2",
			Spec: json.RawMessage(`{"account_id":"123456789012","cidr":"10.0.0.0/16","region":"us-west-2"}`),
		},
	})
	if err != nil {
		t.Fatalf("vpc/v2 React errored: %v", err)
	}
	var st VPCv2Status
	if err := json.Unmarshal(out.Status, &st); err != nil {
		t.Fatalf("decode v2 status: %v", err)
	}
	if st.Region != "us-west-2" {
		t.Fatalf("v2 status must echo the spec region; got %q", st.Region)
	}
	if st.VPCID != "vpc-123456789012" {
		t.Fatalf("v2 status vpc_id = %q, want vpc-123456789012", st.VPCID)
	}
}

// TestVPCv1WorkerIgnoresV2Fields proves the v1 worker still reconciles a v1 spec
// (with the new OPTIONAL tags) fine — the additive v1 evolution is backward-
// compatible: a v1 spec that omits tags AND one that includes them both work.
func TestVPCv1WorkerAcceptsOptionalTags(t *testing.T) {
	for _, spec := range []string{
		`{"account_id":"123456789012","cidr":"10.0.0.0/16"}`,
		`{"account_id":"123456789012","cidr":"10.0.0.0/16","tags":{"team":"payments"}}`,
	} {
		out, err := vpcWorker{}.React(t.Context(), converge.ReactionRequest{
			Resource: converge.Resource{Name: "v1", Spec: json.RawMessage(spec)},
		})
		if err != nil {
			t.Fatalf("v1 React errored for %s: %v", spec, err)
		}
		if len(out.Status) == 0 {
			t.Fatalf("v1 React emitted no status for %s", spec)
		}
	}
}
