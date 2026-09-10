package networking

import "github.com/salesforce/converge/sdk-go/converge"

// Kinds registered by this package.
const (
	KindVPC   converge.Kind = "vpc"
	KindTGW   converge.Kind = "tgw"
	KindRoute converge.Kind = "route"
)

// VPCSpec is the VPC kind's desired state. AccountID is the upstream
// account this VPC lives in. A user creating a VPC by hand must supply a
// valid 12-digit AccountID (enforced at the API edge by minLength/maxLength).
// When a composer emits a VPC alongside a not-yet-ready account, it leaves
// AccountID absent and emits a ValueFlow that fills it from the account's
// status once the account is ready — the composer's child validation skips
// the flowed field (store.ApplyComposeResult → ValidateSpecPartial), so the
// constraint binds on user-create yet permits the deferred flow.
type VPCSpec struct {
	AccountID string `json:"account_id" minLength:"12" maxLength:"12" doc:"Provider 12-digit account id this VPC lives in."`
	CIDR      string `json:"cidr" minLength:"9" doc:"VPC CIDR block, e.g. 10.0.0.0/16."`
	// Tags is an OPTIONAL free-form label map added in a NON-breaking v1 evolution.
	// Because it is optional (omitempty, not in the schema's `required`), adding it
	// to vpc/v1 is a backward-COMPATIBLE change: every existing v1 spec (which
	// omits it) stays valid. The publish linter (kindschema.ManifestSchemaCompat)
	// classifies it ADDITIVE and AUTO-ALLOWS the re-publish of vpc/v1 with no kindVersion
	// bump — the "small evolution stays on v1" half of the versioning demo.
	// (Contrast VPCv2Spec.Region: required = breaking = a new kindVersion, which is
	// the ONLY way to ship a breaking change; there is no in-place override.)
	Tags map[string]string `json:"tags,omitempty" doc:"Optional free-form tags (added in a non-breaking v1 evolution)."`
}

// VPCStatus is what the VPC Worker writes after reconcile.
type VPCStatus struct {
	VPCID string `json:"vpc_id" doc:"Provider-issued VPC id."`
}

// ─────────────────────────────────────────────────────────────────────────
// vpc/v2 — a BREAKING kindVersion bump: v2 adds a REQUIRED `region`. A v1 spec
// ({account_id, cidr}) is INVALID at v2 (region missing), which is exactly why
// it ships as a new KIND VERSION, not an in-place edit — the publish gate would 409 an
// attempt to add a required field to v1. The author rewrites the spec (K8s
// apiVersion / Terraform kindVersion-provider discipline). The worker binary serves
// BOTH kind versions; the core routes each resource to the (kind, kindVersion) it pins.
// ─────────────────────────────────────────────────────────────────────────

// VPCv2Spec is vpc/v2's desired state: v1's fields PLUS a required region. Same
// account_id flow rules as v1 (a composer emits it absent, the value flows in).
type VPCv2Spec struct {
	AccountID string `json:"account_id" minLength:"12" maxLength:"12" doc:"Provider 12-digit account id this VPC lives in."`
	CIDR      string `json:"cidr" minLength:"9" doc:"VPC CIDR block, e.g. 10.0.0.0/16."`
	Region    string `json:"region" minLength:"2" doc:"Cloud region the VPC is created in (v2, required)."`
}

// VPCv2Status is what the vpc/v2 Worker writes: the id PLUS the region it placed
// the VPC in — so the demo can SEE that the v2 worker (not v1) ran the reconcile.
type VPCv2Status struct {
	VPCID  string `json:"vpc_id" doc:"Provider-issued VPC id."`
	Region string `json:"region" doc:"Region the v2 worker placed the VPC in (echoed from spec)."`
}

// TGWSpec describes a transit gateway.
type TGWSpec struct {
	Name string `json:"name" minLength:"1" doc:"Logical TGW name."`
}

// TGWStatus is what the TGW Worker writes after reconcile.
type TGWStatus struct {
	TGWID string `json:"tgw_id" doc:"Provider-issued transit gateway id."`
}

// RouteSpec carries the values a routing-table entry needs. VPCID and
// TGWID are filled in by the framework from the upstream VPC's and
// TGW's statuses respectively (composer-emitted ValueFlows), or the
// user fills them in directly on a manual create. Both are required and
// non-empty (minLength) — a user-created Route with a missing id is
// rejected at the API edge; the composer emits them absent and the
// flowed values arrive before the Route is scheduled, so its child
// validation skips these flowed paths (see store.ApplyComposeResult →
// ValidateSpecPartial), exactly like VPCSpec.AccountID.
type RouteSpec struct {
	VPCID string `json:"vpc_id" minLength:"1" doc:"Provider VPC id this route attaches."`
	TGWID string `json:"tgw_id" minLength:"1" doc:"Provider TGW id the VPC attaches to."`
}

// RouteStatus is what the Route Worker writes after reconcile.
type RouteStatus struct {
	RouteID string `json:"route_id" doc:"Provider-issued route id."`
}
