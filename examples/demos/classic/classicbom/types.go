package classicbom

import (
	"github.com/salesforce/converge/sdk-go/converge"
)

// Kind is the kind name registered by this provider.
const Kind converge.Kind = "classicbom"

// KindVersion is the single web-API version this provider serves (classicbom/v1);
// it is EXPLICIT (>= 1) — there is no implicit v1 default. A future v2 would list a
// second KindVersion in Kinds and branch on req.KindVersion in Work.
const KindVersion = 1

// Kinds reports the (kind, version) pairs this provider serves — just
// classicbom/v1. Pure data (no env, no dial), so the SDK can register it without
// constructing any client. The manifest (CRD) is operator-applied, not derived here.
func (Provider) Kind() converge.KindVersion {
	return converge.KindVersion{Kind: Kind, Version: KindVersion}
}

// ClassicBOMSpec is the minimal subset of a deployment BOM that the provider
// actually consumes during composition: the DeploymentInstance name, each FD's
// identity + AccountsOnly + TGW enablement, and each team's name. A real-world BOM
// would carry many more fields (subnet plans, CIDRs, governance metadata, …); the
// blank `_` field with `additionalProperties:"true"` on every level tells huma's
// schema generator to allow them through validation. Without it, a richer BOM
// would fail server-side validation at /api/resources because huma defaults to
// additionalProperties:false. Go's JSON decoder still ignores unknown keys at
// runtime, so the composer never sees the extras. (This is a sanitized example —
// the demo fixtures carry only the fields modeled below.)
type ClassicBOMSpec struct {
	_                  struct{}            `additionalProperties:"true"`
	DeploymentInstance *DeploymentInstance `json:"deployment_instance"`
}

type DeploymentInstance struct {
	_                 struct{}            `additionalProperties:"true"`
	Name              string              `json:"name" minLength:"1" doc:"Deployment instance name; required and used as the prefix for every composed child name (which must be globally unique), so it must be unique across BOMs."`
	FunctionalDomains []*FunctionalDomain `json:"functional_domains"`
}

type FunctionalDomain struct {
	_                 struct{}       `additionalProperties:"true"`
	Name              string         `json:"name" minLength:"1" doc:"FD name; appears in composed resource names and labels."`
	AccountsOnly      bool           `json:"accounts_only,omitempty" doc:"If true, the FD only composes accounts (no VPCs, no TGW)."`
	AWSTransitGateway *TransitGW     `json:"aws_transit_gateway,omitempty"`
	ServiceTeams      []*ServiceTeam `json:"service_teams,omitempty"`
}

type TransitGW struct {
	_       struct{} `additionalProperties:"true"`
	Enabled bool     `json:"enabled" doc:"If true, the FD composes a TGW + per-team routes."`
}

type ServiceTeam struct {
	_    struct{} `additionalProperties:"true"`
	Name string   `json:"name" minLength:"1" doc:"Team name; used as the per-team resource name suffix."`
}

// Name returns the DeploymentInstance name or "unnamed" if absent.
func (s ClassicBOMSpec) Name() string {
	if s.DeploymentInstance != nil && s.DeploymentInstance.Name != "" {
		return s.DeploymentInstance.Name
	}
	return "unnamed"
}

// ClassicBOMStatus is what the StatusRollup writes to classicbom.status. Read
// by the API for /api/resources/{id} so a single GET surfaces the team
// rollup without a recursive subtree fetch.
type ClassicBOMStatus struct {
	Teams []RolledUpTeam `json:"teams"`
}

// RolledUpTeam is a per-team rollup derived from the descendants.
type RolledUpTeam struct {
	FD        string `json:"fd"`
	Team      string `json:"team"`
	AccountID string `json:"account_id,omitempty"`
	VPCID     string `json:"vpc_id,omitempty"`
	TGWID     string `json:"tgw_id,omitempty"`
	RouteID   string `json:"route_id,omitempty"`
	Ready     bool   `json:"ready"`
}
