// Package model holds the kind-agnostic CORE contract: the kind-identity
// primitives (Kind, KindVersion, ResourceRef, ResourceManifest), the reaction
// request/response and composition value types the control plane builds from a DB
// row and applies (Resource, ReactionRequest, Outcome, ChildSpec, DepEdge, …), the
// author-facing Trigger/Transition enums, the config-merge helpers, the per-stage
// wire DTOs, the CRD-style KindManifest with its validation lattice, and the
// StageDispatcher seam. It is the CORE-SIDE twin of the worker SDK's own types
// (sdk-go/converge) — both describe the same worker proto, neither imports the
// other, and the two meet ONLY at the proto (the broker's internal/wire converter ⇄
// the SDK's convert.go). It imports no other private package and touches no database.
//
// It is INTERNAL: a worker author does NOT import it — they implement
// converge.Provider and speak converge.Resource/Outcome/ReactionRequest
// (sdk-go/converge). This package is what the ENGINE speaks on its side of the
// wire; a change here that alters the wire shape must be mirrored in the SDK's copy
// + the proto.
//
// Concrete per-kind Spec/Status types live next to each provider. A kind comes
// online when its KindManifest is applied to kind_manifest (operator- or
// boot-seeded); this package does not enumerate them.
package model

import "encoding/json"

// Kind names a resource kind. String-aliased so call sites can use
// untyped string literals or typed constants interchangeably.
type Kind string

// KindVersion is a (kind, web-API version) pair — the addressable unit of everything
// versioned: what a provider serves, what a worker advertises, what the broker routes,
// what a config-cache slot holds. Version is EXPLICIT (>= 1); there is no implicit v1.
// This is the ONE pair type across the SDK — providers, the worker client, and the
// remote-worker transport all use it, so no layer re-defines or converts the concept.
type KindVersion struct {
	Kind    Kind
	Version int
}

// ResourceManifest is the self-describing document for a single
// resource: its identity (Kind + Name), Labels, and Spec bundled into
// one object. It is the canonical shape both on disk (the resource JSON
// fixtures) and on the wire (the Apply request body and the
// download/synthesize response). Carrying kind/name/labels alongside
// the spec means a manifest applies on its own, with all of its identity
// in the one document.
//
//	{
//	  "kind":   "some-kind",
//	  "name":   "example-1",
//	  "labels": {"team": "core"},
//	  "spec":   { ... }
//	}
//
// Spec is kept as raw JSON so a manifest round-trips an arbitrary
// per-kind spec without this package knowing its shape, and so large
// specs (tens of MB) are never re-marshaled. Labels is a flat
// string→string map, mirroring resource_meta.labels (JSONB object); it
// has no omitempty so an empty label set serializes as {} (the
// canonical on-disk shape) rather than vanishing — and it must be a
// non-nil map at marshal time for that to hold.
type ResourceManifest struct {
	Kind   Kind              `json:"kind"`
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels"`
	Spec   json.RawMessage   `json:"spec"`
}

// ResourceRef identifies a sibling resource by (Kind, Name) — the
// natural key under a single owner. Composers and the provider
// framework use ResourceRef in DepEdge and ChildSpec; the on-disk row
// uses a UUID id, not a ref.
type ResourceRef struct {
	Kind Kind
	Name string
}

const refSep = "/"

func (r ResourceRef) String() string {
	return string(r.Kind) + refSep + r.Name
}
