package stdcel

import (
	"encoding/json"

	"github.com/salesforce/converge/sdk-go/converge"
)

// Kind is the single composer kind this provider serves; its CRD lives beside
// this package in stdcel.kind.json.
const Kind converge.Kind = "stdcel"

// KindVersion is the single web-API version this provider serves (no implicit
// default, no minor). It mirrors the kind_version in stdcel.kind.json — keep the
// two in sync.
const KindVersion = 1

// reactionCompose is the sole reaction this kind declares (spec change → child
// DAG). Work dispatches on the reaction name so a future second reaction is an
// explicit switch arm, not a silent fall-through.
const reactionCompose = "compose"

// graph is the RESOURCE GRAPH DEFINITION — the kro-style composition-as-data the
// provider reads from the kind's DEFAULT providerconfig (spec ⊕ per-resource
// override). It is the WHOLE composition: a set of child resources whose fields
// are CEL expressions over the instance's own spec and over each other. There is
// NO per-kind Go — one provider serves any graph.
//
// It travels the CONFIG axis (not the bundle) because it is structured, queryable,
// validatable JSON — an operator edits the graph + re-applies and the next compose
// recomposes with no worker redeploy (the celbom/stdstarlark "composition is data"
// property, generalised from a fixed BOM fan-out to an arbitrary graph).
type graph struct {
	// Resources is the child graph. Order is irrelevant: the provider infers the
	// dependency order from the CEL references between resources (a resource that
	// reads ${other.status.x} depends on `other`), exactly like kro's RGD — you
	// declare resources, not an order.
	Resources []resourceDef `json:"resources"`
}

// resourceDef is one node of the graph: a stable local Id (how other resources and
// the graph reference it) and a Template the provider stamps into a child resource.
type resourceDef struct {
	// Id is the graph-local handle other resources reference in CEL as
	// `${<id>.spec.x}` / `${<id>.status.x}`. Unique within a graph; never leaves
	// the composer (the child's real identity is Template.Kind + the resolved
	// Template.Name).
	Id string `json:"id"`
	// Template is the child resource to compose: its Kind, a Name expression, and a
	// Spec whose string leaves may be CEL expressions.
	Template template `json:"template"`
}

// template is a child resource with CEL-expression leaves. Every string value in
// Name and (recursively) Spec is scanned for ${...} CEL expressions:
//
//   - ${schema.spec.x}      → the INSTANCE's own spec (the stdcel resource's spec —
//     kro's top-level `schema.spec`, the user-supplied inputs).
//   - ${<id>.spec.x}        → another graph resource's resolved spec. A compile-time
//     wiring, resolved during compose (no runtime edge).
//   - ${<id>.status.x}      → another graph resource's RUNTIME status. This can't be
//     known at compose time, so it becomes a DEPENDENCY EDGE + VALUE FLOW: the
//     engine schedules `<id>` first and, once it is Ready, flows its status[x] into
//     this child's spec before this child runs.
//
// A leaf may be a bare literal (no ${...}), a whole-value expression ("${expr}",
// which keeps the CEL result's native type — int/bool/list/map, not just string),
// or an interpolation ("prefix-${expr}-suffix", always a string).
type template struct {
	Kind string `json:"kind"`
	// KindVersion is the web-API version the emitted child is applied at. REQUIRED
	// and explicit (>= 1) in the graph — no implicit v1 default (ApplyComposeResult
	// rejects a 0).
	KindVersion int             `json:"kind_version"`
	Name        string          `json:"name"` // CEL-interpolated, e.g. "${schema.spec.name}-vpc"
	Spec        json.RawMessage `json:"spec,omitempty"`
	// Labels are extra static labels stamped on the child (merged under the
	// provider's own composed_by/graph_id labels, which win). Values may interpolate
	// ${...} like Name.
	Labels map[string]string `json:"labels,omitempty"`
}
