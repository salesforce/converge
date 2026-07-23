// Package wire is the CORE-SIDE converter between the internal/model stage
// request/response types and their typed proto wire forms (sdk-go/workerpb — the
// public worker contract). It is engine PLUMBING, not an author API — a provider
// author never imports it; the broker (internal/broker) does. The core ENCODES a
// stage's request (to dispatch it) and DECODES the worker's result (ComposeResult,
// status, conditions); the mirror direction — decoding the request, encoding the
// result — is the WORKER's job, done by the SDK's OWN converter (sdk-go/converge's
// convert.go), never shared with this one. The two converters meet only at the proto,
// so the SDK and the core stay decoupled while both honoring one wire contract.
//
// Encoding discipline: the STRUCTURAL collections (the rollup subtree, compose
// children/edges/configs, the operation row) are TYPED proto messages — the wire
// never JSON-marshals thousands of objects. Only the OPAQUE provider JSON (a
// Resource's spec/status, a ChildSpec's `any` spec, an Operation's input) stays
// `bytes` — it is arbitrary user JSON with no fixed schema, a raw []byte copy.
package wire

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/sdk-go/workerpb"
)

// ── uuid / time helpers ──

func idBytes(id uuid.UUID) []byte { return id[:] }

func idPtrBytes(p *uuid.UUID) []byte {
	if p == nil {
		return nil
	}
	return p[:]
}

func timePtrMs(p *time.Time) int64 {
	if p == nil {
		return 0
	}
	return p.UnixMilli()
}

// RawOrNil returns nil for an empty/absent wire field so a decoder sees the same
// nil bytes the in-process path passes (rather than an empty []byte). Exported so
// the broker + remote worker share this one definition over the Connect seam.
func RawOrNil(b []byte) json.RawMessage {
	if len(b) == 0 {
		return nil
	}
	return b
}

// anyToBytes JSON-encodes an opaque `any` spec to bytes (nil → nil). This is the
// one place opaque provider data is (un)marshaled — it has no proto schema. A
// marshal failure is RETURNED, never swallowed: a composer that emits an
// unmarshalable spec must fail the stage (no silent nil-spec on the wire).
func anyToBytes(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return json.Marshal(v)
}

func bytesToAny(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return json.RawMessage(b) // round-trips as raw JSON; providers re-decode their own shape
}

// ── Resource ──

func ResourceToProto(r model.Resource) *workerpb.Resource {
	return &workerpb.Resource{
		Id:                    idBytes(r.ID),
		Kind:                  string(r.Kind),
		Name:                  r.Name,
		OwnerId:               idPtrBytes(r.OwnerID),
		RootId:                idPtrBytes(r.RootID),
		Spec:                  r.Spec,
		Status:                r.Status,
		Generation:            r.Generation,
		SyncedGen:             r.SyncedGen,
		IsReady:               r.IsReady,
		Finalizers:            r.Finalizers,
		DeletionRequestedAtMs: timePtrMs(r.DeletionRequestedAt),
		FrozenUntilMs:         timePtrMs(r.FrozenUntil),
		Quarantined:           r.Quarantined,
		Labels:                r.Labels,
	}
}

func resourcesToProto(rs []model.Resource) []*workerpb.Resource {
	if len(rs) == 0 {
		return nil
	}
	out := make([]*workerpb.Resource, len(rs))
	for i, r := range rs {
		out[i] = ResourceToProto(r)
	}
	return out
}

// ── ChildSpec / DepEdge / ProviderConfigSpec ──

func childSpecsToProto(cs []model.ChildSpec) ([]*workerpb.ChildSpec, error) {
	if len(cs) == 0 {
		return nil, nil
	}
	out := make([]*workerpb.ChildSpec, len(cs))
	for i, c := range cs {
		spec, err := anyToBytes(c.Spec)
		if err != nil {
			return nil, fmt.Errorf("encode child %s/%s spec: %w", c.Kind, c.Name, err)
		}
		// KindVersion MUST ride the wire (no implicit v1 default): a composer stamps an
		// explicit version on every child, and ApplyComposeResult rejects a 0 — so if it
		// were dropped here it would arrive as a rejected 0 over the Connect path.
		out[i] = &workerpb.ChildSpec{Kind: string(c.Kind), KindVersion: int32(c.KindVersion), Name: c.Name, Spec: spec, Labels: c.Labels}
	}
	return out, nil
}

func childSpecsFromProto(ps []*workerpb.ChildSpec) []model.ChildSpec {
	if len(ps) == 0 {
		return nil
	}
	out := make([]model.ChildSpec, len(ps))
	for i, p := range ps {
		out[i] = model.ChildSpec{Kind: model.Kind(p.GetKind()), KindVersion: int(p.GetKindVersion()), Name: p.GetName(), Spec: bytesToAny(p.GetSpec()), Labels: p.GetLabels()}
	}
	return out
}

func refToProto(r model.ResourceRef) *workerpb.ResourceRef {
	return &workerpb.ResourceRef{Kind: string(r.Kind), Name: r.Name}
}

func refFromProto(p *workerpb.ResourceRef) model.ResourceRef {
	if p == nil {
		return model.ResourceRef{}
	}
	return model.ResourceRef{Kind: model.Kind(p.GetKind()), Name: p.GetName()}
}

func edgesToProto(es []model.DepEdge) []*workerpb.DepEdge {
	if len(es) == 0 {
		return nil
	}
	out := make([]*workerpb.DepEdge, len(es))
	for i, e := range es {
		vs := make([]*workerpb.ValueFlow, len(e.Values))
		for j, v := range e.Values {
			vs[j] = &workerpb.ValueFlow{DependentField: v.DependentField, SourceField: v.SourceField}
		}
		out[i] = &workerpb.DepEdge{From: refToProto(e.From), To: refToProto(e.To), Values: vs}
	}
	return out
}

func edgesFromProto(ps []*workerpb.DepEdge) []model.DepEdge {
	if len(ps) == 0 {
		return nil
	}
	out := make([]model.DepEdge, len(ps))
	for i, p := range ps {
		vs := make([]model.ValueFlow, len(p.GetValues()))
		for j, v := range p.GetValues() {
			vs[j] = model.ValueFlow{DependentField: v.GetDependentField(), SourceField: v.GetSourceField()}
		}
		out[i] = model.DepEdge{From: refFromProto(p.GetFrom()), To: refFromProto(p.GetTo()), Values: vs}
	}
	return out
}

func configsToProto(cs []model.ProviderConfigSpec) ([]*workerpb.ProviderConfigSpec, error) {
	if len(cs) == 0 {
		return nil, nil
	}
	out := make([]*workerpb.ProviderConfigSpec, len(cs))
	for i, c := range cs {
		spec, err := anyToBytes(c.Spec)
		if err != nil {
			return nil, fmt.Errorf("encode config %q (kind %s) spec: %w", c.Name, c.Kind, err)
		}
		// KindVersion MUST ride the wire (no implicit v1 default) — same contract as
		// childSpecsToProto: a composer emits an explicit version per config.
		out[i] = &workerpb.ProviderConfigSpec{Name: c.Name, Kind: string(c.Kind), KindVersion: int32(c.KindVersion), IsDefault: c.IsDefault, Spec: spec}
	}
	return out, nil
}

func configsFromProto(ps []*workerpb.ProviderConfigSpec) []model.ProviderConfigSpec {
	if len(ps) == 0 {
		return nil
	}
	out := make([]model.ProviderConfigSpec, len(ps))
	for i, p := range ps {
		out[i] = model.ProviderConfigSpec{Name: p.GetName(), Kind: model.Kind(p.GetKind()), KindVersion: int(p.GetKindVersion()), IsDefault: p.GetIsDefault(), Spec: bytesToAny(p.GetSpec())}
	}
	return out
}

// ── Conditions ──

func ConditionsFromProto(ps []*workerpb.Condition) []model.Condition {
	if len(ps) == 0 {
		return nil
	}
	out := make([]model.Condition, len(ps))
	for i, p := range ps {
		out[i] = model.Condition{Type: p.GetType(), Status: model.ConditionStatus(p.GetStatus()), Reason: p.GetReason(), Message: p.GetMessage()}
	}
	return out
}

// ── Per-stage requests ──

func ComposeReqToProto(r model.ComposeRequest) (*workerpb.ComposeRequest, error) {
	desired, err := childSpecsToProto(r.Desired)
	if err != nil {
		return nil, err
	}
	configs, err := configsToProto(r.Configs)
	if err != nil {
		return nil, err
	}
	return &workerpb.ComposeRequest{
		Root:     ResourceToProto(r.Root),
		Observed: resourcesToProto(r.Observed),
		Desired:  desired,
		Edges:    edgesToProto(r.Edges),
		Configs:  configs,
	}, nil
}

func WorkReqToProto(r model.WorkRequest) *workerpb.WorkRequest {
	return &workerpb.WorkRequest{Resource: ResourceToProto(r.Resource), Status: r.Status}
}

func RollupReqToProto(r model.RollupRequest) *workerpb.RollupRequest {
	return &workerpb.RollupRequest{Root: ResourceToProto(r.Root), Descendants: resourcesToProto(r.Descendants), Status: r.Status}
}

func DeleteReqToProto(r model.DeleteRequest) *workerpb.DeleteRequest {
	return &workerpb.DeleteRequest{Resource: ResourceToProto(r.Resource)}
}

func OperateReqToProto(r model.OperateRequest) *workerpb.OperateRequest {
	return &workerpb.OperateRequest{Resource: ResourceToProto(r.Resource), Operation: operationToProto(r.Operation)}
}

func ReactReqToProto(r model.ReactRequest) *workerpb.ReactRequest {
	return &workerpb.ReactRequest{Resource: ResourceToProto(r.Resource), Transition: transitionToProto(r.Transition), DedupToken: r.DedupToken}
}

// transitionToProto/transitionFromProto convert model.Transition (the typed
// string enum on the Go side) to/from the workerpb.Transition wire enum. Both
// are closed sets; an unknown value maps to UNSPECIFIED / "" so a garbled wire
// value surfaces as an empty transition (the dispatcher/handler can reject it)
// rather than silently masquerading as a valid one.
var transitionToProtoMap = map[model.Transition]workerpb.Transition{
	model.TransitionCreated:  workerpb.Transition_TRANSITION_CREATED,
	model.TransitionSynced:   workerpb.Transition_TRANSITION_SYNCED,
	model.TransitionDegraded: workerpb.Transition_TRANSITION_DEGRADED,
	model.TransitionFailed:   workerpb.Transition_TRANSITION_FAILED,
	model.TransitionDeleted:  workerpb.Transition_TRANSITION_DELETED,
}

func transitionToProto(t model.Transition) workerpb.Transition {
	return transitionToProtoMap[t] // unknown → TRANSITION_UNSPECIFIED (0)
}

func operationToProto(o model.Operation) *workerpb.Operation {
	return &workerpb.Operation{
		ResourceId: idBytes(o.ResourceID), Verb: o.Verb,
		// Attempts narrows int→int32; only an issue past ~2.1B retries of one
		// operation, which is unreachable (terminal failure stops retries long before).
		Input: o.Input, Attempts: int32(o.Attempts), RequestedBy: o.RequestedBy,
	}
}

// ── Compose result ──

// ComposeResultFromProto decodes a worker's compose reply. The broker DECODES results (it
// never encodes them — the worker's own SDK converter does that on the wire), so this
// direction is the only compose-result codec the core carries.
func ComposeResultFromProto(p *workerpb.ComposeResult) model.ComposePipelineResult {
	if p == nil {
		return model.ComposePipelineResult{}
	}
	return model.ComposePipelineResult{
		Desired:    childSpecsFromProto(p.GetDesired()),
		Edges:      edgesFromProto(p.GetEdges()),
		Configs:    configsFromProto(p.GetConfigs()),
		Status:     RawOrNil(p.GetStatus()),
		Conditions: ConditionsFromProto(p.GetConditions()),
	}
}
