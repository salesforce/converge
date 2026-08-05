package converge

// convert.go maps the SDK's contract types (types.go: Resource, ChildSpec, DepEdge,
// Condition, Operation, the per-stage DTOs) to and from the generated worker proto
// (sdk-go/workerpb), at the worker's wire edge. The runner decodes an incoming StageTask
// into a ReactionRequest (requestFromTask), runs the provider's handler, then encodes the
// Outcome into a StageComplete (completeFromOutcome).
//
// Encoding discipline: the STRUCTURAL collections (the rollup subtree, compose
// children/edges/configs, the operation row) are TYPED proto messages — the wire never
// JSON-marshals thousands of objects. Only the OPAQUE provider JSON (a Resource's
// spec/status, a ChildSpec's `any` spec, an Operation's input) stays `bytes` — arbitrary
// user JSON with no fixed schema, carried as a raw []byte copy.

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

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

func bytesToID(b []byte) uuid.UUID {
	if len(b) != 16 {
		return uuid.Nil
	}
	var id uuid.UUID
	copy(id[:], b)
	return id
}

func bytesToIDPtr(b []byte) *uuid.UUID {
	if len(b) != 16 {
		return nil
	}
	id := bytesToID(b)
	return &id
}

func timePtrMs(p *time.Time) int64 {
	if p == nil {
		return 0
	}
	return p.UnixMilli()
}

func msToTimePtr(ms int64) *time.Time {
	if ms == 0 {
		return nil
	}
	t := time.UnixMilli(ms).UTC()
	return &t
}

// rawOrNil returns nil for an empty/absent wire field so a decoder sees the same nil
// bytes an in-process path would pass (rather than an empty []byte).
func rawOrNil(b []byte) json.RawMessage {
	if len(b) == 0 {
		return nil
	}
	return b
}

// anyToBytes JSON-encodes an opaque `any` spec to bytes (nil → nil). This is the one
// place opaque provider data is (un)marshaled — it has no proto schema. A marshal
// failure is RETURNED, never swallowed: a composer that emits an unmarshalable spec must
// fail the stage (no silent nil-spec on the wire).
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

func resourceToProto(r Resource) *workerpb.Resource {
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

func resourceFromProto(p *workerpb.Resource) Resource {
	if p == nil {
		return Resource{}
	}
	return Resource{
		ID:                  bytesToID(p.GetId()),
		Kind:                Kind(p.GetKind()),
		Name:                p.GetName(),
		OwnerID:             bytesToIDPtr(p.GetOwnerId()),
		RootID:              bytesToIDPtr(p.GetRootId()),
		Spec:                rawOrNil(p.GetSpec()),
		Status:              rawOrNil(p.GetStatus()),
		Generation:          p.GetGeneration(),
		SyncedGen:           p.GetSyncedGen(),
		IsReady:             p.GetIsReady(),
		Finalizers:          p.GetFinalizers(),
		DeletionRequestedAt: msToTimePtr(p.GetDeletionRequestedAtMs()),
		FrozenUntil:         msToTimePtr(p.GetFrozenUntilMs()),
		Quarantined:         p.GetQuarantined(),
		Labels:              p.GetLabels(),
	}
}

func resourcesFromProto(ps []*workerpb.Resource) []Resource {
	if len(ps) == 0 {
		return nil
	}
	out := make([]Resource, len(ps))
	for i, p := range ps {
		out[i] = resourceFromProto(p)
	}
	return out
}

// ── ChildSpec / DepEdge / ProviderConfigSpec ──

func childSpecsToProto(cs []ChildSpec) ([]*workerpb.ChildSpec, error) {
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
		// explicit version on every child, and the core's ApplyComposeResult rejects a 0 —
		// so if it were dropped here it would arrive as a rejected 0 over the Connect path.
		out[i] = &workerpb.ChildSpec{Kind: string(c.Kind), KindVersion: int32(c.KindVersion), Name: c.Name, Spec: spec, Labels: c.Labels}
	}
	return out, nil
}

func childSpecsFromProto(ps []*workerpb.ChildSpec) []ChildSpec {
	if len(ps) == 0 {
		return nil
	}
	out := make([]ChildSpec, len(ps))
	for i, p := range ps {
		out[i] = ChildSpec{Kind: Kind(p.GetKind()), KindVersion: int(p.GetKindVersion()), Name: p.GetName(), Spec: bytesToAny(p.GetSpec()), Labels: p.GetLabels()}
	}
	return out
}

func refToProto(r ResourceRef) *workerpb.ResourceRef {
	return &workerpb.ResourceRef{Kind: string(r.Kind), Name: r.Name}
}

func refFromProto(p *workerpb.ResourceRef) ResourceRef {
	if p == nil {
		return ResourceRef{}
	}
	return ResourceRef{Kind: Kind(p.GetKind()), Name: p.GetName()}
}

func edgesToProto(es []DepEdge) []*workerpb.DepEdge {
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

func edgesFromProto(ps []*workerpb.DepEdge) []DepEdge {
	if len(ps) == 0 {
		return nil
	}
	out := make([]DepEdge, len(ps))
	for i, p := range ps {
		vs := make([]ValueFlow, len(p.GetValues()))
		for j, v := range p.GetValues() {
			vs[j] = ValueFlow{DependentField: v.GetDependentField(), SourceField: v.GetSourceField()}
		}
		out[i] = DepEdge{From: refFromProto(p.GetFrom()), To: refFromProto(p.GetTo()), Values: vs}
	}
	return out
}

func configsToProto(cs []ProviderConfigSpec) ([]*workerpb.ProviderConfigSpec, error) {
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

func configsFromProto(ps []*workerpb.ProviderConfigSpec) []ProviderConfigSpec {
	if len(ps) == 0 {
		return nil
	}
	out := make([]ProviderConfigSpec, len(ps))
	for i, p := range ps {
		out[i] = ProviderConfigSpec{Name: p.GetName(), Kind: Kind(p.GetKind()), KindVersion: int(p.GetKindVersion()), IsDefault: p.GetIsDefault(), Spec: bytesToAny(p.GetSpec())}
	}
	return out
}

// ── Conditions ──

func conditionsToProto(conds []Condition) []*workerpb.Condition {
	if len(conds) == 0 {
		return nil
	}
	out := make([]*workerpb.Condition, len(conds))
	for i, c := range conds {
		out[i] = &workerpb.Condition{Type: c.Type, Status: string(c.Status), Reason: c.Reason, Message: c.Message}
	}
	return out
}

// ── Per-stage requests ──

func composeReqFromProto(p *workerpb.ComposeRequest) composeRequest {
	return composeRequest{
		Root:     resourceFromProto(p.GetRoot()),
		Observed: resourcesFromProto(p.GetObserved()),
		Desired:  childSpecsFromProto(p.GetDesired()),
		Edges:    edgesFromProto(p.GetEdges()),
		Configs:  configsFromProto(p.GetConfigs()),
	}
}

// workReqToProto is kept (unlike the other to-proto request encoders, which live in the
// broker) because the SDK's own worker tests build a fake WORK StageTask from a
// workRequest DTO — the worker side otherwise only DECODES requests.
func workReqToProto(r workRequest) *workerpb.WorkRequest {
	return &workerpb.WorkRequest{Resource: resourceToProto(r.Resource), Status: r.Status}
}

func workReqFromProto(p *workerpb.WorkRequest) workRequest {
	return workRequest{Resource: resourceFromProto(p.GetResource()), Status: rawOrNil(p.GetStatus())}
}

func rollupReqFromProto(p *workerpb.RollupRequest) rollupRequest {
	return rollupRequest{Root: resourceFromProto(p.GetRoot()), Descendants: resourcesFromProto(p.GetDescendants()), Status: rawOrNil(p.GetStatus())}
}

func deleteReqFromProto(p *workerpb.DeleteRequest) deleteRequest {
	return deleteRequest{Resource: resourceFromProto(p.GetResource())}
}

func operateReqFromProto(p *workerpb.OperateRequest) operateRequest {
	return operateRequest{Resource: resourceFromProto(p.GetResource()), Operation: operationFromProto(p.GetOperation())}
}

func reactReqFromProto(p *workerpb.ReactRequest) reactRequest {
	return reactRequest{Resource: resourceFromProto(p.GetResource()), Transition: transitionFromProto(p.GetTransition()), DedupToken: p.GetDedupToken()}
}

// transitionFromProto converts the workerpb.Transition wire enum to the SDK's typed
// Transition (a closed set); an unknown/UNSPECIFIED value maps to "" so a garbled wire
// value surfaces as an empty transition (the handler can reject it) rather than silently
// masquerading as a valid one. Only the decode direction lives in the worker SDK — the
// broker owns the encode side.
func transitionFromProto(p workerpb.Transition) Transition {
	switch p {
	case workerpb.Transition_TRANSITION_CREATED:
		return TransitionCreated
	case workerpb.Transition_TRANSITION_SYNCED:
		return TransitionSynced
	case workerpb.Transition_TRANSITION_DEGRADED:
		return TransitionDegraded
	case workerpb.Transition_TRANSITION_FAILED:
		return TransitionFailed
	case workerpb.Transition_TRANSITION_DELETED:
		return TransitionDeleted
	default:
		return "" // UNSPECIFIED / unknown → empty
	}
}

func operationFromProto(p *workerpb.Operation) Operation {
	if p == nil {
		return Operation{}
	}
	return Operation{
		ResourceID: bytesToID(p.GetResourceId()), Verb: p.GetVerb(),
		Input: rawOrNil(p.GetInput()), Attempts: int(p.GetAttempts()), RequestedBy: p.GetRequestedBy(),
	}
}

// ── Compose result ──

func composeResultToProto(r composePipelineResult) (*workerpb.ComposeResult, error) {
	desired, err := childSpecsToProto(r.Desired)
	if err != nil {
		return nil, err
	}
	configs, err := configsToProto(r.Configs)
	if err != nil {
		return nil, err
	}
	return &workerpb.ComposeResult{
		Desired:    desired,
		Edges:      edgesToProto(r.Edges),
		Configs:    configs,
		Status:     r.Status,
		Conditions: conditionsToProto(r.Conditions),
	}, nil
}

// ── Stage dispatch — the mirror of sdk-ts/src/convert.ts requestFromTask / ──
// ── completeFromOutcome / failComplete. ──

// requestFromTask decodes a StageTask into a unified ReactionRequest, filling the
// stage-specific fields per the task's workerpb.Stage (WORK/COMPOSE/ROLLUP/DELETE/
// OPERATE/REACT). The bool reports whether the stage is recognized AND carries its
// request payload — false means the caller must fail the task terminally (a config/data
// fault the proto can't enforce: the "exactly one *_req set" invariant). The broker
// already selected the reaction from the DB manifest and shipped its NAME + the fixed
// Trigger falls out of the stage shape; the provider switches on req.Reaction if its kind
// serves several reactions.
func requestFromTask(t *workerpb.StageTask) (ReactionRequest, bool) {
	req := ReactionRequest{
		Reaction:    t.GetReaction(),
		KindVersion: int(t.GetKindVersion()),
		Env: &Env{
			// Per-task CUSTOM providerconfig override (nil = use the kind default the provider
			// holds from OnConfig). The provider resolves effective = default ⊕ override via
			// EffectiveConfig; the bundle override (opaque bytes; empty = kind default) resolves
			// via EffectiveBundle.
			ProviderConfig: rawOrNil(t.GetProviderConfig()),
			ProviderBundle: t.GetProviderBundle(),
		},
	}
	switch t.GetStage() {
	case workerpb.Stage_STAGE_WORK:
		if t.GetWorkReq() == nil {
			return req, false
		}
		wr := workReqFromProto(t.GetWorkReq())
		req.Trigger = TriggerSpecChange
		req.Resource, req.Status = wr.Resource, wr.Status
	case workerpb.Stage_STAGE_COMPOSE:
		if t.GetComposeReq() == nil {
			return req, false
		}
		cr := composeReqFromProto(t.GetComposeReq())
		req.Trigger = TriggerSpecChange
		req.Resource, req.Observed = cr.Root, cr.Observed
	case workerpb.Stage_STAGE_ROLLUP:
		if t.GetRollupReq() == nil {
			return req, false
		}
		rr := rollupReqFromProto(t.GetRollupReq())
		req.Trigger = TriggerChildrenSettled
		req.Resource, req.Descendants, req.Status = rr.Root, rr.Descendants, rr.Status
	case workerpb.Stage_STAGE_DELETE:
		if t.GetDeleteReq() == nil {
			return req, false
		}
		req.Trigger = TriggerDeleteRequested
		req.Resource = deleteReqFromProto(t.GetDeleteReq()).Resource
	case workerpb.Stage_STAGE_OPERATE:
		if t.GetOperateReq() == nil {
			return req, false
		}
		or := operateReqFromProto(t.GetOperateReq())
		op := or.Operation
		req.Trigger = TriggerOperation
		req.Resource, req.Operation = or.Resource, &op
	case workerpb.Stage_STAGE_REACT:
		// A reactor: the reactor kind runs its reaction (a side effect) for a watched
		// resource that crossed a transition. Status-only on the wire, like WORK; the
		// Transition + DedupToken let the handler scope/idempotency-key the effect. The
		// broker acks lifecycle_outbox only after this Completes without error, so a failure
		// re-arms the delivery (at-least-once).
		if t.GetReactReq() == nil {
			return req, false
		}
		rr := reactReqFromProto(t.GetReactReq())
		req.Trigger = TriggerReactor
		req.Resource, req.Transition, req.DedupToken = rr.Resource, rr.Transition, rr.DedupToken
		req.Generation = t.GetGeneration()
	default:
		return req, false // unknown stage
	}
	return req, true
}

// completeFromOutcome encodes a handler's Outcome into a StageComplete for the task's
// stage: status + conditions for WORK/ROLLUP/REACT, the compose result for COMPOSE (via
// composeResultToProto, which can fail on an unmarshalable child spec), the verb output
// for OPERATE, and the bare completion for DELETE. LeaseToken, ClaimEpoch, and Stage are
// copied from the task so any broker can fence the write on the echoed epoch.
func completeFromOutcome(t *workerpb.StageTask, out Outcome) (*workerpb.StageComplete, error) {
	sc := &workerpb.StageComplete{LeaseToken: t.GetLeaseToken(), ClaimEpoch: t.GetClaimEpoch(), Stage: t.GetStage()}
	switch t.GetStage() {
	case workerpb.Stage_STAGE_WORK, workerpb.Stage_STAGE_ROLLUP, workerpb.Stage_STAGE_REACT:
		sc.Status, sc.Conditions = out.Status, conditionsToProto(out.Conditions)
	case workerpb.Stage_STAGE_COMPOSE:
		compose, err := composeResultToProto(composePipelineResult{
			Desired: out.Children, Edges: out.Edges, Configs: out.Configs,
			Status: out.Status, Conditions: out.Conditions,
		})
		if err != nil {
			return nil, fmt.Errorf("encode compose result: %w", err)
		}
		sc.Compose = compose
	case workerpb.Stage_STAGE_OPERATE:
		sc.Output = out.OperationOutput
	case workerpb.Stage_STAGE_DELETE:
		// DELETE reports only the bare completion.
	case workerpb.Stage_STAGE_UNSPECIFIED:
		// Never dispatched (a real stage is always set); the bare completion is harmless.
	}
	return sc, nil
}

// failComplete builds a StageComplete that FAILS the task with message. isTerminal marks
// the failure non-retryable (a config/data fault or a panic); a transient failure
// (isTerminal=false) re-dispatches. LeaseToken, ClaimEpoch, and Stage are copied from the
// task so the fenced write lands on the claiming row.
func failComplete(t *workerpb.StageTask, message string, isTerminal bool) *workerpb.StageComplete {
	return &workerpb.StageComplete{
		LeaseToken:   t.GetLeaseToken(),
		ClaimEpoch:   t.GetClaimEpoch(),
		Stage:        t.GetStage(),
		ErrorMessage: message,
		Terminal:     isTerminal,
	}
}
