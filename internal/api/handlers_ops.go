package api

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/salesforce/converge/internal/store"
)

// Subresource operation handlers: verbs declared by a kind's manifest
// operation reactions dispatch through resource_operations + a
// work_queue row with task_type='operate'. work_queue's
// UNIQUE(resource_id, task_type) gives operate its own slot, so a
// verb can run concurrently with the kind's reconcile work.

// invokeOperation creates a resource_operations row, validates the
// verb's input against the manifest's verb input schema, and enqueues
// a work_queue 'operate' task. Returns the new operation row.
func (s *Server) invokeOperation(ctx context.Context, in *invokeOpInput) (*invokeOpOutput, error) {
	id, err := s.resolveID(ctx, in.Kind, in.Name)
	if err != nil {
		return nil, err
	}
	info, err := s.repo.GetResourceInfo(ctx, id)
	if err != nil {
		return nil, huma.Error404NotFound("resource not found")
	}
	// Verb existence + input validation come from the kind's manifest (pure data
	// carried in the manifest), so a verb is invocable the moment its kind is
	// declared — even while the kind's Setup is still pending. The enqueued
	// 'operate' task simply waits in work_queue until the kind comes online,
	// exactly like apply/reconcile for a pending kind.
	km, ok := s.schemaForKind(info.Kind)
	if !ok {
		return nil, huma.Error404NotFound("unknown kind: " + string(info.Kind))
	}
	// A verb exists iff the manifest declares an operation reaction for it.
	if _, ok := km.OperationReaction(in.Body.Verb); !ok {
		return nil, huma.Error404NotFound("kind " + string(info.Kind) + " has no verb " + in.Body.Verb)
	}
	if err := s.schema().validateVerbInput(info.Kind, in.Body.Verb, in.Body.Input); err != nil {
		return nil, err
	}

	// Reject verbs on a FROZEN resource up front (409), so the operation row
	// isn't created and left stuck-pending (EnqueueOperationWork also guards the
	// enqueue as defense-in-depth, but that would silently no-op the task while
	// leaving a 'pending' op row). A Quarantined resource was deliberately set
	// aside; an Orphaned one is pending teardown — neither should run a verb
	// against the live provider. (Deleting is allowed to keep finalizer/cleanup
	// verbs working during teardown.)
	switch info.Phase {
	case phaseQuarantined:
		return nil, huma.Error409Conflict("resource is quarantined; unquarantine it before invoking verbs")
	case phaseOrphaned:
		return nil, huma.Error409Conflict("resource is orphaned (pending teardown); cannot invoke verbs")
	}

	// Create the resource_operations row + enqueue the 'operate' task in
	// one store call; we need the full row back for the response body.
	op, err := s.repo.CreateOperationRow(ctx, id, in.Body.Verb, in.Body.Input, "api")
	if err != nil {
		return nil, internalError(ctx, err)
	}

	out := &invokeOpOutput{Status: http.StatusAccepted}
	out.Body = operationToDTO(op)
	return out, nil
}

func (s *Server) listOperations(ctx context.Context, in *listOpsInput) (*listOpsOutput, error) {
	id, err := s.resolveID(ctx, in.Kind, in.Name)
	if err != nil {
		return nil, err
	}
	rows, err := s.readRepo.ListOperationsByResource(ctx, id, in.States, in.Limit)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	out := &listOpsOutput{}
	out.Body.Operations = make([]operationDTO, len(rows))
	for i, r := range rows {
		out.Body.Operations[i] = operationToDTO(r)
	}
	return out, nil
}

func (s *Server) getOperation(ctx context.Context, in *getOpInput) (*getOpOutput, error) {
	id, err := s.resolveID(ctx, in.Kind, in.Name)
	if err != nil {
		return nil, err
	}
	op, err := s.readRepo.GetOperation(ctx, in.OpUID)
	if err != nil {
		return nil, huma.Error404NotFound("operation not found")
	}
	// The op UID is opaque, but still validate it belongs to THIS resource so one
	// resource's ops can't be read through another's path. The resource id used
	// here is internal-only and never returned.
	if op.ResourceID != id {
		return nil, huma.Error404NotFound("operation does not belong to resource")
	}
	out := &getOpOutput{Body: operationToDTO(op)}
	return out, nil
}

func operationToDTO(r store.ResourceOperation) operationDTO {
	return operationDTO{
		UID:          r.ID.String(),
		Verb:         r.Verb,
		Input:        r.Input,
		Output:       r.Output,
		State:        r.State,
		ErrorMessage: r.ErrorMessage,
		Attempts:     r.Attempts,
		RequestedBy:  r.RequestedBy,
		RequestedAt:  r.RequestedAt,
		CompletedAt:  r.CompletedAt,
	}
}
