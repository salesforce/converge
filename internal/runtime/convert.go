package runtime

import (
	"encoding/json"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// conditionWire is the JSON shape the drainer's conditions pass reads
// (elem->>'type' etc.). Kept local to the outbox encoding.
type conditionWire struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// marshalConditions encodes a condition slice into the JSONB array the
// outbox carries. Returns nil for an empty slice so the column stays
// NULL and the drainer's conditions pass short-circuits.
func marshalConditions(conds []model.Condition) json.RawMessage {
	if len(conds) == 0 {
		return nil
	}
	wire := make([]conditionWire, len(conds))
	for i, c := range conds {
		wire[i] = conditionWire{
			Type:    c.Type,
			Status:  string(c.Status),
			Reason:  c.Reason,
			Message: c.Message,
		}
	}
	b, err := json.Marshal(wire)
	if err != nil {
		return nil
	}
	return b
}

// deriveHealthAndConditions folds a successful reconcile's accumulated
// conditions into (a) the health_ok scalar the drainer writes and (b)
// the JSONB array it upserts into resource_conditions.
//
//   - If a stage emitted an explicit Ready condition, health_ok follows
//     its status (True/Unknown => healthy, False => unhealthy) and every
//     reported condition is persisted.
//   - If NO Ready condition was emitted but the full pipeline ran
//     (advance==true), the resource is implicitly healthy: health_ok is
//     set true. We still only write condition ROWS when the provider
//     reported some (custom axes); a kind that reports nothing produces
//     (true-or-nil, nil) — and when there were zero conditions at all and
//     advance is true we return (nil, nil) to stay completely off the
//     conditions path. Returning healthOK=nil there keeps health_ok at
//     its column default (true) without a write — byte-identical to the
//     pre-conditions hot path (the 1M benchmark hits exactly this).
//   - On a partial reconcile (advance==false, waiting on descendants) we
//     don't assert health at all: (nil, <any reported conds>).
func deriveHealthAndConditions(conds []model.Condition, advance bool) (*bool, json.RawMessage) {
	var ready *model.Condition
	for i := range conds {
		if conds[i].Type == model.TypeReady {
			ready = &conds[i]
			break
		}
	}

	condsJSON := marshalConditions(conds)

	switch {
	case ready != nil:
		h := ready.Status != model.ConditionFalse
		return &h, condsJSON
	case len(conds) == 0 && advance:
		// Fast path: nothing to say, fully synced → leave health_ok at
		// its default with no write and no conditions row.
		return nil, nil
	case advance:
		// Custom conditions but no Ready axis, full pipeline ran →
		// implicitly healthy, and persist the custom conditions.
		h := true
		return &h, condsJSON
	default:
		// Partial reconcile: don't assert health yet.
		return nil, condsJSON
	}
}

func taskToResource(task store.WorkTask) model.Resource {
	// Name is not set: the work-queue claim doesn't carry it (it lives in
	// resource_meta). The reconcile path doesn't read it; the operate verb
	// fetches it by id (see reactOperate).
	return model.Resource{
		ID:         task.ResourceID,
		Kind:       task.Kind,
		Spec:       task.Spec,
		Generation: task.Generation,
	}
}

// dbqToResource projects a full store.ResourceRow row into the
// provider-side view used by the Rollup pipeline.
func dbqToResource(r store.ResourceRow) model.Resource {
	out := model.Resource{
		ID:         r.ID,
		Kind:       r.Kind,
		Name:       r.Name,
		Spec:       r.Spec,
		Status:     r.Status,
		Generation: r.Generation,
		SyncedGen:  r.SyncedGen,
		IsReady:    r.IsReady,
		Finalizers: r.Finalizers,
	}
	if r.OwnerID.Valid {
		id := uuid.UUID(r.OwnerID.Bytes)
		out.OwnerID = &id
	}
	if r.RootID.Valid {
		id := uuid.UUID(r.RootID.Bytes)
		out.RootID = &id
	}
	if r.DeletionRequestedAt.Valid {
		t := r.DeletionRequestedAt.Time
		out.DeletionRequestedAt = &t
	}
	// frozen_until: one column, two faces. 'infinity' (pgx InfinityModifier) ⇒
	// quarantined; a finite instant ⇒ orphaned (the grace teardown deadline).
	if r.FrozenUntil.Valid {
		if r.FrozenUntil.InfinityModifier == pgtype.Infinity {
			out.Quarantined = true
		} else {
			t := r.FrozenUntil.Time
			out.FrozenUntil = &t
		}
	}
	if len(r.Labels) > 0 {
		_ = json.Unmarshal(r.Labels, &out.Labels)
	}
	return out
}

func derefStringPort(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
