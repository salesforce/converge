package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/salesforce/converge/internal/dbq"
)

// EventType is the small enum of event kinds the framework emits at
// known transition points. Providers can also emit ad-hoc events
// with their own type strings for domain-specific signals.
//
// Vocabulary tracks the stages of the unified 'reconcile' task and the
// other task_types ('delete', 'operate'):
//
//	compose-*   Composer pipeline ran (and what it did)
//	work-*      Worker pipeline result
//	rollup-*    StatusRollup pipeline result
//	delete-*    Deleter ran a finalizer
//	operate-*   Operator handled a verb (subresource)
//	condition-* drainer flipped a condition's status
//	deletion-*  soft-delete protocol transitions
//	spec-*      API mutation bumped generation
type EventType string

const (
	EventComposeSucceeded  EventType = "compose-succeeded"
	EventComposeFailed     EventType = "compose-failed"
	EventWorkStarted       EventType = "work-started"
	EventWorkSucceeded     EventType = "work-succeeded"
	EventWorkFailed        EventType = "work-failed"
	EventRollupSucceeded   EventType = "rollup-succeeded"
	EventRollupFailed      EventType = "rollup-failed"
	EventDeleteSucceeded   EventType = "delete-succeeded"
	EventDeleteFailed      EventType = "delete-failed"
	EventOperateStarted    EventType = "operate-started"
	EventOperateSucceeded  EventType = "operate-succeeded"
	EventOperateFailed     EventType = "operate-failed"
	EventConditionFlipped  EventType = "condition-flipped" // drainer wrote a condition
	EventDeletionRequested EventType = "deletion-requested"
	EventDeletionFinalized EventType = "deletion-finalized"
	EventRequeued          EventType = "requeued" // reaper re-armed a stalled task
	EventSpecChanged       EventType = "spec-changed"
)

// Event is the input shape for EmitEvent. ResourceID and Type are
// required; everything else is optional.
type Event struct {
	ResourceID uuid.UUID
	Type       EventType
	Actor      string         // worker_id, provider name, "api", "reaper", ""
	Reason     string         // short machine-readable code (e.g., "ImagePullError")
	Message    string         // human-readable
	Detail     map[string]any // structured payload; marshaled to jsonb (nil → '{}')
}

// EmitEvent appends one row to resource_events. Best-effort by design:
// errors are returned to the caller, but most call sites should log
// and proceed since the event log is observability, not control flow.
func (s *Store) EmitEvent(ctx context.Context, e Event) error {
	detail := json.RawMessage(`{}`)
	if e.Detail != nil {
		b, err := json.Marshal(e.Detail)
		if err != nil {
			return err
		}
		detail = b
	}
	return s.queries().AppendResourceEvent(ctx, dbq.AppendResourceEventParams{
		ResourceID: e.ResourceID,
		Type:       string(e.Type),
		Actor:      nilIfEmpty(e.Actor),
		Reason:     nilIfEmpty(e.Reason),
		Message:    nilIfEmpty(e.Message),
		Detail:     detail,
	})
}

// ListEvents returns the most recent events for one resource, paginated
// by the (createdAt, id) cursor returned alongside the results.
type EventRow struct {
	ID         int64           `json:"id"`
	ResourceID uuid.UUID       `json:"resource_id"`
	Type       string          `json:"type"`
	Actor      string          `json:"actor,omitempty"`
	Reason     string          `json:"reason,omitempty"`
	Message    string          `json:"message,omitempty"`
	Detail     json.RawMessage `json:"detail"`
	CreatedAt  time.Time       `json:"created_at"`
}

func (s *Store) ListEvents(ctx context.Context, resourceID uuid.UUID, cursorAt time.Time, cursorID int64, limit int) ([]EventRow, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	cur := pgtype.Timestamptz{Time: cursorAt, Valid: !cursorAt.IsZero()}
	if !cur.Valid {
		// sqlc generated a zero-time sentinel comparison; pass an
		// explicit zero to match it.
		cur = pgtype.Timestamptz{Valid: true}
	}
	rows, err := s.queries().ListResourceEvents(ctx, dbq.ListResourceEventsParams{
		ResourceID: resourceID,
		CursorAt:   cur,
		CursorID:   cursorID,
		Lim:        int32(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]EventRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, EventRow{
			ID:         r.ID,
			ResourceID: r.ResourceID,
			Type:       r.Type,
			Actor:      derefString(r.Actor),
			Reason:     derefString(r.Reason),
			Message:    derefString(r.Message),
			Detail:     r.Detail,
			CreatedAt:  r.CreatedAt.Time,
		})
	}
	return out, nil
}
