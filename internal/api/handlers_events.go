package api

import (
	"context"
	"strconv"
)

// Events: append-only audit log per resource.

func (s *Server) listEvents(ctx context.Context, in *listEventsInput) (*listEventsOutput, error) {
	id, err := s.resolveID(ctx, in.Kind, in.Name)
	if err != nil {
		return nil, err
	}
	// Decode the opaque page cursor into its (created_at, sequence) parts. The
	// bigint sequence lives INSIDE the token — it's never a bare field/param.
	cursorAt, cursorID := decodeCursorInt64(in.Cursor)
	rows, err := s.readRepo.ListEvents(ctx, id, cursorAt, cursorID, in.Limit)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	out := &listEventsOutput{}
	out.Body.Events = make([]eventItem, len(rows))
	for i, r := range rows {
		out.Body.Events[i] = eventItem{
			Type:      r.Type,
			Actor:     r.Actor,
			Reason:    r.Reason,
			Message:   r.Message,
			Detail:    r.Detail,
			CreatedAt: r.CreatedAt,
		}
	}
	// Next cursor packs the last row's (created_at, sequence) into one opaque
	// token so the internal sequence never appears bare on the wire.
	if len(rows) == in.Limit && len(rows) > 0 {
		last := rows[len(rows)-1]
		out.NextCursor = encodeCursor(last.CreatedAt.UTC(), strconv.FormatInt(last.ID, 10))
	}
	return out, nil
}
