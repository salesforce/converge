package store

import (
	"context"

	"github.com/google/uuid"
	"github.com/salesforce/converge/internal/dbq"
)

// ─────────────────────────────────────────────────────────────────────────
// Operations (subresources).
// ─────────────────────────────────────────────────────────────────────────

// GetOperation returns one operation by id.
func (s *Store) GetOperation(ctx context.Context, id uuid.UUID) (dbq.ResourceOperation, error) {
	return s.queries().GetOperation(ctx, id)
}

// SetOperationRunning marks an op row as running and bumps attempts.
func (s *Store) SetOperationRunning(ctx context.Context, id uuid.UUID) error {
	return s.queries().SetOperationRunning(ctx, id)
}
