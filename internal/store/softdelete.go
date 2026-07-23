package store

import (
	"context"

	"github.com/google/uuid"
	"github.com/salesforce/converge/internal/dbq"
)

// ─────────────────────────────────────────────────────────────────────────
// Soft-delete protocol.
// ─────────────────────────────────────────────────────────────────────────

// RequestResourceDeletion implements the K8s soft-delete contract: sets
// deletion_requested_at, seeds finalizers from the caller-provided
// FinalizerName, and enqueues a delete task. If finalizerName is empty,
// hard-deletes the row in-line. Returns true if the row existed.
func (s *Store) RequestResourceDeletion(ctx context.Context, id uuid.UUID, finalizerName, actor string) (bool, error) {
	return s.queries().RequestResourceDeletion(ctx, dbq.RequestResourceDeletionParams{
		PID:        id,
		PFinalizer: finalizerName,
		PActor:     actor,
	})
}

// QuarantineResource sets a failed/stuck resource aside (freezes it from all
// schedulers + unblocks its root's rollup, without deleting it). Returns true
// if the row existed and was quarantinable (not mid-teardown / not already
// quarantined). See quarantine_resource in 00001_schema.sql.
func (s *Store) QuarantineResource(ctx context.Context, id uuid.UUID, actor string) (bool, error) {
	return s.queries().QuarantineResource(ctx, dbq.QuarantineResourceParams{PID: id, PActor: actor})
}

// UnquarantineResource clears a quarantine and re-arms the resource (the SQL
// function calls schedule_eligible on it). Returns true if the row existed and
// was quarantined. See unquarantine_resource in 00001_schema.sql.
func (s *Store) UnquarantineResource(ctx context.Context, id uuid.UUID, actor string) (bool, error) {
	return s.queries().UnquarantineResource(ctx, dbq.UnquarantineResourceParams{PID: id, PActor: actor})
}
