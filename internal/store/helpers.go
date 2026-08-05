package store

import (
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// Shared conversions used across the store's row↔model mapping. Kept in one place
// so the pgtype boilerplate isn't re-inlined at every call site.

// toUUID wraps a uuid.UUID as a non-null pgtype.UUID (what the sqlc-generated
// params expect). The store passes real ids, so Valid is always true.
func toUUID(u uuid.UUID) pgtype.UUID { return pgtype.UUID{Bytes: u, Valid: true} }

// derefString returns *p, or "" when nil — the nullable-text column → Go string
// mapping for row projections.
func derefString(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// isNoRows reports whether err is pgx's "no rows" sentinel — the store's uniform
// "not found" test, so a caller distinguishes absence from a real error.
func isNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }
