package store

import (
	"context"

	"github.com/google/uuid"
	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/model"
)

// ─────────────────────────────────────────────────────────────────────────
// Subgraph reads.
// ─────────────────────────────────────────────────────────────────────────

// SubgraphTransitiveRow is the per-node row of a transitive subgraph
// walk, re-exported so the API projection layer maps it without
// importing dbq.
type SubgraphTransitiveRow = dbq.SubgraphTransitiveRow

type SubgraphDep = dbq.SubgraphDepsRow

func (s *Store) SubgraphDeps(ctx context.Context, ids []uuid.UUID) ([]SubgraphDep, error) {
	return s.queries().SubgraphDeps(ctx, ids)
}

// SubgraphTransitive centers the dependency walk on the resource addressed by
// (kind, name) — resolved inside the query, so no separate id probe.
func (s *Store) SubgraphTransitive(
	ctx context.Context,
	kind model.Kind, name string,
	ancestorDepth, descendantDepth, limit int,
) ([]SubgraphTransitiveRow, error) {
	return s.queries().SubgraphTransitive(ctx, dbq.SubgraphTransitiveParams{
		Kind:    string(kind),
		Name:    name,
		Column3: int32(ancestorDepth),
		Column4: int32(descendantDepth),
		Column5: int32(limit),
	})
}

// ListDescendants returns the entire subtree rooted at rootID, each row
// carrying resources.* plus name/labels joined from
// resource_meta (ResourceRow).
func (s *Store) ListDescendants(ctx context.Context, rootID uuid.UUID) ([]ResourceRow, error) {
	return s.queries().ListDescendants(ctx, toUUID(rootID))
}

// DescendantsSettled reports whether no LIVE descendant of root is still
// lagging — the dispatcher's rollup-readiness gate (see the DescendantsSettled
// query in db/queries/resources.sql for the predicate contract).
func (s *Store) DescendantsSettled(ctx context.Context, rootID uuid.UUID) (bool, error) {
	return s.queries().DescendantsSettled(ctx, toUUID(rootID))
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
