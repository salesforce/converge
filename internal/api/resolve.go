package api

import (
	"context"
	"errors"
	"strings"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// resolve.go is the public-identity → internal-id boundary for the HTTP layer.
// Resources are addressed on the wire by their PUBLIC (kind, name) — unique per
// uniq_resource_meta. Each name-keyed handler resolves that pair to the internal
// uuid ONCE, at its top, then delegates to the unchanged id-based store reads.
// This keeps the whole SQL/store layer internally id-based (no churn, no perf
// hit beyond a single indexed probe) while the id never leaves the app.

// ref builds the PUBLIC "kind/name" graph-join key that node/edge DTOs use in
// place of any internal id.
func ref(kind, name string) string { return kind + "/" + name }

// resolveID translates (kind, name) into the internal resource id. Every
// former-{id} handler starts with this. It distinguishes ABSENCE from a read
// FAULT: no such (kind, name) → 404; a genuine DB error → 500 (internalError,
// logged). Conflating them hid outages behind a 404 (a transient DB failure
// looked like a missing resource).
func (s *Server) resolveID(ctx context.Context, kind, name string) (uuid.UUID, error) {
	id, err := s.readRepo.ResolveResourceIDByKindName(ctx, model.Kind(kind), name)
	if err != nil {
		if errors.Is(err, store.ErrResourceNotFound) {
			return uuid.UUID{}, huma.Error404NotFound("resource not found: " + kind + "/" + name)
		}
		return uuid.UUID{}, internalError(ctx, err)
	}
	return id, nil
}

// parseOwnerRefs turns the repeated ?owner=kind/name query values into ObjectRefs.
// Splits on the FIRST '/': '/' is the kind/name delimiter and neither a kind nor a
// name contains one (names use hyphens as their own separator), so the first '/'
// unambiguously ends the kind. Malformed entries (no '/') are skipped.
func parseOwnerRefs(raw []string) []store.ObjectRef {
	if len(raw) == 0 {
		return nil
	}
	out := make([]store.ObjectRef, 0, len(raw))
	for _, s := range raw {
		kind, name, ok := strings.Cut(s, "/")
		if !ok || kind == "" || name == "" {
			continue
		}
		out = append(out, store.ObjectRef{Kind: kind, Name: name})
	}
	return out
}

// resolveOwnerIDs batch-resolves ?owner=kind/name refs to internal ids for the
// cross-owner scoping filters. Semantics matter for correctness:
//   - NO refs supplied            → nil  → the scoped queries treat a nil/NULL
//     owner array as "whole cluster" (the correct unscoped default).
//   - refs supplied, some resolve → those ids (a partial miss just contributes
//     fewer rows to the union).
//   - refs supplied, NONE resolve → a SENTINEL [uuid.Nil], NOT nil. A caller
//     that scoped to owners that don't exist (a stale ?owner= bookmark, a typo,
//     a deleted owner) must get an EMPTY result, never the whole cluster. Since
//     the scoped SQL is `owner_ids IS NULL OR owner_id = ANY(owner_ids)`, a nil
//     here would silently widen to every resource. uuid.Nil is never a real
//     resource id, so `owner_id = ANY('{00000000-…}')` matches nothing — a
//     non-nil, len>0 array that passes the page handler's `len>0` guard too.
//
// A DB read FAULT is returned as an error (→ 500 at the caller), NOT folded into the
// empty-scope sentinel: conflating a transient failure with "these owners don't exist"
// would return a spurious 200-with-zero-rows during an outage, hiding it (the same
// absence-vs-fault distinction resolveID enforces).
func (s *Server) resolveOwnerIDs(ctx context.Context, raw []string) ([]uuid.UUID, error) {
	refs := parseOwnerRefs(raw)
	if len(refs) == 0 {
		return nil, nil // no scope requested → whole cluster
	}
	resolved, err := s.readRepo.ResolveResourceIDsByKindName(ctx, refs)
	if err != nil {
		return nil, err // read fault → surface it; the caller maps to 500
	}
	if len(resolved) == 0 {
		// Refs WERE supplied but none resolved → empty scope, not whole cluster.
		return []uuid.UUID{uuid.Nil}, nil
	}
	ids := make([]uuid.UUID, len(resolved))
	for i, r := range resolved {
		ids[i] = r.ID
	}
	return ids, nil
}
