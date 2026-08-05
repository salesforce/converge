package api

import (
	"context"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// projection_owner.go: resolving a resource's OWNER envelope + the owner-id →
// public (kind, name) cache the list projections use to synthesize owner labels
// without ever leaking an internal id.

// ownerRef is the resolved public identity of an owner, cached by the owner's
// internal id (the cache KEY is the only place an id is used — it never leaves
// the app). Roots have no owner so their entry is absent.
type ownerRef struct {
	Kind string
	Name string
}

// loadOwnerInfos resolves the full owner row for every DISTINCT valid id
// in `owners` with one batched GetResourcesInfo call. Callers that only
// need the owner_kind synth label use loadOwnerCacheFromIDs (a thin
// projection over this); callers that also echo owner envelopes to the UI
// (the multi-owner page group headers) consume the rows directly so a
// single round-trip serves both.
func (s *Server) loadOwnerInfos(ctx context.Context, owners []pgtype.UUID) []store.GetResourcesInfoRow {
	seen := map[uuid.UUID]struct{}{}
	for _, o := range owners {
		if o.Valid {
			seen[uuid.UUID(o.Bytes)] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return nil
	}
	ids := make([]uuid.UUID, 0, len(seen))
	for k := range seen {
		ids = append(ids, k)
	}
	infos, err := s.readRepo.GetResourcesInfo(ctx, ids)
	if err != nil {
		return nil
	}
	return infos
}

// loadOwnerCacheFromIDs resolves each owner id to its public (kind, name) ref,
// keyed by the internal id (the id is used ONLY as the map key). The list-row
// projections read this to fill owner_kind/owner_name without leaking the id.
func (s *Server) loadOwnerCacheFromIDs(ctx context.Context, owners []pgtype.UUID) map[uuid.UUID]ownerRef {
	infos := s.loadOwnerInfos(ctx, owners)
	if len(infos) == 0 {
		return nil
	}
	out := make(map[uuid.UUID]ownerRef, len(infos))
	for _, i := range infos {
		out[i.ID] = ownerRef{Kind: string(i.Kind), Name: i.Name}
	}
	return out
}

// loadParentInfo reads the owner envelope by its PUBLIC (kind, name) and returns
// it together with the owner's internal id — so an owner-scoped handler gets both
// the header AND the id its scoped query needs from ONE indexed read (no separate
// resolve probe). A missing owner is a 404.
func (s *Server) loadParentInfo(ctx context.Context, kind, name string) (resourceInfo, uuid.UUID, error) {
	parent, err := s.readRepo.GetResourceInfoByName(ctx, model.Kind(kind), name)
	if err != nil {
		return resourceInfo{}, uuid.UUID{}, huma.Error404NotFound("not found")
	}
	// Owner envelope carries synthesized Synced/Ready from the scalars
	// (no extra query — the stored Ready/custom rows would only matter
	// for the detail view, and the owner header just needs the two axes).
	// Its own owner (grandparent) ref is not carried by the info row and isn't
	// rendered on a header, so owner_kind/owner_name are left empty here.
	return resourceInfo{
		Kind:                string(parent.Kind),
		Name:                parent.Name,
		Generation:          parent.Generation,
		SyncedGen:           parent.SyncedGen,
		IsReady:             parent.IsReady,
		HealthOK:            parent.HealthOk,
		Phase:               parent.Phase,
		Conditions:          synthesizeConditions(parent.Generation, parent.SyncedGen, parent.HealthOk, parent.Phase, nil),
		DeletionRequestedAt: parent.DeletionRequestedAt,
		Labels:              decodeJSONMap(parent.Labels),
		CreatedAt:           parent.CreatedAt,
		UpdatedAt:           parent.UpdatedAt,
	}, parent.ID, nil
}

func infoFromGetResource(m store.GetResourcesInfoRow) resourceInfo {
	return resourceInfo{
		Kind:                string(m.Kind),
		Name:                m.Name,
		Generation:          m.Generation,
		SyncedGen:           m.SyncedGen,
		IsReady:             m.IsReady,
		HealthOK:            m.HealthOk,
		Phase:               m.Phase,
		Conditions:          synthesizeConditions(m.Generation, m.SyncedGen, m.HealthOk, m.Phase, nil),
		DeletionRequestedAt: m.DeletionRequestedAt,
		Labels:              decodeJSONMap(m.Labels),
		CreatedAt:           m.CreatedAt,
		UpdatedAt:           m.UpdatedAt,
	}
}
