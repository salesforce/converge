package api

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/salesforce/converge/internal/store"
)

type slimResourceRow interface {
	store.GetChildrenByOwnerRow |
		store.GetResourcesChangedSinceRow |
		store.ListResourcesPageRow |
		store.SubgraphTransitiveRow
}

func rowToListItem[R slimResourceRow](r R, cache map[uuid.UUID]ownerRef) resourceListItem {
	switch v := any(r).(type) {
	case store.GetChildrenByOwnerRow:
		return slimFromFields(string(v.Kind), 0, v.Name, v.OwnerID,
			v.Generation, v.SyncedGen, v.IsReady, v.Phase,
			v.DeletionRequestedAt, v.Labels, v.CreatedAt, v.UpdatedAt, cache)
	case store.GetResourcesChangedSinceRow:
		return slimFromFields(string(v.Kind), 0, v.Name, v.OwnerID,
			v.Generation, v.SyncedGen, v.IsReady, v.Phase,
			v.DeletionRequestedAt, v.Labels, v.CreatedAt, v.UpdatedAt, cache)
	case store.ListResourcesPageRow:
		// Only the list-page row carries kind_version (added to that SELECT for the
		// version filter/display); the other slim rows leave it 0 (unset → omitted).
		return slimFromFields(string(v.Kind), int(v.KindVersion), v.Name, v.OwnerID,
			v.Generation, v.SyncedGen, v.IsReady, v.Phase,
			v.DeletionRequestedAt, v.Labels, v.CreatedAt, v.UpdatedAt, cache)
	case store.SubgraphTransitiveRow:
		return slimFromFields(string(v.Kind), 0, v.Name, v.OwnerID,
			v.Generation, v.SyncedGen, v.IsReady, v.Phase,
			v.DeletionRequestedAt, v.Labels, v.CreatedAt, v.UpdatedAt, cache)
	}
	return resourceListItem{}
}

func slimFromFields(
	kind string, kindVersion int, name string, ownerCol pgtype.UUID,
	gen, syncedGen int64, ready bool, phase string,
	delReq pgtype.Timestamptz, labels json.RawMessage,
	created, updated pgtype.Timestamptz,
	cache map[uuid.UUID]ownerRef,
) resourceListItem {
	var owner *uuid.UUID
	if ownerCol.Valid {
		owner = ptrFromBytes(ownerCol.Bytes)
	}
	item := resourceListItem{
		Kind:                kind,
		KindVersion:         kindVersion,
		Name:                name,
		Generation:          gen,
		SyncedGen:           syncedGen,
		IsReady:             ready,
		Phase:               phase,
		DeletionRequestedAt: delReq,
		Labels:              synthLabels(decodeJSONMap(labels), owner, cache),
		CreatedAt:           created,
		UpdatedAt:           updated,
	}
	if owner != nil {
		if ref, ok := cache[*owner]; ok {
			item.OwnerKind, item.OwnerName = ref.Kind, ref.Name
		}
	}
	return item
}

func loadOwnerCacheFromSlimRows[R slimResourceRow](
	s *Server, ctx context.Context, rows []R,
) map[uuid.UUID]ownerRef {
	owners := make([]pgtype.UUID, len(rows))
	for i, r := range rows {
		switch v := any(r).(type) {
		case store.GetChildrenByOwnerRow:
			owners[i] = v.OwnerID
		case store.GetResourcesChangedSinceRow:
			owners[i] = v.OwnerID
		case store.ListResourcesPageRow:
			owners[i] = v.OwnerID
		case store.SubgraphTransitiveRow:
			owners[i] = v.OwnerID
		}
	}
	return s.loadOwnerCacheFromIDs(ctx, owners)
}
