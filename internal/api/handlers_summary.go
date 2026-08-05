package api

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/salesforce/converge/internal/store"
)

// Summary handlers: per-root and cross-cluster readiness/kind counts.
// The per-root summary also carries grouped failure samples; the
// cross-cluster (scoped) summary is counts-only. Used by the dashboard cards.

func (s *Server) getSummary(ctx context.Context, in *kindNamePath) (*summaryOutput, error) {
	parent, id, err := s.loadParentInfo(ctx, in.Kind, in.Name)
	if err != nil {
		return nil, err
	}
	rows, err := s.readRepo.CountChildrenByReadiness(ctx, id)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	out := &summaryOutput{}
	out.Body.Owner = parent
	out.Body.Totals, out.Body.ByKind = aggregateReadinessKind(rows)
	return out, nil
}

func (s *Server) getSummaryScoped(ctx context.Context, in *summaryScopedInput) (*summaryScopedOutput, error) {
	q := s.readRepo
	owners, err := s.resolveOwnerIDs(ctx, in.Owners)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	rows, err := q.CountResourcesByReadinessScoped(ctx, owners)
	if err != nil {
		return nil, internalError(ctx, err)
	}

	var ownerInfos []resourceInfo
	if len(owners) > 0 {
		infos, _ := q.GetResourcesInfo(ctx, owners)
		ownerInfos = make([]resourceInfo, len(infos))
		for i, m := range infos {
			ownerInfos[i] = infoFromGetResource(m)
		}
	}

	out := &summaryScopedOutput{}
	out.Body.Owners = ownerInfos
	out.Body.Totals, out.Body.ByKind = aggregateReadinessKindScoped(rows)
	return out, nil
}

func (s *Server) getChanges(ctx context.Context, in *changesInput) (*changesOutput, error) {
	parent, id, err := s.loadParentInfo(ctx, in.Kind, in.Name)
	if err != nil {
		return nil, err
	}
	rows, err := s.readRepo.GetResourcesChangedSince(ctx, store.GetResourcesChangedSinceParams{
		OwnerID:   pgtype.UUID{Bytes: id, Valid: true},
		UpdatedAt: pgtype.Timestamptz{Time: in.Since, Valid: true},
	})
	if err != nil {
		return nil, internalError(ctx, err)
	}
	cache := loadOwnerCacheFromSlimRows(s, ctx, rows)
	out := &changesOutput{}
	out.Body.Owner = parent
	out.Body.Resources = make([]resourceListItem, len(rows))
	maxAt := in.Since
	for i, r := range rows {
		out.Body.Resources[i] = rowToListItem(r, cache)
		if r.UpdatedAt.Valid && r.UpdatedAt.Time.After(maxAt) {
			maxAt = r.UpdatedAt.Time
		}
	}
	out.NextCursor = maxAt.Format(time.RFC3339Nano)
	if len(rows) == 1000 {
		out.More = "true"
	}
	return out, nil
}
