package api

import (
	"context"
	"net/http"
	"strconv"

	"github.com/danielgtaylor/huma/v2"

	"github.com/salesforce/converge/internal/model"
)

// Spec revision history + rollback (roots). The live spec is inline on
// resources.spec; root revisions are logged to spec_versions. History lists
// those versions; rollback CHECKS OUT a chosen version (by authored
// generation) as the live spec — copying its body inline + bumping
// generation, navigable in any order. Children carry spec inline and aren't
// versioned, so this is roots-only.

// listSpecHistory returns a root's spec revisions, newest-first, bodies
// elided. A non-root (or a root with no edits) returns an empty list.
func (s *Server) listSpecHistory(ctx context.Context, in *listSpecHistoryInput) (*listSpecHistoryOutput, error) {
	// Single query by (kind, name) — the root is resolved inside the query.
	rows, err := s.readRepo.ListSpecHistory(ctx, model.Kind(in.Kind), in.Name, in.Limit)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	out := &listSpecHistoryOutput{}
	out.Body.Revisions = make([]specRevisionItem, len(rows))
	for i, r := range rows {
		out.Body.Revisions[i] = specRevisionItem{
			Generation: r.Generation,
			Source:     r.Source,
			SizeBytes:  r.SizeBytes,
			IsCurrent:  r.IsCurrent,
			CreatedAt:  r.CreatedAt.Time,
		}
	}
	return out, nil
}

// rollbackResource checks out a historic revision (by authored generation)
// as the live spec. Returns 200 with the resource's generation after
// checkout, or 404 if the (id, generation) pair doesn't exist.
func (s *Server) rollbackResource(ctx context.Context, in *rollbackInput) (*rollbackOutput, error) {
	id, err := s.resolveID(ctx, in.Kind, in.Name)
	if err != nil {
		return nil, err
	}
	gen, err := s.commands.RollbackSpec(ctx, id, in.Body.Generation, "api")
	if err != nil {
		return nil, internalError(ctx, err)
	}
	if gen == 0 {
		return nil, huma.Error404NotFound("revision not found")
	}
	out := &rollbackOutput{Status: http.StatusOK}
	out.Body.Generation = gen
	return out, nil
}

// downloadSpecRevisionInput is the path input for the historic-spec download.
// Path fields declared directly (not embedded) — see rawDownloadInput: a Huma
// StreamResponse op doesn't promote an embedded struct's path tags.
type downloadSpecRevisionInput struct {
	Kind       string `path:"kind" doc:"Resource kind."`
	Name       string `path:"name" doc:"Resource name."`
	Generation int64  `path:"generation" doc:"Authored generation to download"`
}

// downloadSpecRevision streams a historic spec body (one revision from
// spec_versions) as a downloadable JSON file. A Huma StreamResponse: raw bytes
// verbatim, no envelope, so the downloaded file is the exact stored spec.
func (s *Server) downloadSpecRevision(ctx context.Context, in *downloadSpecRevisionInput) (*huma.StreamResponse, error) {
	body, err := s.readRepo.GetSpecRevision(ctx, model.Kind(in.Kind), in.Name, in.Generation)
	if err != nil || body == nil {
		return nil, huma.Error404NotFound("spec revision not found")
	}
	return jsonAttachment("spec", "gen"+strconv.FormatInt(in.Generation, 10), "revision", body), nil
}
