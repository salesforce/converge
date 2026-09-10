package api

import (
	"context"
	"strconv"

	"github.com/danielgtaylor/huma/v2"
)

// Topology handlers: group-by-label drill-down across the children
// of a root, plus the leaves and label-key autocomplete endpoints.

func (s *Server) getTopology(ctx context.Context, in *topologyInput) (*topologyOutput, error) {
	parent, id, err := s.loadParentInfo(ctx, in.Kind, in.Name)
	if err != nil {
		return nil, err
	}
	if len(in.GroupBy) == 0 {
		return nil, huma.Error400BadRequest("group_by is required")
	}
	path, err := parsePathSegments(in.Path)
	if err != nil {
		return nil, huma.Error400BadRequest(err.Error())
	}

	level := len(path)
	if level >= len(in.GroupBy) {
		// Path consumed all group_by levels — caller should switch to
		// /topology/leaves; return empty.
		out := &topologyOutput{Total: "0"}
		out.Body.Owner = parent
		out.Body.GroupBy = in.GroupBy
		out.Body.Path = in.Path
		out.Body.NextKey = ""
		return out, nil
	}
	nextKey := in.GroupBy[level]

	buckets, err := s.readRepo.AggregateTopology(ctx, id, path, nextKey, in.Cursor, in.Limit)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	total, _ := s.readRepo.CountTopologyBuckets(ctx, id, path, nextKey)

	out := &topologyOutput{Total: strconv.Itoa(total)}
	out.Body.Owner = parent
	out.Body.GroupBy = in.GroupBy
	out.Body.Path = in.Path
	out.Body.NextKey = nextKey
	out.Body.Buckets = make([]topologyBucket, len(buckets))
	for i, b := range buckets {
		out.Body.Buckets[i] = topologyBucket{Bucket: b.Bucket, ByReadiness: b.ByReadiness, Total: b.Total}
	}
	if len(buckets) > 0 {
		out.NextCursor = buckets[len(buckets)-1].Bucket
	}
	return out, nil
}

func (s *Server) getTopologyLeaves(ctx context.Context, in *topologyLeavesInput) (*topologyLeavesOutput, error) {
	parent, id, err := s.loadParentInfo(ctx, in.Kind, in.Name)
	if err != nil {
		return nil, err
	}
	path, err := parsePathSegments(in.Path)
	if err != nil {
		return nil, huma.Error400BadRequest(err.Error())
	}
	// The leaves cursor keysets on (name, id); decode the opaque token into both.
	cursorName, cursorID := decodeCursorNameID(in.Cursor)
	rows, err := s.readRepo.TopologyLeaves(ctx, id, path, cursorName, cursorID, in.Limit)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	cache := loadOwnerCacheFromSlimRows(s, ctx, rows)
	out := &topologyLeavesOutput{}
	out.Body.Owner = parent
	out.Body.Resources = make([]resourceListItem, len(rows))
	for i, r := range rows {
		out.Body.Resources[i] = rowToListItem(r, cache)
	}
	// Next cursor packs the last leaf's (name, id) so the id stays opaque.
	if n := len(rows); n > 0 {
		out.NextCursor = encodeCursorNameID(rows[n-1].Name, rows[n-1].ID.String())
	}
	return out, nil
}

func (s *Server) getTopologyKeys(ctx context.Context, in *topologyKeysInput) (*topologyKeysOutput, error) {
	id, err := s.resolveID(ctx, in.Kind, in.Name)
	if err != nil {
		return nil, err
	}
	keys, err := s.readRepo.TopologyLabelKeys(ctx, id, in.Limit)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	out := &topologyKeysOutput{}
	out.Body.Keys = keys
	return out, nil
}
