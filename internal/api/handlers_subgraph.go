package api

import (
	"context"

	"github.com/google/uuid"

	"github.com/salesforce/converge/internal/model"
)

// Subgraph: N-hop dependency walk centered on a single resource.
// Used by the resource-detail panel's "View subgraph" button.

func (s *Server) getSubgraph(ctx context.Context, in *subgraphInput) (*subgraphOutput, error) {
	// The walk centers on (kind, name), resolved inside the query — no probe.
	rows, err := s.readRepo.SubgraphTransitive(ctx, model.Kind(in.Kind), in.Name, in.AncestorDepth, in.DescendantDepth, in.Limit)
	if err != nil {
		return nil, internalError(ctx, err)
	}

	cache := loadOwnerCacheFromSlimRows(s, ctx, rows)

	nodeIDs := make([]uuid.UUID, len(rows))
	for i, r := range rows {
		nodeIDs[i] = r.ID
	}
	deps, _ := s.readRepo.SubgraphDeps(ctx, nodeIDs)

	out := &subgraphOutput{}
	out.Body.Nodes = make([]subgraphNode, len(rows))
	for i, r := range rows {
		out.Body.Nodes[i] = subgraphNode{
			resourceListItem: rowToListItem(r, cache),
			Ref:              ref(string(r.Kind), r.Name),
			MinDepth:         r.MinDepth,
		}
	}
	// Edges join nodes by their public kind/name ref — never an id.
	out.Body.Edges = make([]graphEdge, len(deps))
	for i, d := range deps {
		out.Body.Edges[i] = graphEdge{
			From: ref(d.DependentKind, d.DependentName),
			To:   ref(d.DependencyKind, d.DependencyName),
		}
	}
	return out, nil
}
