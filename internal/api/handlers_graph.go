package api

import (
	"context"
)

// Graph: full children + dep edges of a single owner. /subgraph
// covers the multi-hop walk; this endpoint flattens the immediate
// owner-scoped graph for the dashboard's owner detail view.

func (s *Server) getGraph(ctx context.Context, in *graphInput) (*graphOutput, error) {
	// One indexed read for the owner envelope + its id; the scoped queries below
	// reuse the id — no separate resolve probe. Both are LIMIT-capped (in.Limit)
	// so a huge owner can't stream an unbounded node/edge set.
	parent, id, err := s.loadParentInfo(ctx, in.Kind, in.Name)
	if err != nil {
		return nil, err
	}
	q := s.readRepo
	resources, err := q.GetChildrenByOwner(ctx, id, in.Limit)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	deps, err := q.GetDepsByOwner(ctx, id, in.Limit)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	out := &graphOutput{}
	out.Body.Owner = parent
	out.Body.Nodes = make([]graphNode, len(resources))
	for i, r := range resources {
		out.Body.Nodes[i] = graphNode{
			Ref:                 ref(string(r.Kind), r.Name),
			Kind:                string(r.Kind),
			Name:                r.Name,
			IsReady:             r.IsReady,
			DeletionRequestedAt: r.DeletionRequestedAt,
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
