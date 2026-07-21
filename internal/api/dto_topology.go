package api

import (
	"github.com/jackc/pgx/v5/pgtype"
)

// ── topology ───────────────────────────────────────────────────────────
type topologyInput struct {
	Kind    string   `path:"kind" doc:"Resource kind."`
	Name    string   `path:"name" doc:"Resource name; identity together with kind."`
	GroupBy []string `query:"group_by,explode"`
	Path    []string `query:"path,explode" doc:"key:value pairs already drilled."`
	Cursor  string   `query:"cursor"`
	Limit   int      `query:"limit" default:"200" minimum:"1" maximum:"500"`
}

// topologyBucket: one drill-down cell. ByReadiness has keys
// {ready,not_ready,deleting} matching the SQL rollup.
type topologyBucket struct {
	Bucket      string         `json:"bucket"`
	ByReadiness map[string]int `json:"by_readiness"`
	Total       int            `json:"total"`
}

type topologyOutput struct {
	Total      string `header:"X-Total"`
	NextCursor string `header:"X-Next-Cursor"`
	Body       struct {
		Owner   resourceInfo     `json:"owner"`
		GroupBy []string         `json:"group_by"`
		Path    []string         `json:"path"`
		NextKey string           `json:"next_key"`
		Buckets []topologyBucket `json:"buckets"`
	}
}

type topologyLeavesInput struct {
	Kind string   `path:"kind" doc:"Resource kind."`
	Name string   `path:"name" doc:"Resource name; identity together with kind."`
	Path []string `query:"path,explode"`
	// Cursor is an OPAQUE keyset token (echoed from X-Next-Cursor) packing the
	// leaf's (name, id) keyset so no id appears bare.
	Cursor string `query:"cursor"`
	Limit  int    `query:"limit" default:"200" minimum:"1" maximum:"500"`
}

type topologyLeavesOutput struct {
	NextCursor string `header:"X-Next-Cursor"`
	Body       struct {
		Owner     resourceInfo       `json:"owner"`
		Resources []resourceListItem `json:"resources"`
	}
}

type topologyKeysInput struct {
	Kind  string `path:"kind" doc:"Resource kind."`
	Name  string `path:"name" doc:"Resource name; identity together with kind."`
	Limit int    `query:"limit" default:"100" minimum:"1" maximum:"500"`
}

type topologyKeysOutput struct {
	Body struct {
		Keys []string `json:"keys"`
	}
}

// ── subgraph ───────────────────────────────────────────────────────────
type subgraphInput struct {
	Kind            string `path:"kind" doc:"Resource kind."`
	Name            string `path:"name" doc:"Resource name; identity together with kind."`
	AncestorDepth   int    `query:"ancestor_depth" default:"3" minimum:"0" maximum:"10"`
	DescendantDepth int    `query:"descendant_depth" default:"1" minimum:"0" maximum:"10"`
	Limit           int    `query:"limit" default:"200" minimum:"1" maximum:"1000"`
}

type subgraphNode struct {
	resourceListItem
	// Ref is the node's PUBLIC "kind/name" join key — the graph edges reference
	// nodes by this, not by any internal id.
	Ref      string `json:"ref"`
	MinDepth int32  `json:"min_depth"`
}

type subgraphOutput struct {
	Body struct {
		Nodes []subgraphNode `json:"nodes"`
		Edges []graphEdge    `json:"edges"`
	}
}

// ── graph ──────────────────────────────────────────────────────────────
type graphOutput struct {
	Body struct {
		Owner resourceInfo `json:"owner"`
		Nodes []graphNode  `json:"nodes"`
		Edges []graphEdge  `json:"edges"`
	}
}

// graphNode: a child node in the per-owner overview graph. Ref is the node's
// PUBLIC "kind/name" join key (edges reference it, not an id). IsReady is the
// SQL rollup; DeletionRequestedAt non-nil means the node is in the finalizer
// drain so the UI can render it dimmed.
type graphNode struct {
	Ref                 string             `json:"ref"`
	Kind                string             `json:"kind"`
	Name                string             `json:"name"`
	IsReady             bool               `json:"is_ready"`
	DeletionRequestedAt pgtype.Timestamptz `json:"deletion_requested_at,omitempty"`
}

// graphEdge joins two nodes by their PUBLIC "kind/name" ref — never an id.
type graphEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
}
