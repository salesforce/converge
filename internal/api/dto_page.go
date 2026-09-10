package api

import (
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// graphInput is the flat (non-paginated) owner-scoped graph view. It takes a
// bounded `limit` so a composer root with millions of children can never stream
// an unbounded response; walk large sets via the keyset-paginated /children
// collection instead.
type graphInput struct {
	Kind  string `path:"kind" doc:"Owner resource kind."`
	Name  string `path:"name" doc:"Owner resource name."`
	Limit int    `query:"limit" default:"500" minimum:"1" maximum:"1000" doc:"Max nodes AND max edges returned."`
}

// ── roots page (paged + filtered list of root resources) ───────────────
type listRootsPageInput struct {
	Kinds []string `query:"kind,explode" doc:"Repeatable kind filter (any version of these kinds)."`
	// KV is the repeatable (kind, kind_version) PAIR filter — the Resources-page
	// kind/version chips, each "kind/vN" (e.g. kv=vpc/v1&kv=account/v2). Matches
	// resources at exactly those (kind, kind_version) pairs. Independent of `kind`
	// above (which is version-agnostic). Malformed entries are ignored.
	KV            []string  `query:"kv,explode" doc:"Repeatable (kind, version) pair filter as kind/vN (e.g. vpc/v1). Exact per-pair match."`
	KindVersion   int       `query:"kind_version" minimum:"0" maximum:"32767" doc:"DEPRECATED single-version filter (use kv=kind/vN); 0/omitted = any version. Only meaningful alongside a kind filter."`
	NameLike      string    `query:"name" doc:"ILIKE substring filter."`
	CreatedAfter  time.Time `query:"created_after" doc:"RFC3339."`
	CreatedBefore time.Time `query:"created_before" doc:"RFC3339."`
	UpdatedAfter  time.Time `query:"updated_after" doc:"RFC3339."`
	UpdatedBefore time.Time `query:"updated_before" doc:"RFC3339."`
	GenMin        int64     `query:"gen_min" doc:"Min generation."`
	GenMax        int64     `query:"gen_max" doc:"Max generation."`
	CountMin      int64     `query:"count_min" doc:"Min child count."`
	CountMax      int64     `query:"count_max" doc:"Max child count."`
	Phase         string    `query:"phase" enum:"Ready,Reconciling,Degraded,Failed,Deleting,Orphaned,Quarantined" doc:"Only return rows in this readiness phase."`
	Limit         int       `query:"limit" default:"50" minimum:"1" maximum:"500"`
	// Cursor is an OPAQUE keyset token (echoed from X-Next-Cursor); it packs
	// the (created_at, id) keyset so no id appears bare on the wire.
	Cursor string `query:"cursor"`
}

type listRootsPageOutput struct {
	NextCursor string `header:"X-Next-Cursor"`
	Total      string `header:"X-Total"`
	Body       struct {
		Roots []rootPageItem `json:"roots"`
	}
}

// ── owner-scoped page (children of a single owner) ─────────────────────
type listResourcesPageInput struct {
	Kind        string   `path:"kind" doc:"Resource kind."`
	Name        string   `path:"name" doc:"Resource name; identity together with kind."`
	Kinds       []string `query:"kind,explode"`
	KV          []string `query:"kv,explode" doc:"Repeatable (kind, version) pair filter as kind/vN (e.g. vpc/v1). Exact per-pair match."`
	KindVersion int      `query:"kind_version" minimum:"0" maximum:"32767" doc:"DEPRECATED single-version filter (use kv=kind/vN); 0/omitted = any."`
	Phases      []string `query:"phase,explode" enum:"Ready,Reconciling,Degraded,Failed,Deleting,Orphaned,Quarantined" doc:"Return rows in ANY of these readiness phases (repeat the param to union; omit for all)."`
	NameLike    string   `query:"name"`
	Labels      []string `query:"label,explode"`
	Limit       int      `query:"limit" default:"100" minimum:"1" maximum:"500"`
	// Cursor is an OPAQUE keyset pagination token (echoed from X-Next-Cursor);
	// it encodes (created_at, id) internally so no id ever appears bare.
	Cursor string `query:"cursor"`
}

type listResourcesPageOutput struct {
	NextCursor string `header:"X-Next-Cursor"`
	Body       struct {
		Owner     resourceInfo       `json:"owner"`
		Resources []resourceListItem `json:"resources"`
	}
}

// ── multi-owner / cross-cluster page ───────────────────────────────────
// Owners scope by PUBLIC (kind, name) refs: repeat ?owner=kind/name. The handler
// batch-resolves them to ids and delegates to the id-based page query.
type listResourcesPageMultiInput struct {
	Owners      []string `query:"owner,explode" doc:"Scope to these owners' children; each is kind/name. Repeat for a union."`
	Kinds       []string `query:"kind,explode"`
	KV          []string `query:"kv,explode" doc:"Repeatable (kind, version) pair filter as kind/vN (e.g. vpc/v1). Exact per-pair match."`
	KindVersion int      `query:"kind_version" minimum:"0" maximum:"32767" doc:"DEPRECATED single-version filter (use kv=kind/vN); 0/omitted = any."`
	Phases      []string `query:"phase,explode" enum:"Ready,Reconciling,Degraded,Failed,Deleting,Orphaned,Quarantined" doc:"Return rows in ANY of these readiness phases (repeat the param to union; omit for all)."`
	NameLike    string   `query:"name"`
	Labels      []string `query:"label,explode"`
	Limit       int      `query:"limit" default:"100" minimum:"1" maximum:"500"`
	Cursor      string   `query:"cursor"`
}

type listResourcesPageMultiOutput struct {
	NextCursor string `header:"X-Next-Cursor"`
	Body       struct {
		Owners    []resourceInfo     `json:"owners"`
		Resources []resourceListItem `json:"resources"`
	}
}

// ── summary ────────────────────────────────────────────────────────────

// readinessKindCount mirrors dbq.CountChildrenByReadiness's per-kind
// phase breakdown. The UI summary card renders one stacked bar per kind
// from these six buckets (the generated `phase` values).
type readinessKindCount struct {
	Kind        string `json:"kind"`
	Ready       int64  `json:"ready"`
	Reconciling int64  `json:"reconciling"`
	Degraded    int64  `json:"degraded"`
	Failed      int64  `json:"failed"`
	Deleting    int64  `json:"deleting"`
	Orphaned    int64  `json:"orphaned"`
	Quarantined int64  `json:"quarantined"`
}

// readinessTotals is the "all kinds together" rollup used as the top
// counters above the per-kind table.
type readinessTotals struct {
	Ready       int64 `json:"ready"`
	Reconciling int64 `json:"reconciling"`
	Degraded    int64 `json:"degraded"`
	Failed      int64 `json:"failed"`
	Deleting    int64 `json:"deleting"`
	Orphaned    int64 `json:"orphaned"`
	Quarantined int64 `json:"quarantined"`
}

type summaryOutput struct {
	Body struct {
		Owner  resourceInfo         `json:"owner"`
		Totals readinessTotals      `json:"totals"`
		ByKind []readinessKindCount `json:"by_kind"`
	}
}

type summaryScopedInput struct {
	Owners []string `query:"owner,explode" doc:"Scope to these owners; each is kind/name. Repeat for a union."`
}

type summaryScopedOutput struct {
	Body struct {
		Owners []resourceInfo       `json:"owners"`
		Totals readinessTotals      `json:"totals"`
		ByKind []readinessKindCount `json:"by_kind"`
	}
}

// ── dependencies (upstream wait status + value flow mappings) ─────────
// valueMappingDTO is one declared value-flow on a dep edge: the
// dependent field that gets patched from the named source field. A flow
// has no per-flow latch — the bootstrap window is closed by the
// dependent's own dep-gate (synced_gen < generation), and the
// drain_outbox_batch substitute pass keeps re-applying the patch on
// every upstream status change. A declared mapping is the wired flow's
// steady state, not a "done" mark.
type valueMappingDTO struct {
	DependentField string `json:"dependent_field"`
	SourceField    string `json:"source_field"`
}

// upstreamDep is one upstream this resource depends on plus the
// per-edge value flows. UpstreamReady is the is_ready rollup from the
// upstream row; UpstreamHealthOK is its Ready/health axis, so the UI can
// distinguish "upstream not synced yet" from "upstream synced but
// unhealthy". ValueMappings lists every flow declared on the edge so the
// UI can show the wired data path; flows have no latch state of their
// own (the substitute pass keeps re-applying them), so they're shown
// whether or not the upstream has produced a value yet.
type upstreamDep struct {
	Kind              string             `json:"kind"`
	Name              string             `json:"name"`
	UpstreamReady     bool               `json:"upstream_ready"`
	UpstreamHealthOK  bool               `json:"upstream_health_ok"`
	UpstreamUpdatedAt pgtype.Timestamptz `json:"upstream_updated_at,omitempty"`
	ValueMappings     []valueMappingDTO  `json:"value_mappings"`
}

type listDependenciesOutput struct {
	Body struct {
		Dependencies []upstreamDep `json:"dependencies"`
	}
}

// ── changes ────────────────────────────────────────────────────────────
type changesInput struct {
	Kind  string    `path:"kind" doc:"Resource kind."`
	Name  string    `path:"name" doc:"Resource name; identity together with kind."`
	Since time.Time `query:"since" doc:"Only resources whose updated_at > this."`
}

type changesOutput struct {
	NextCursor string `header:"X-Next-Cursor"`
	More       string `header:"X-More" doc:"true if response was capped at 1000."`
	Body       struct {
		Owner     resourceInfo       `json:"owner"`
		Resources []resourceListItem `json:"resources"`
	}
}

// ── events (resource audit log) ────────────────────────────────────────
type listEventsInput struct {
	Kind string `path:"kind" doc:"Resource kind."`
	Name string `path:"name" doc:"Resource name; identity together with kind."`
	// Cursor is an OPAQUE keyset token (echoed from X-Next-Cursor) packing the
	// event's (created_at, sequence) so the bigint sequence never appears bare.
	Cursor string `query:"cursor" doc:"Opaque pagination token for paging older events."`
	Limit  int    `query:"limit" default:"100" minimum:"1" maximum:"500"`
}

// eventItem is one audit-log row. It carries NO id: the internal bigint sequence
// is an ordering key only (packed into the opaque page cursor), and the event is
// already scoped to its resource by the route, so no resource id is echoed.
type eventItem struct {
	Type      string          `json:"type"`
	Actor     string          `json:"actor,omitempty"`
	Reason    string          `json:"reason,omitempty"`
	Message   string          `json:"message,omitempty"`
	Detail    json.RawMessage `json:"detail"`
	CreatedAt time.Time       `json:"created_at"`
}

type listEventsOutput struct {
	NextCursor string `header:"X-Next-Cursor"`
	Body       struct {
		Events []eventItem `json:"events"`
	}
}
