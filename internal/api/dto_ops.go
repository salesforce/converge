package api

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// ── operations (subresource verb invocations) ──────────────────────────

// invokeOpInput is the request body for POST /api/resources/{kind}/{name}/ops/{verb}.
// Input is opaque JSON; the schema is the verb's input schema from the kind's
// manifest (rendered in /api/kinds/{kind}/schema).
type invokeOpInput struct {
	Kind string `path:"kind" doc:"Resource kind."`
	Name string `path:"name" doc:"Resource name; identity together with kind."`
	// The verb + input are the request BODY: this POST CREATES an operation in the
	// resource's /ops collection (returning the created row at 202), so the verb is
	// part of the representation, not a path segment. Keeping it out of the path also
	// frees .../ops/{op_uid} to be the operation ITEM slot (GET now, DELETE/cancel
	// later) with no template collision against a verb name.
	Body struct {
		Verb  string          `json:"verb" minLength:"1" doc:"Operator verb name (must be a declared verb of the kind)."`
		Input json.RawMessage `json:"input,omitempty" doc:"Verb-specific input."`
	}
}

// operationDTO is one row from resource_operations rendered for the API. An
// operation is a transient sub-resource with no natural name; UID is its opaque
// handle (used to fetch it back at .../ops/{op_uid}), NOT a resource id. It
// carries no resource_id — the op is already scoped to its resource by the route.
type operationDTO struct {
	UID          string             `json:"uid"`
	Verb         string             `json:"verb"`
	Input        json.RawMessage    `json:"input,omitempty"`
	Output       json.RawMessage    `json:"output,omitempty"`
	State        string             `json:"state"`
	ErrorMessage *string            `json:"error_message,omitempty"`
	Attempts     int32              `json:"attempts"`
	RequestedBy  *string            `json:"requested_by,omitempty"`
	RequestedAt  pgtype.Timestamptz `json:"requested_at"`
	CompletedAt  pgtype.Timestamptz `json:"completed_at,omitempty"`
}

type invokeOpOutput struct {
	Status int `header:"-"`
	Body   operationDTO
}

type listOpsInput struct {
	Kind string `path:"kind" doc:"Resource kind."`
	Name string `path:"name" doc:"Resource name; identity together with kind."`
	// enum lists the legal operation states — huma rejects an out-of-set value at the
	// edge (422), so an unknown filter can't reach the query.
	States []string `query:"state,explode" enum:"pending,running,succeeded,failed" doc:"Filter by state; repeat for a union."`
	Limit  int      `query:"limit" default:"100" minimum:"1" maximum:"500"`
}

type listOpsOutput struct {
	Body struct {
		Operations []operationDTO `json:"operations"`
	}
}

type getOpInput struct {
	Kind  string    `path:"kind" doc:"Resource kind."`
	Name  string    `path:"name" doc:"Resource name; identity together with kind."`
	OpUID uuid.UUID `path:"op_uid" doc:"Opaque operation handle."`
}

type getOpOutput struct {
	Body operationDTO
}

// ── spec revision history (roots) ──────────────────────────────────────
type listSpecHistoryInput struct {
	Kind  string `path:"kind" doc:"Resource kind."`
	Name  string `path:"name" doc:"Resource name; identity together with kind."`
	Limit int    `query:"limit" default:"50" minimum:"1" maximum:"200"`
}

// specRevisionItem is one entry in a root's spec history — metadata only,
// no body (the UI lazy-fetches a body via /api/raw/.../spec/{generation}).
// A revision is identified by its authored generation.
type specRevisionItem struct {
	Generation int64     `json:"generation"`
	Source     string    `json:"source"`
	SizeBytes  int64     `json:"size_bytes"`
	IsCurrent  bool      `json:"is_current"`
	CreatedAt  time.Time `json:"created_at"`
}

type listSpecHistoryOutput struct {
	Body struct {
		Revisions []specRevisionItem `json:"revisions"`
	}
}

type rollbackInput struct {
	Kind string `path:"kind" doc:"Resource kind."`
	Name string `path:"name" doc:"Resource name; identity together with kind."`
	Body struct {
		Generation int64 `json:"generation" doc:"The authored generation to check out."`
	}
}

type rollbackOutput struct {
	Status int `header:"-"`
	Body   struct {
		Generation int64 `json:"generation"`
	}
}
