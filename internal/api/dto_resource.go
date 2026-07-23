package api

import (
	"encoding/json"

	"github.com/jackc/pgx/v5/pgtype"
)

// kindNamePath is the universal {kind}/{name} path identity for resource
// endpoints. A resource is addressed by its PUBLIC (kind, name) — unique per
// uniq_resource_meta — never by the internal uuid. Handlers resolve it to an id
// once at their top (see resolveID) and delegate to the id-based reads.
type kindNamePath struct {
	Kind string `path:"kind" doc:"Resource kind."`
	Name string `path:"name" doc:"Resource name; identity together with kind."`
}

// conditionDTO is one K8s/Crossplane-style status condition. The Synced
// axis and happy-path Ready=True are synthesized by the API from the
// resource's scalar axes (synced_gen vs generation, health_ok); custom
// and non-default Ready conditions come from the resource_conditions
// table. LastTransitionTime is the K8s lastTransitionTime (zero/omitted
// for synthesized conditions that were never stored).
type conditionDTO struct {
	Type               string             `json:"type"`
	Status             string             `json:"status"`
	Reason             string             `json:"reason,omitempty"`
	Message            string             `json:"message,omitempty"`
	ObservedGeneration int64              `json:"observed_generation"`
	LastTransitionAt   pgtype.Timestamptz `json:"last_transition_at,omitempty"`
}

// resourceInfo is the parent/owner shape embedded in list responses.
// Used for the "this is what you're scoped under" envelope on
// owner-scoped pages (children/page, topology/leaves, summary).
//
// Carries both K8s readiness axes: SyncedGen vs Generation is the Synced
// axis, HealthOK is the Ready/health axis, and IsReady is their AND
// (plus not-deleting). Conditions is the full synthesized + stored array
// (Synced/Ready/custom). DeletionRequestedAt is non-nil when the row is
// in the soft-delete finalizer drain.
type resourceInfo struct {
	Kind                string             `json:"kind"`
	KindVersion         int                `json:"kind_version,omitempty"`
	Name                string             `json:"name"`
	OwnerKind           string             `json:"owner_kind,omitempty"`
	OwnerName           string             `json:"owner_name,omitempty"`
	Generation          int64              `json:"generation"`
	SyncedGen           int64              `json:"synced_gen"`
	IsReady             bool               `json:"is_ready"`
	HealthOK            bool               `json:"health_ok"`
	Phase               string             `json:"phase"`
	Conditions          []conditionDTO     `json:"conditions,omitempty"`
	Finalizers          []string           `json:"finalizers,omitempty"`
	DeletionRequestedAt pgtype.Timestamptz `json:"deletion_requested_at,omitempty"`
	Labels              map[string]string  `json:"labels"`
	CreatedAt           pgtype.Timestamptz `json:"created_at"`
	UpdatedAt           pgtype.Timestamptz `json:"updated_at"`
}

// resourceListItem is the slim row shape returned by list/page/
// subgraph/topology-leaves endpoints. The UI list views render only the
// `phase` badge, name, kind, generation pair, and updated_at; everything
// else is fetched on click via GET /api/resources/{id}.
//
// `phase` is THE classification signal: a single string the server
// computed (the generated resources.phase scalar), so the client renders
// it verbatim and never re-derives readiness. This is what makes the
// failed-filter result and the rendered badge structurally identical —
// no conditions array is shipped on a list row.
//
// Stripped vs resourceInfo:
//   - spec, status: huge for composer kinds (~MB compressed,
//     dozens of MB JSON) and never rendered in lists.
//   - health_ok, conditions, finalizers: not consumed by any list view
//     (phase subsumes them).
//
// Adding fields here is a wire-protocol commitment: they get fetched
// on every page poll. Detail-only fields belong on resourceFull.
type resourceListItem struct {
	Kind                string             `json:"kind"`
	KindVersion         int                `json:"kind_version,omitempty"`
	Name                string             `json:"name"`
	OwnerKind           string             `json:"owner_kind,omitempty"`
	OwnerName           string             `json:"owner_name,omitempty"`
	Generation          int64              `json:"generation"`
	SyncedGen           int64              `json:"synced_gen"`
	IsReady             bool               `json:"is_ready"`
	Phase               string             `json:"phase"`
	DeletionRequestedAt pgtype.Timestamptz `json:"deletion_requested_at,omitempty"`
	Labels              map[string]string  `json:"labels"`
	CreatedAt           pgtype.Timestamptz `json:"created_at"`
	UpdatedAt           pgtype.Timestamptz `json:"updated_at"`
}

// resourceFull is resourceInfo + spec + status. Inlined fields (not
// embedded) because huma's OpenAPI generator doesn't flatten Go
// embeddings.
// providerConfigRefDTO is the detail-page link to a resource's attached custom
// provider config: name is the global handle the provider-configs page resolves
// by, kind == the resource's kind.
type providerConfigRefDTO struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

type resourceFull struct {
	// UID is the resource's INTERNAL uuid, surfaced ONLY here (the full detail
	// read) as the k8s metadata.uid analogue: an opaque, stable incarnation
	// handle for "same name, different object" detection. It is NEVER an
	// addressing argument (resources are addressed by kind/name) and never
	// appears in list rows, graph edges, or cross-object refs.
	UID  string `json:"uid"`
	Kind string `json:"kind"`
	// KindVersion is the resource's pinned web-API version (v1, v2, …).
	KindVersion         int                `json:"kind_version"`
	Name                string             `json:"name"`
	OwnerKind           string             `json:"owner_kind,omitempty"`
	OwnerName           string             `json:"owner_name,omitempty"`
	RootKind            string             `json:"root_kind,omitempty"`
	RootName            string             `json:"root_name,omitempty"`
	Generation          int64              `json:"generation"`
	SyncedGen           int64              `json:"synced_gen"`
	IsReady             bool               `json:"is_ready"`
	HealthOK            bool               `json:"health_ok"`
	Phase               string             `json:"phase"`
	Conditions          []conditionDTO     `json:"conditions,omitempty"`
	Finalizers          []string           `json:"finalizers,omitempty"`
	DeletionRequestedAt pgtype.Timestamptz `json:"deletion_requested_at,omitempty"`
	Labels              map[string]string  `json:"labels"`
	CreatedAt           pgtype.Timestamptz `json:"created_at"`
	UpdatedAt           pgtype.Timestamptz `json:"updated_at"`
	Spec                json.RawMessage    `json:"spec"`
	Status              json.RawMessage    `json:"status,omitempty"`
	// ManifestDrift is true when this resource is PINNED to an older kind_manifest
	// version than the kind's CURRENT applied version — i.e. a CRD was re-applied
	// after this resource last scheduled. The raw internal version counters are
	// not exposed; the drift SIGNAL is. Only the detail read populates this.
	ManifestDrift bool `json:"manifest_drift"`
	// ProviderConfig is the attached CUSTOM config (if any) — name + kind so
	// the detail page can link to its provider-configs page. Omitted when the
	// resource has no custom config (uses its kind default only).
	ProviderConfig *providerConfigRefDTO `json:"provider_config,omitempty"`
	// Work is the live work_queue claim for this resource (if any) — who
	// is reconciling it and for how long. Surfaced so a long-running
	// reconcile shows more than a flat "Reconciling": which worker/pod, how
	// long it's been running, whether its heartbeat is fresh (alive) or
	// stale (about to be reaped), and which attempt this is. Omitted when
	// the resource is settled (no queued/claimed task).
	Work *workDTO `json:"work,omitempty"`
	// FailureTerminal is true when the current-generation failure is TERMINAL —
	// the reconcile won't be retried automatically (either the provider declared it
	// terminal, or a persistently-transient failure was escalated to terminal at the
	// max_transient_attempts cap). A phase='Failed' row with this set is
	// DEAD-LETTERED: recover by editing the spec or forcing a reconcile (both bump
	// generation → eligible again, counter reset). Only meaningful for a Failed row.
	FailureTerminal bool `json:"failure_terminal"`
	// FailureAttempts is the DURABLE count of CONSECUTIVE transient reconcile
	// failures for the current generation. It climbs toward MaxTransientAttempts (the
	// kind's cap); at the cap the failure dead-letters (FailureTerminal flips true).
	// 0 = no transient failures for this generation. Surfaced so an operator sees a
	// resource "on attempt 4 of 20" approaching dead-letter vs. one already there.
	FailureAttempts int32 `json:"failure_attempts"`
	// MaxTransientAttempts is the kind's transient-failure dead-letter cap
	// (kind_config), for rendering "attempt N of CAP". nil when the kind has no
	// config or the cap is unset (0 = unbounded: transient failures retry forever).
	MaxTransientAttempts *int32 `json:"max_transient_attempts,omitempty"`
}

// workDTO is the read-only view of a resource's in-flight work_queue row,
// for the detail panel's "what's happening right now" section. All
// timing fields are also rendered as human strings server-side so the UI
// doesn't recompute them. There is at most one row per task_type; the
// detail handler surfaces the reconcile claim (the long-running one).
type workDTO struct {
	TaskType string `json:"task_type"`
	// BrokerID is the CLAIMING process (the lease holder): "<hostname>-<pid>-<uuid8>".
	// In a fanned-out deploy this is the BROKER that claimed the row, NOT the worker
	// that runs the stage — see WorkerID. NULL/empty means queued but not yet claimed.
	BrokerID string `json:"broker_id,omitempty"`
	// WorkerID is the dumb WORKER that actually runs the stage, stamped by the broker
	// at dispatch from the identity it OBSERVED for that worker's connection (its mTLS
	// client cert's SPIFFE ID, a trusted service-mesh header, or the peer IP — never a
	// value the worker self-reported). Empty until the task is handed to a worker (or on
	// an in-process run, where BrokerID already is the executor). This is the "running
	// on <worker>" the UI shows — the key to which worker's log to read.
	WorkerID string `json:"worker_id,omitempty"`
	Claimed  bool   `json:"claimed"`
	// Attempts > 1 means the task has been retried (failing+requeued), not
	// merely running long once.
	Attempts int32 `json:"attempts"`
	// ClaimedAt is when this attempt was (re)queued; Elapsed is now-then.
	ClaimedAt pgtype.Timestamptz `json:"claimed_at"`
	Elapsed   string             `json:"elapsed"`
	// HeartbeatAt is the worker's last liveness ping; HeartbeatAge is
	// now-then. A fresh heartbeat = genuinely working; an age past the
	// reaper's stale window = stuck/dead, about to be reclaimed.
	HeartbeatAt  pgtype.Timestamptz `json:"heartbeat_at"`
	HeartbeatAge string             `json:"heartbeat_age"`
	// Generation the in-flight task is reconciling (may lag the resource's
	// current generation if a newer spec edit landed mid-flight).
	Generation int64 `json:"generation"`
}

// ── objects ────────────────────────────────────────────────────────────

// applyResourceManifestInput backs the single create-or-update "Apply"
// endpoint. The request body IS a ResourceManifest ({kind, name,
// labels, spec}) — the same self-describing shape stored on disk and
// returned by the manifest download — so a downloaded file re-applies
// verbatim. The resource is keyed by (kind, name): a new pair creates,
// an existing one updates. There is no separate update-by-id input.
type applyResourceManifestInput struct {
	Body struct {
		// Type is the OPTIONAL self-describing object tag (see the applyType* enum). For
		// this endpoint it may only be "resource"; it is accepted + validated but never
		// affects routing (the URL already selects the resource endpoint). Lets a
		// directory-sync client tell a resource file apart from a providerconfig one,
		// which share kind/name/spec.
		Type string `json:"type,omitempty" enum:"resource" doc:"Optional self-describing tag; must be \"resource\" if set. Ignored for routing."`
		Kind string `json:"kind" doc:"Resource kind."`
		// KindVersion is the web-API version this apply targets (v1, v2, …). REQUIRED
		// and explicit (>= 1): no implicit v1 default. On CREATE it pins the
		// resource's kindVersion. On an existing resource, a kindVersion DIFFERENT from
		// the one it currently pins is a breaking KIND-VERSION FLIP: the spec is
		// rewritten to the new kindVersion's shape and the row's kindVersion +
		// generation bump IN PLACE (same uuid — value-flows survive, no
		// orphan-teardown). Same kindVersion = an ordinary in-place update.
		KindVersion int               `json:"kind_version" minimum:"1" maximum:"32767" doc:"Web-API version (v1, v2, …; 1–32767). REQUIRED — no implicit v1 default. A change from the resource's current kindVersion is a breaking flip."`
		Name        string            `json:"name" minLength:"1" doc:"Resource name; identity together with kind."`
		Labels      map[string]string `json:"labels,omitempty" doc:"Full label set (string→string); applied wholesale — omitting a key removes it."`
		Spec        json.RawMessage   `json:"spec" doc:"Per-kind desired-state spec."`
		// ProviderConfigRef attaches a CUSTOM config: the name of a
		// providerconfig resource whose document overrides this kind's
		// default per field. Cloned into the work queue at schedule (like
		// spec), so edits apply on the next reconcile. Omit to use only the
		// kind default. Empty clears any existing attachment.
		ProviderConfigRef string `json:"provider_config_ref,omitempty" doc:"Name of a providerconfig resource to attach as a per-resource config override."`
	}
}

// Apply-result vocabulary — the kubectl-style outcome an Apply reports in the
// X-Apply-Result header, shared by the resource and providerconfig apply handlers so
// both speak one word set. reconcileStatusReconciling is the reconcile action's
// accepted-body status. Named so the handlers don't scatter the same literals.
const (
	applyResultCreated    = "created"    // a new object was inserted (201)
	applyResultConfigured = "configured" // spec/labels changed (200)
	applyResultUnchanged  = "unchanged"  // identical re-apply, no generation bump (200)

	reconcileStatusReconciling = "reconciling" // POST …/reconcile → 202 body status
)

// Object-type discriminator vocabulary. `type` is an OPTIONAL, self-describing tag a
// client may set on any apply body to say WHICH control-plane object the document is —
// so a tool that syncs a directory of mixed manifests (conctl sync) can route each file
// by its content, with no filename convention. The API does NOT route on it: each apply
// endpoint already knows its own object type from its URL, so per body the field is
// accepted and validated to the ONE matching value (the huma `enum:"…"` tag on each
// Type field, which must stay in sync with these) but never changes behavior. It is the
// same tag `conctl apply`/`conctl sync` read to route a file client-side. The archtest
// TestApplyTypeEnumsMatch pins the tags to these constants.
const (
	applyTypeResource       = "resource"
	applyTypeManifest       = "manifest" // a kind CRD
	applyTypeProviderConfig = "providerconfig"
	applyTypeReactorBinding = "reactorbinding"
)

// applyTypeValues is the full set, referenced by the guard test that asserts each apply
// body's `type` enum tag matches its constant — so a renamed value can't silently drift
// the wire contract from what conctl sends.
var applyTypeValues = []string{
	applyTypeResource, applyTypeManifest, applyTypeProviderConfig, applyTypeReactorBinding,
}

type resourceOutput struct {
	Status int `header:"-"`
	// Apply reports the kubectl-style outcome of an Apply (manifest): one of the
	// applyResult* values (created / configured / unchanged). Surfaced as a response
	// header so the body stays a pure resource. Empty on plain reads.
	Apply string `header:"X-Apply-Result"`
	Body  resourceFull
}

type reconcileOutput struct {
	Status int `header:"-"`
	Body   struct {
		Status string `json:"status"`
	}
}

type deleteResourceOutput struct {
	Status int `header:"-"`
	Body   struct {
		Deleted bool `json:"deleted"`
	}
}

// quarantineOutput is returned by both the quarantine and unquarantine
// endpoints; Quarantined is the resulting state (true after quarantine, false
// after release).
type quarantineOutput struct {
	Status int `header:"-"`
	Body   struct {
		Quarantined bool `json:"quarantined"`
	}
}

// ResourceInfo is an EXPORTED alias of resourceInfo used only for embedding in
// rootPageItem below. Huma promotes the fields of an embedded struct into the
// parent's OpenAPI schema ONLY when the embedded field's name is exported; an
// embedded unexported `resourceInfo` would emit a schema with just
// resource_count while the JSON still carries the promoted kind/name/phase/…
// (a spec-vs-wire mismatch that breaks generated clients). Embedding the alias
// keeps the flat wire shape AND makes Huma reflect the full field set.
type ResourceInfo = resourceInfo

type rootPageItem struct {
	ResourceInfo
	ResourceCount int64 `json:"resource_count"`
}
