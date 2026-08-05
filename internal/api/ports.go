package api

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// ResourceCommands is the write/use-case boundary the HTTP server depends
// on for resource mutations (create / patch / re-pend / soft-delete).
// *store.Store satisfies it. It is scoped to just the data mutations the
// handlers need — "create a root", not a goroutine/process-lifecycle manager —
// so the API's write paths stay decoupled from the engine.
type ResourceCommands interface {
	ApplySpec(ctx context.Context, kind model.Kind, name string, spec json.RawMessage, labels map[string]string) (store.ApplyResult, error)
	// ApplySpecWithConfig is ApplySpec plus a per-resource CUSTOM config
	// attachment (the providerconfig name in provider_config_ref). Empty name
	// means "no attachment", identical to ApplySpec. (kindVersion = 1.)
	ApplySpecWithConfig(ctx context.Context, kind model.Kind, name string, spec json.RawMessage, labels map[string]string, providerConfigName string) (store.ApplyResult, error)
	// ApplySpecKindVersionWithConfig pins the web-API kindVersion on CREATE (the apply path
	// uses it so a new resource records the version it was authored against).
	ApplySpecKindVersionWithConfig(ctx context.Context, kind model.Kind, kindVersion int, name string, spec json.RawMessage, labels map[string]string, providerConfigName string) (store.ApplyResult, error)
	// ResolveResourceIDKindVersionByKindName resolves (id, current kindVersion, found) so the
	// apply path can detect a breaking kindVersion FLIP (applied kindVersion != pinned kindVersion).
	ResolveResourceIDKindVersionByKindName(ctx context.Context, kind model.Kind, name string) (uuid.UUID, int, bool, error)
	// FlipResourceKindVersion performs the uuid-stable flip: rewrite the spec to the
	// new kindVersion's shape and bump the resource's kindVersion + generation in place.
	FlipResourceKindVersion(ctx context.Context, id uuid.UUID, oldKindVersion, newKindVersion int, newSpec json.RawMessage, kind model.Kind) (store.FlipResult, error)
	Reconcile(ctx context.Context, id uuid.UUID) error
	RequestResourceDeletion(ctx context.Context, id uuid.UUID, finalizer, actor string) (bool, error)
	// QuarantineResource sets a failed/stuck resource aside (freezes it from all
	// schedulers + unblocks its root's rollup, without deleting it).
	QuarantineResource(ctx context.Context, id uuid.UUID, actor string) (bool, error)
	// UnquarantineResource clears a quarantine and re-arms the resource.
	UnquarantineResource(ctx context.Context, id uuid.UUID, actor string) (bool, error)
	// RollbackSpec checks out a historic root revision (by authored
	// generation) as the live spec, returning the resource's generation after
	// checkout (0 if the revision wasn't found).
	RollbackSpec(ctx context.Context, id uuid.UUID, generation int64, actor string) (int64, error)
}

var _ ResourceCommands = (*store.Store)(nil)

// ProviderConfigStore is the read+write boundary for the providerconfigs table
// (the runtime-editable per-kind config store). *store.Store satisfies it.
type ProviderConfigStore interface {
	UpsertProviderConfig(ctx context.Context, name string, kind model.Kind, kindVersion int, isDefault bool, spec json.RawMessage, data []byte) (store.ProviderConfig, bool, error)
	GetProviderConfig(ctx context.Context, name string) (store.ProviderConfig, bool, error)
	ListProviderConfigs(ctx context.Context, f store.ProviderConfigFilter, limit, offset int) ([]store.ProviderConfig, error)
	CountProviderConfigs(ctx context.Context, f store.ProviderConfigFilter) (int64, error)
	DeleteProviderConfig(ctx context.Context, name string) (bool, error)
}

// ReactorBindingStore is the read+write boundary for reactor_bindings — the
// runtime-editable "what reacts to what" rule table that drives the
// lifecycle-reactor spine. A binding is NOT a resource: it parameterises the
// dispatcher, which reads it fresh on every claim (no cache), so an edit takes
// effect live with no restart. *store.Store satisfies it.
type ReactorBindingStore interface {
	UpsertReactorBinding(ctx context.Context, b store.ReactorBinding) error
	ListReactorBindings(ctx context.Context) ([]store.ReactorBinding, error)
	DeleteReactorBinding(ctx context.Context, name string) (bool, error)
}

var _ ProviderConfigStore = (*store.Store)(nil)

// KindConfigStore is the read+write boundary for the kind_config table (the
// per-kind OPERATIONAL settings — concurrency cap + resync policy). The API
// joins the reads with the kind's default providerconfig so an operator sees
// both in one panel, without the two tables being fused in storage, and exposes
// an authoritative write so caps/resync are runtime-editable from the UI.
// *store.Store satisfies it.
type KindConfigStore interface {
	GetKindConfig(ctx context.Context, kind model.Kind, kindVersion int) (store.KindConfig, bool, error)
	ListKindConfig(ctx context.Context) ([]store.KindConfig, error)
	UpsertKindConfig(ctx context.Context, k store.KindConfig) error
	GetDefaultProviderConfig(ctx context.Context, kind model.Kind, kindVersion int) (store.DefaultProviderConfig, bool, error)
}

var _ KindConfigStore = (*store.Store)(nil)

// KindManifestStore is the read+write boundary for the kind_manifest table — the
// per-(kind, kindVersion) CRD definition of a kind (its JSON Schemas, declared
// reactions, finalizer + policy). An operator applies a manifest here (the two DB
// triggers derive kind_config + reactor_bindings); the reaction engine +
// KindManifestCache read it. *store.Store satisfies it.
type KindManifestStore interface {
	UpsertKindManifest(ctx context.Context, m model.KindManifest) (int64, error)
	GetKindManifest(ctx context.Context, kind model.Kind, kindVersion int) (model.KindManifest, bool, error)
	ListKindManifests(ctx context.Context) ([]model.KindManifest, error)
	// DeleteKindManifest removes one (kind, kindVersion) — refused (ErrKindVersionInUse)
	// if a resource or exact-version-pinned binding still references it; cascades the
	// version's provider configs. found=false = already gone (idempotent 404).
	DeleteKindManifest(ctx context.Context, kind model.Kind, kindVersion int) (found bool, deletedConfigs int64, err error)
}

var _ KindManifestStore = (*store.Store)(nil)

// The read surface is grouped into cohesive role interfaces (below), each the
// slice of the query layer one family of handlers uses. Repo composes them into
// the full read facade the Server holds; a handler that only needs one family
// can name that role directly. *store.Store satisfies every role. Because the
// whole query surface lives behind these interfaces, NO handler imports dbq —
// store is the sole importer of the generated query layer, so a `just sqlc`
// regen can't ripple into the HTTP layer. The row/param types in these
// signatures are store-owned aliases (see internal/store/queries.go).

// ResourceReads is the single-resource read family: identity resolution + the
// per-resource detail queries the object/history handlers call.
type ResourceReads interface {
	// Public-identity resolution: (kind, name) → internal id. Every name-keyed
	// handler calls this once at its top, then delegates to the id-based reads.
	ResolveResourceIDByKindName(ctx context.Context, kind model.Kind, name string) (uuid.UUID, error)
	ResolveResourceIDsByKindName(ctx context.Context, refs []store.ObjectRef) ([]store.ResolvedRef, error)

	// Per-resource reads. The single-resource reads are addressed by the PUBLIC
	// (kind, name) and fold the id-resolve into the query itself (one indexed
	// seek, no separate probe); GetResource returns r.id for the detail
	// handler's own follow-ups (conditions, work_queue). GetResourceInfo stays
	// id-based for the internal paths that already hold an id (apply read-back,
	// owner-cache batch); GetResourceInfoByName is its (kind,name) counterpart
	// used for the owner envelope (returns r.id, reused by the scoped query).
	GetResource(ctx context.Context, kind model.Kind, name string) (store.GetResourceRow, error)
	GetResourceInfo(ctx context.Context, id uuid.UUID) (store.GetResourceInfoRow, error)
	GetResourceInfoByName(ctx context.Context, kind model.Kind, name string) (store.GetResourceInfoByNameRow, error)
	GetResourcesInfo(ctx context.Context, ids []uuid.UUID) ([]store.GetResourcesInfoRow, error)
	GetResourceManifest(ctx context.Context, kind model.Kind, name string) (store.GetResourceManifestRow, error)
	GetResourceStatus(ctx context.Context, kind model.Kind, name string) (store.GetResourceStatusRow, error)

	// Spec revision history (roots): list revisions (bodies elided) + fetch
	// one historic body (by authored generation) for raw download / prefill.
	// Both addressed by (kind, name).
	ListSpecHistory(ctx context.Context, kind model.Kind, name string, limit int) ([]store.SpecHistoryRow, error)
	GetSpecRevision(ctx context.Context, kind model.Kind, name string, generation int64) (json.RawMessage, error)
	GetResourceDependencies(ctx context.Context, kind model.Kind, name string) ([]store.GetResourceDependenciesRow, error)
	ListResourceConditions(ctx context.Context, resourceID uuid.UUID) ([]store.ListResourceConditionsRow, error)
	GetWorkQueueByResource(ctx context.Context, resourceID uuid.UUID) ([]store.GetWorkQueueByResourceRow, error)
}

// ListReads is the owner-scoped children, page/list, and count family — the
// paginated roots/children/summary handlers.
type ListReads interface {
	GetChildrenByOwner(ctx context.Context, ownerID uuid.UUID, limit int) ([]store.GetChildrenByOwnerRow, error)
	GetDepsByOwner(ctx context.Context, ownerID uuid.UUID, limit int) ([]store.GetDepsByOwnerRow, error)
	GetResourcesChangedSince(ctx context.Context, arg store.GetResourcesChangedSinceParams) ([]store.GetResourcesChangedSinceRow, error)
	ListResourcesPage(ctx context.Context, arg store.ListResourcesPageParams) ([]store.ListResourcesPageRow, error)
	ListRootResourcesPage(ctx context.Context, arg store.ListRootResourcesPageParams) ([]store.ListRootResourcesPageRow, error)
	CountResourcesFiltered(ctx context.Context, arg store.CountResourcesFilteredParams) (int64, error)
	CountRootResourcesFiltered(ctx context.Context, arg store.CountRootResourcesFilteredParams) (int64, error)
	CountChildrenByReadiness(ctx context.Context, ownerID uuid.UUID) ([]store.CountChildrenByReadinessRow, error)
	CountResourcesByReadinessScoped(ctx context.Context, ownerIDs []uuid.UUID) ([]store.CountResourcesByReadinessScopedRow, error)
}

// TopologyReads is the group-by-label drill-down + subgraph family.
type TopologyReads interface {
	AggregateTopology(ctx context.Context, ownerID uuid.UUID, path []store.TopologyPathSegment, nextKey, cursor string, limit int) ([]store.TopologyBucket, error)
	CountTopologyBuckets(ctx context.Context, ownerID uuid.UUID, path []store.TopologyPathSegment, nextKey string) (int, error)
	TopologyLeaves(ctx context.Context, ownerID uuid.UUID, path []store.TopologyPathSegment, cursorName string, cursorID uuid.UUID, limit int) ([]store.GetChildrenByOwnerRow, error)
	TopologyLabelKeys(ctx context.Context, ownerID uuid.UUID, limit int) ([]string, error)
	SubgraphTransitive(ctx context.Context, kind model.Kind, name string, ancestorDepth, descendantDepth, limit int) ([]store.SubgraphTransitiveRow, error)
	SubgraphDeps(ctx context.Context, ids []uuid.UUID) ([]store.SubgraphDep, error)
}

// OpsRepo is the subresource-verb operations family (the only READ role that
// also writes — an operation row is created + advanced as verbs run).
type OpsRepo interface {
	CreateOperationRow(ctx context.Context, resourceID uuid.UUID, verb string, input []byte, requestedBy string) (store.ResourceOperation, error)
	GetOperation(ctx context.Context, id uuid.UUID) (store.ResourceOperation, error)
	ListOperationsByResource(ctx context.Context, resourceID uuid.UUID, states []string, limit int) ([]store.ResourceOperation, error)
	SetOperationRunning(ctx context.Context, id uuid.UUID) error
}

// EventRepo is the per-resource audit log family (emit on mutations + list).
type EventRepo interface {
	EmitEvent(ctx context.Context, e store.Event) error
	ListEvents(ctx context.Context, resourceID uuid.UUID, cursorAt time.Time, cursorID int64, limit int) ([]store.EventRow, error)
}

// ClusterReads is the running-fleet registry read (the cluster view).
type ClusterReads interface {
	ListClusterMembers(ctx context.Context) ([]store.ClusterMember, error)
}

// Repo is the composed read facade the HTTP server holds — the union of the read
// roles above. The Server carries one bound to the primary pool (read-after-write)
// and one to the read replica (UI GETs). A handler that only needs one family may
// depend on that role interface instead of the whole Repo.
type Repo interface {
	ResourceReads
	ListReads
	TopologyReads
	OpsRepo
	EventRepo
	ClusterReads
}

// Compile-time check that *store.Store satisfies the API repo
// contract. Drift in either side fails the build at this boundary
// instead of at handler call sites.
var _ Repo = (*store.Store)(nil)
