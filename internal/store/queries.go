package store

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/model"
)

// queries.go is the read/query face of the persistence boundary: the
// thin pass-through methods the API server's handlers call, plus the
// row/param types they consume, re-exported from the sqlc-generated dbq
// package as store types.
//
// This is what keeps `store` the SOLE importer of dbq. Handlers depend
// on store (via the api.Repo port) and never see dbq, so a `just sqlc`
// regen can reshape generated code without rippling into the HTTP layer.
// The aliases are zero-cost (`type X = dbq.X` is identity) so the
// projection mappers and struct literals in api compile unchanged.

// ── re-exported row / param types (identity aliases over dbq) ──────────────
type (
	GetResourceRow                     = dbq.GetResourceRow
	GetResourceInfoRow                 = dbq.GetResourceInfoRow
	GetResourceInfoByNameRow           = dbq.GetResourceInfoByNameRow
	GetResourcesInfoRow                = dbq.GetResourcesInfoRow
	GetChildrenByOwnerRow              = dbq.GetChildrenByOwnerRow
	GetDepsByOwnerRow                  = dbq.GetDepsByOwnerRow
	GetResourceDependenciesRow         = dbq.GetResourceDependenciesRow
	GetResourcesChangedSinceRow        = dbq.GetResourcesChangedSinceRow
	GetResourcesChangedSinceParams     = dbq.GetResourcesChangedSinceParams
	ListResourcesPageRow               = dbq.ListResourcesPageRow
	ListResourcesPageParams            = dbq.ListResourcesPageParams
	ListRootResourcesPageRow           = dbq.ListRootResourcesPageRow
	ListRootResourcesPageParams        = dbq.ListRootResourcesPageParams
	CountResourcesFilteredParams       = dbq.CountResourcesFilteredParams
	CountRootResourcesFilteredParams   = dbq.CountRootResourcesFilteredParams
	CountChildrenByReadinessRow        = dbq.CountChildrenByReadinessRow
	CountResourcesByReadinessScopedRow = dbq.CountResourcesByReadinessScopedRow
	ListResourceConditionsRow          = dbq.ResourceCondition
	GetWorkQueueByResourceRow          = dbq.GetWorkQueueByResourceRow
	GetResourceManifestRow             = dbq.GetResourceManifestRow
	GetResourceStatusRow               = dbq.GetResourceStatusRow
	ResourceOperation                  = dbq.ResourceOperation
	// ResourceRow is the full descendant row (resources.* + name/labels
	// joined from resource_meta, as returned by ListDescendants),
	// re-exported so the runtime's rollup projection maps it without
	// importing dbq.
	ResourceRow = dbq.ListDescendantsRow
)

// ── name → id resolution (the public-identity boundary) ────────────────────

// ObjectRef is a PUBLIC resource reference: the (kind, name) identity that is
// unique per uniq_resource_meta. It is the only handle the external surfaces
// (API/UI/conctl) use; the internal uuid never leaves the app.
type ObjectRef struct {
	Kind string
	Name string
}

// ResolveResourceIDByKindName translates the public (kind, name) identity into
// the internal resource id. Returns ErrResourceNotFound when no such resource
// exists (so the API 404s absence but 500s a real read fault — never conflates the
// two); any other error is the raw DB error. A single indexed probe on
// uniq_resource_meta.
func (s *Store) ResolveResourceIDByKindName(ctx context.Context, kind model.Kind, name string) (uuid.UUID, error) {
	id, err := s.queries().ResolveResourceIDByKindName(ctx, dbq.ResolveResourceIDByKindNameParams{
		Kind: string(kind),
		Name: name,
	})
	if err != nil {
		if isNoRows(err) {
			return uuid.UUID{}, ErrResourceNotFound
		}
		return uuid.UUID{}, err
	}
	return id, nil
}

// ResolveResourceIDKindVersionByKindName resolves (id, current kindVersion) from the public
// (kind, name) — the apply path's flip-detect probe. found=false (pgx.ErrNoRows)
// means the resource doesn't exist yet (a plain create at the applied kindVersion).
func (s *Store) ResolveResourceIDKindVersionByKindName(ctx context.Context, kind model.Kind, name string) (uuid.UUID, int, bool, error) {
	row, err := s.queries().ResolveResourceIDKindVersionByKindName(ctx, dbq.ResolveResourceIDKindVersionByKindNameParams{
		Kind: string(kind),
		Name: name,
	})
	if err != nil {
		if isNoRows(err) {
			return uuid.Nil, 0, false, nil
		}
		return uuid.Nil, 0, false, err
	}
	return row.ID, int(row.KindVersion), true, nil
}

// ResolveResourceIDsByKindName batch-resolves public refs to ids. Unresolved
// refs are simply absent from the result — the caller decides whether a missing
// owner is an error (it isn't for a cross-owner scoping filter).
func (s *Store) ResolveResourceIDsByKindName(ctx context.Context, refs []ObjectRef) ([]ResolvedRef, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	kinds := make([]string, len(refs))
	names := make([]string, len(refs))
	for i, r := range refs {
		kinds[i], names[i] = r.Kind, r.Name
	}
	rows, err := s.queries().ResolveResourceIDsByKindName(ctx, dbq.ResolveResourceIDsByKindNameParams{
		Column1: kinds,
		Column2: names,
	})
	if err != nil {
		return nil, err
	}
	out := make([]ResolvedRef, len(rows))
	for i, r := range rows {
		out[i] = ResolvedRef{ID: r.ID, Kind: r.Kind, Name: r.Name}
	}
	return out, nil
}

// ResolvedRef is one row of a batch resolution: the public identity plus its
// internal id (used only inside the app to drive the id-based reads).
type ResolvedRef struct {
	ID   uuid.UUID
	Kind string
	Name string
}

// ── per-resource reads ─────────────────────────────────────────────────────

// GetResource reads one resource by its PUBLIC (kind, name) — no separate
// id-resolve probe; the returned row carries r.id for the detail handler's own
// follow-ups (conditions, work_queue).
func (s *Store) GetResource(ctx context.Context, kind model.Kind, name string) (GetResourceRow, error) {
	row, err := s.queries().GetResource(ctx, dbq.GetResourceParams{Kind: string(kind), Name: name})
	if err != nil {
		// Map a genuine ABSENCE to the typed sentinel so the API returns 404, while a
		// transient read FAULT (conn reset, statement timeout, replica-in-recovery)
		// surfaces as a real error → 500 — never conflating the two (see resolve.go /
		// ResolveResourceIDByKindName, which enforce the same distinction).
		if isNoRows(err) {
			return GetResourceRow{}, ErrResourceNotFound
		}
		return GetResourceRow{}, err
	}
	return row, nil
}

func (s *Store) GetResourceInfo(ctx context.Context, id uuid.UUID) (GetResourceInfoRow, error) {
	return s.queries().GetResourceInfo(ctx, id)
}

// GetResourceInfoByName is GetResourceInfo addressed by (kind, name). The
// owner-scoped handlers use it for the owner envelope; it returns r.id so the
// follow-up owner-scoped query reuses it (one seek, no separate resolve).
func (s *Store) GetResourceInfoByName(ctx context.Context, kind model.Kind, name string) (GetResourceInfoByNameRow, error) {
	return s.queries().GetResourceInfoByName(ctx, dbq.GetResourceInfoByNameParams{Kind: string(kind), Name: name})
}

func (s *Store) GetResourcesInfo(ctx context.Context, ids []uuid.UUID) ([]GetResourcesInfoRow, error) {
	return s.queries().GetResourcesInfo(ctx, ids)
}

func (s *Store) GetResourceManifest(ctx context.Context, kind model.Kind, name string) (GetResourceManifestRow, error) {
	return s.queries().GetResourceManifest(ctx, dbq.GetResourceManifestParams{Kind: string(kind), Name: name})
}

func (s *Store) GetResourceStatus(ctx context.Context, kind model.Kind, name string) (GetResourceStatusRow, error) {
	return s.queries().GetResourceStatus(ctx, dbq.GetResourceStatusParams{Kind: string(kind), Name: name})
}

func (s *Store) GetResourceDependencies(ctx context.Context, kind model.Kind, name string) ([]GetResourceDependenciesRow, error) {
	return s.queries().GetResourceDependencies(ctx, dbq.GetResourceDependenciesParams{Kind: string(kind), Name: name})
}

func (s *Store) ListResourceConditions(ctx context.Context, resourceID uuid.UUID) ([]ListResourceConditionsRow, error) {
	return s.queries().ListResourceConditions(ctx, resourceID)
}

// ── children / owner-scoped reads ──────────────────────────────────────────

// GetChildrenByOwner returns up to `limit` children of an owner (LIMIT-capped so
// a huge owner can't stream an unbounded response — use ListResourcesPage to walk
// large sets).
func (s *Store) GetChildrenByOwner(ctx context.Context, ownerID uuid.UUID, limit int) ([]GetChildrenByOwnerRow, error) {
	return s.queries().GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{
		OwnerID: toUUID(ownerID),
		Limit:   int32(limit),
	})
}

// GetWorkQueueByResource returns the in-flight/pending work_queue rows for
// one resource (read-only; for the API's "who is working this" detail
// view). Returns 0..few rows (at most one per task_type).
func (s *Store) GetWorkQueueByResource(ctx context.Context, resourceID uuid.UUID) ([]GetWorkQueueByResourceRow, error) {
	return s.queries().GetWorkQueueByResource(ctx, resourceID)
}

// GetDepsByOwner returns up to `limit` dependency edges under an owner (LIMIT-
// capped for the same reason as GetChildrenByOwner — the graph view must not
// stream an unbounded edge set).
func (s *Store) GetDepsByOwner(ctx context.Context, ownerID uuid.UUID, limit int) ([]GetDepsByOwnerRow, error) {
	return s.queries().GetDepsByOwner(ctx, dbq.GetDepsByOwnerParams{
		OwnerID: toUUID(ownerID),
		Limit:   int32(limit),
	})
}

func (s *Store) GetResourcesChangedSince(ctx context.Context, arg GetResourcesChangedSinceParams) ([]GetResourcesChangedSinceRow, error) {
	return s.queries().GetResourcesChangedSince(ctx, arg)
}

// ── list / page / count ────────────────────────────────────────────────────

func (s *Store) ListResourcesPage(ctx context.Context, arg ListResourcesPageParams) ([]ListResourcesPageRow, error) {
	return s.queries().ListResourcesPage(ctx, arg)
}

func (s *Store) ListRootResourcesPage(ctx context.Context, arg ListRootResourcesPageParams) ([]ListRootResourcesPageRow, error) {
	return s.queries().ListRootResourcesPage(ctx, arg)
}

func (s *Store) CountResourcesFiltered(ctx context.Context, arg CountResourcesFilteredParams) (int64, error) {
	return s.queries().CountResourcesFiltered(ctx, arg)
}

func (s *Store) CountRootResourcesFiltered(ctx context.Context, arg CountRootResourcesFilteredParams) (int64, error) {
	return s.queries().CountRootResourcesFiltered(ctx, arg)
}

func (s *Store) CountChildrenByReadiness(ctx context.Context, ownerID uuid.UUID) ([]CountChildrenByReadinessRow, error) {
	return s.queries().CountChildrenByReadiness(ctx, toUUID(ownerID))
}

func (s *Store) CountResourcesByReadinessScoped(ctx context.Context, ownerIDs []uuid.UUID) ([]CountResourcesByReadinessScopedRow, error) {
	return s.queries().CountResourcesByReadinessScoped(ctx, ownerIDs)
}

// ── operations (subresource verbs) ─────────────────────────────────────────

// CreateOperationRow creates a resource_operations row AND enqueues the
// matching 'operate' work_queue task, returning the full new row (the
// API needs it for its response body). Distinct from CreateOperation,
// which returns only the id for internal callers.
//
// Both writes run in ONE transaction: the operation record and its backing
// work_queue task commit atomically, so there is no window where the operation
// row exists with no task (or vice versa). If the enqueue is a no-op — the
// resource is frozen (0 selected rows) or an operate task is already queued
// (ON CONFLICT) — the whole tx rolls back and ErrOperationNotEnqueued is
// returned, rather than leaving a durable 'pending' operation with nothing to
// run it. This keeps the operate path at-least-once: a recorded operation
// always has a task behind it.
func (s *Store) CreateOperationRow(ctx context.Context, resourceID uuid.UUID, verb string, input []byte, requestedBy string) (ResourceOperation, error) {
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return ResourceOperation{}, fmt.Errorf("store: underlying DBTX cannot begin a transaction")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return ResourceOperation{}, fmt.Errorf("begin operation tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after Commit

	q := s.WithTx(tx).queries()

	op, err := q.CreateOperation(ctx, dbq.CreateOperationParams{
		ResourceID:  resourceID,
		Verb:        verb,
		Input:       input,
		RequestedBy: nilIfEmpty(requestedBy),
	})
	if err != nil {
		return ResourceOperation{}, fmt.Errorf("create operation row: %w", err)
	}
	enqueued, err := q.EnqueueOperationWork(ctx, dbq.EnqueueOperationWorkParams{
		ResourceID: resourceID,
		OpID:       toUUID(op.ID),
	})
	if err != nil {
		return ResourceOperation{}, fmt.Errorf("enqueue operation work: %w", err)
	}
	// Zero rows enqueued = the resource is frozen or an operate task is already
	// queued. Fail closed (roll back the operation row) instead of returning a
	// success with no backing task.
	if enqueued == 0 {
		return ResourceOperation{}, ErrOperationNotEnqueued
	}
	if err := tx.Commit(ctx); err != nil {
		return ResourceOperation{}, fmt.Errorf("commit operation tx: %w", err)
	}
	return op, nil
}

// ListOperationsByResource lists a resource's operations, optionally
// filtered by state, newest first.
func (s *Store) ListOperationsByResource(ctx context.Context, resourceID uuid.UUID, states []string, limit int) ([]ResourceOperation, error) {
	return s.queries().ListOperationsByResource(ctx, dbq.ListOperationsByResourceParams{
		ResourceID: resourceID,
		States:     states,
		Lim:        int32(limit),
	})
}
