package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/model"
)

// ─────────────────────────────────────────────────────────────────────────
// Composer bulk operations: child upsert + dep-edge insert.
// ─────────────────────────────────────────────────────────────────────────

// ChildDecl is one row's worth of "what we want this child resource to
// be" inside its parent's compose tx. ID is the existing child's id when
// this decl updates a row the composer already produced (the diff in
// ApplyComposeResult resolves it from ListExistingChildren); it is the
// zero UUID for a brand-new child, which BatchUpsertChildren inserts with
// a freshly generated id. Splitting insert-vs-update in Go keeps each DB
// side a clean id-keyed bulk statement — no (kind,name) join on write,
// and the hot resources row never carries name/labels (those go to the
// resource_meta side table).
type ChildDecl struct {
	ID   uuid.UUID
	Kind model.Kind
	// KindVersion is the web-API version to stamp on the child (from
	// ChildSpec.KindVersion). REQUIRED and explicit (>= 1): BatchUpsertChildren and
	// ApplyComposeResult reject a 0, never coerce it to v1. On a NEW child it is the
	// insert value; on an UPDATE of an adopted child it flips the row's kindVersion in
	// place (the composer-driven flip), so a v2 composer upgrades a v1 child on the
	// same uuid.
	KindVersion int
	Name        string
	Spec        json.RawMessage
	Labels      json.RawMessage
}

// DepDecl is one (dependent → dependency) edge plus optional value
// flow metadata. ValueFlows is the JSON-encoded array (each element
// {"dep_field":"/x","src_field":"/y"}).
type DepDecl struct {
	DependentID  uuid.UUID
	DependencyID uuid.UUID
	ValueFlows   json.RawMessage
}

// ResolvedResource is the (id, kind, name) tuple BatchUpsertChildren
// returns so the caller can resolve refs to ids for dep upsert.
type ResolvedResource struct {
	ID   uuid.UUID
	Kind model.Kind
	Name string
}

// BatchUpsertChildren writes the children of a single parent resource.
// Each child is a resources row (id, owner, root, kind, spec) + a
// resource_meta row (id, kind, name, labels). Decls with a zero ID are
// NEW (insert both, fresh id); decls with an ID are EXISTING (update
// spec + labels by id). Returns the (id, kind, name) for every decl so
// the caller resolves refs to ids without a (kind,name) join.
func (s *Store) BatchUpsertChildren(ctx context.Context, ownerID, rootID uuid.UUID, decls []ChildDecl) ([]ResolvedResource, error) {
	if len(decls) == 0 {
		return nil, nil
	}

	labelOf := func(d ChildDecl) []byte {
		if len(d.Labels) == 0 {
			return []byte("{}")
		}
		return d.Labels
	}

	// Partition into new (no id) and existing (id set). Assign ids to new
	// rows here so the meta insert can reference them and the caller gets
	// them back without a lookup. Children carry spec inline and are not
	// versioned, so no spec-version bookkeeping here.
	var (
		newIDs              []uuid.UUID
		nKinds, nNames      []string
		newSpecs, newLabels [][]byte
		newKindVersions     []int32
		updIDs              []uuid.UUID
		updSpecs, updLabels [][]byte
		updKindVersions     []int32
		out                 = make([]ResolvedResource, 0, len(decls))
	)
	for _, d := range decls {
		id := d.ID
		// kind_version is REQUIRED and explicit (>= 1) on every emitted child: a
		// composer MUST stamp the version each child is applied at — there is no
		// implicit v1 default. A 0/unset child version is a composer bug, rejected
		// here (before the tx) rather than silently written as v1.
		if d.KindVersion < 1 {
			return nil, fmt.Errorf("compose child %s/%s: kind_version is required and must be >= 1 (got %d)", d.Kind, d.Name, d.KindVersion)
		}
		kindVersion := int32(d.KindVersion)
		if id == uuid.Nil {
			id = uuid.New()
			newIDs = append(newIDs, id)
			nKinds = append(nKinds, string(d.Kind))
			nNames = append(nNames, d.Name)
			newSpecs = append(newSpecs, d.Spec)
			newLabels = append(newLabels, labelOf(d))
			newKindVersions = append(newKindVersions, kindVersion)
		} else {
			updIDs = append(updIDs, id)
			updSpecs = append(updSpecs, d.Spec)
			updLabels = append(updLabels, labelOf(d))
			updKindVersions = append(updKindVersions, kindVersion)
		}
		out = append(out, ResolvedResource{ID: id, Kind: d.Kind, Name: d.Name})
	}

	// INSERT new: hot rows first (resource_meta FKs resources.id), then meta.
	if len(newIDs) > 0 {
		if _, err := s.db.Exec(ctx, dbq.InsertChildrenHotSQL, newIDs, ownerID, rootID, nKinds, newSpecs, newKindVersions); err != nil {
			return nil, fmt.Errorf("insert children (hot): %w", err)
		}
		if _, err := s.db.Exec(ctx, dbq.InsertChildrenMetaSQL, newIDs, nKinds, nNames, newLabels); err != nil {
			return nil, fmt.Errorf("insert children (meta): %w", err)
		}
	}
	// UPDATE existing: spec + kindVersion inline on the hot row (the composer-driven
	// flip when kindVersion changes), labels on the meta row.
	if len(updIDs) > 0 {
		if _, err := s.db.Exec(ctx, dbq.UpdateChildrenHotSQL, updIDs, updSpecs, updKindVersions); err != nil {
			return nil, fmt.Errorf("update children (hot): %w", err)
		}
		if _, err := s.db.Exec(ctx, dbq.UpdateChildrenMetaSQL, updIDs, updLabels); err != nil {
			return nil, fmt.Errorf("update children (meta): %w", err)
		}
	}
	return out, nil
}

// BatchUpsertResourceDeps sorts edges by (dependent_id, dependency_id)
// and runs the multi-arg unnest insert.
func (s *Store) BatchUpsertResourceDeps(ctx context.Context, deps []DepDecl) error {
	if len(deps) == 0 {
		return nil
	}

	sorted := make([]DepDecl, len(deps))
	copy(sorted, deps)
	sort.Slice(sorted, func(i, j int) bool {
		if c := bytes.Compare(sorted[i].DependentID[:], sorted[j].DependentID[:]); c != 0 {
			return c < 0
		}
		return bytes.Compare(sorted[i].DependencyID[:], sorted[j].DependencyID[:]) < 0
	})

	dependents := make([]uuid.UUID, len(sorted))
	dependencies := make([]uuid.UUID, len(sorted))
	flows := make([][]byte, len(sorted))
	for i, e := range sorted {
		dependents[i] = e.DependentID
		dependencies[i] = e.DependencyID
		if len(e.ValueFlows) > 0 {
			flows[i] = e.ValueFlows
		} else {
			flows[i] = []byte(`[]`)
		}
	}

	_, err := s.db.Exec(ctx, dbq.BatchUpsertResourceDepsSQL, dependents, dependencies, flows)
	return err
}

// ListExistingDepsByOwner returns every edge whose dependent is a child
// of ownerID, with its current value_flows — the set the composer diffs
// the freshly-produced edges against (to prune stale ones and upsert
// changed flows).
func (s *Store) ListExistingDepsByOwner(ctx context.Context, ownerID uuid.UUID) ([]DepDecl, error) {
	rows, err := s.db.Query(ctx, dbq.ListExistingDepsByOwnerSQL, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DepDecl
	for rows.Next() {
		var d DepDecl
		if err := rows.Scan(&d.DependentID, &d.DependencyID, &d.ValueFlows); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// DeleteResourceDeps prunes edges by (dependent, dependency) pair — the
// edge analogue of DeleteChildren. The two slices are parallel.
func (s *Store) DeleteResourceDeps(ctx context.Context, dependents, dependencies []uuid.UUID) error {
	if len(dependents) == 0 {
		return nil
	}
	_, err := s.db.Exec(ctx, dbq.DeleteResourceDepsSQL, dependents, dependencies)
	return err
}

// DeleteChildren hard-deletes child rows by id.
func (s *Store) DeleteChildren(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.db.Exec(ctx, dbq.DeleteChildrenSQL, ids)
	return err
}

// MarkChildrenOrphaned stamps the orphan-grace mark (frozen_until = now() +
// per-id grace) on children the latest compose dropped, for kinds with a grace
// window — instead of deleting them. ids and graceSecs are PARALLEL arrays
// (graceSecs[i] is ids[i]'s window). ids MUST be sorted ascending by the caller
// (ascending-id lock discipline). A child already in the soft-delete drain or
// already quarantined ('infinity') is left untouched (the SQL's
// deletion_requested_at IS NULL + frozen_until IS DISTINCT FROM 'infinity'
// guards). No-op for an empty slice.
func (s *Store) MarkChildrenOrphaned(ctx context.Context, ids []uuid.UUID, graceSecs []int32) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.db.Exec(ctx, dbq.MarkChildrenOrphanedSQL, ids, graceSecs)
	return err
}

// ClearOrphanMarks cancels the orphan-grace mark on re-adopted children (a
// compose re-emitted a previously-dropped child), returning how many rows were
// actually cleared (for counts.Readopted). Gated `frozen_until IS NOT NULL AND
// <> 'infinity'`, so it is a cheap no-op for never-orphaned children and never
// un-quarantines. Does NOT bump generation. ids should be sorted ascending by
// the caller. Returns 0 for an empty slice.
func (s *Store) ClearOrphanMarks(ctx context.Context, ids []uuid.UUID) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	tag, err := s.db.Exec(ctx, dbq.ClearOrphanMarksSQL, ids)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

// ProviderConfigDecl is one provider config a Composer emits, the right-hand
// side of ApplyComposeResult's config diff. Spec is the marshaled document.
type ProviderConfigDecl struct {
	Name        string
	Kind        model.Kind
	KindVersion int
	IsDefault   bool
	Spec        json.RawMessage
}

// ExistingProviderConfig is one config a composer root already owns — the
// left-hand side of the config diff (loaded by ListProviderConfigsByOwner).
type ExistingProviderConfig struct {
	ID          uuid.UUID
	Name        string
	Kind        model.Kind
	KindVersion int
	IsDefault   bool
	Spec        json.RawMessage
}

// ListProviderConfigsByOwner returns every config owned by ownerID — the set
// ApplyComposeResult diffs a fresh compose's emitted configs against.
func (s *Store) ListProviderConfigsByOwner(ctx context.Context, ownerID uuid.UUID) ([]ExistingProviderConfig, error) {
	rows, err := s.queries().ListProviderConfigsByOwner(ctx, toUUID(ownerID))
	if err != nil {
		return nil, err
	}
	out := make([]ExistingProviderConfig, len(rows))
	for i, r := range rows {
		out[i] = ExistingProviderConfig{
			ID:          r.ID,
			Name:        r.Name,
			Kind:        model.Kind(r.Kind),
			KindVersion: int(r.KindVersion),
			IsDefault:   r.IsDefault,
			Spec:        r.Spec,
		}
	}
	return out, nil
}

// BatchUpsertProviderConfigs bulk-upserts the configs a single composer root
// (ownerID) emits, owner-guarded so it can't clobber another owner's or a
// user/API config (see BatchUpsertProviderConfigsSQL). Caller passes only the
// changed/new delta.
func (s *Store) BatchUpsertProviderConfigs(ctx context.Context, ownerID uuid.UUID, decls []ProviderConfigDecl) error {
	if len(decls) == 0 {
		return nil
	}
	names := make([]string, len(decls))
	kinds := make([]string, len(decls))
	kindVersions := make([]int32, len(decls))
	defaults := make([]bool, len(decls))
	specs := make([][]byte, len(decls))
	for i, d := range decls {
		// kind_version is REQUIRED and explicit (>= 1) on every emitted config: a
		// composer MUST name the (kind, kind_version) each config is for — no implicit
		// v1 default (a 0 is a composer bug, rejected before the tx).
		if d.KindVersion < 1 {
			return fmt.Errorf("compose provider config %q for %s: kind_version is required and must be >= 1 (got %d)", d.Name, d.Kind, d.KindVersion)
		}
		names[i] = d.Name
		kinds[i] = string(d.Kind)
		kindVersions[i] = int32(d.KindVersion)
		defaults[i] = d.IsDefault
		if len(d.Spec) > 0 {
			specs[i] = d.Spec
		} else {
			specs[i] = []byte(`{}`)
		}
	}
	_, err := s.db.Exec(ctx, dbq.BatchUpsertProviderConfigsSQL, ownerID, names, kinds, defaults, specs, kindVersions)
	return err
}

// DeleteProviderConfigs hard-deletes config rows by id — the config analogue of
// DeleteChildren, used by the composer to prune configs it no longer emits.
func (s *Store) DeleteProviderConfigs(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.db.Exec(ctx, dbq.DeleteProviderConfigsSQL, ids)
	return err
}

// AnalyzeComposeTables refreshes planner stats on resources and
// resource_deps after a large compose commit.
func (s *Store) AnalyzeComposeTables(ctx context.Context) error {
	return s.queries().AnalyzeComposeTables(ctx)
}

// ApplyValueFlowsForDependents bootstraps value-flow substitution on
// the given freshly-composed dependents.
func (s *Store) ApplyValueFlowsForDependents(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	return s.queries().ApplyValueFlowsForDependents(ctx, ids)
}

// GetComposedGen returns the resource's last composer high-water mark.
// Worker uses it to skip re-running the composer on a re-pend that's
// only waiting on descendants.
func (s *Store) GetComposedGen(ctx context.Context, id uuid.UUID) (int64, error) {
	return s.queries().GetComposedGen(ctx, id)
}

// ScheduleEligible asks the SQL scheduler to insert work_queue rows for
// any of the given candidate resource ids whose dependencies are
// satisfied. Returns the count actually inserted.
func (s *Store) ScheduleEligible(ctx context.Context, ids []uuid.UUID) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	n, err := s.queries().ScheduleEligible(ctx, ids)
	return int(n), err
}
