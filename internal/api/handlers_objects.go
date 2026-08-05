package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// Handlers for the /api/resources collection and /api/resources/{id}.
// Every entity is a row in `resources`, so these handlers differ only
// in which subset they list and which fields they project.

// applyResourceManifest is the single create-or-update entry point
// ("Apply"). The request body is a ResourceManifest ({kind, name,
// labels, spec}), so a downloaded manifest re-applies as-is. It upserts
// a resource keyed by the global (kind, name): a new (kind,name) creates
// a root (201), an existing one — root OR owned child — has its
// spec+labels overwritten in place (200) without reparenting. Generation
// is bumped only on a genuine spec change (the UpsertResource CTE bumps
// it inline), so re-applying an identical spec is a no-op. There is no
// separate create vs. update endpoint.
func (s *Server) applyResourceManifest(ctx context.Context, in *applyResourceManifestInput) (*resourceOutput, error) {
	kind := model.Kind(in.Body.Kind)
	// A REACTOR kind owns no resource — it's wired by a kind manifest + providerconfig
	// + reactor_binding, never applied as a resource. Reject it here: without this a
	// reactor's providerconfig body (e.g. statussink's {endpoint,prefix}) mis-applied
	// with --type resource sails through validateSpec (a reactor declares no resource
	// spec_schema, so validateSpec no-ops) and silently creates a junk resource row.
	if s.isReactorKind(kind) {
		return nil, huma.Error422UnprocessableEntity("kind " + in.Body.Kind + " is a REACTOR (owns no resource); wire it with a providerconfig + reactor_binding, not a resource apply")
	}
	// kind_version is REQUIRED and explicit (>= 1) — there is no implicit v1 default.
	// The body field's `minimum:"1"` tag rejects a missing/0 value at the edge (422)
	// before this handler runs (an omitted int defaults to 0, which minimum:"1" still
	// rejects), so no manual guard is needed here.
	kindVersion := in.Body.KindVersion
	// Validate against the APPLIED kindVersion's schema (per-(kind, kindVersion) validator), so
	// a v2 apply is checked against v2's spec_schema at the API edge.
	if err := s.schema().validateSpec(kind, kindVersion, in.Body.Spec); err != nil {
		return nil, err
	}
	// FLIP DETECTION: if this (kind, name) already exists at a DIFFERENT
	// kindVersion, the apply is a breaking kindVersion flip — rewrite the spec + bump the
	// kindVersion/generation IN PLACE (same uuid; value-flows survive, no
	// orphan-teardown), NOT an ordinary in-place update. A resource at the same
	// kindVersion (or a brand-new one) takes the normal apply path below.
	id, curKindVersion, found, rerr := s.commands.ResolveResourceIDKindVersionByKindName(ctx, kind, in.Body.Name)
	// FAIL CLOSED on a genuine resolve error (found=false already folds in
	// pgx.ErrNoRows = "doesn't exist yet"). Never swallow it: a transient DB error
	// here must not silently make isCreate/isFlip false and route a version FLIP down
	// the same-version update path (which keeps the row's pinned kindVersion but
	// rewrites its spec) — that would write a v2-shaped spec onto a v1-pinned row and
	// bypass the retirement gate. Surface it so the caller retries the whole apply.
	if rerr != nil {
		return nil, internalError(ctx, rerr)
	}
	// RETIREMENT GATE (freeze-new / drain-existing): reject an apply that would put
	// a resource ONTO a retired version — a CREATE (!found) at a retired version, or
	// a FLIP (found at a different version) whose TARGET version is retired. An
	// in-place update of a resource already on a version is NOT blocked (existing
	// resources keep reconciling so they can drain), and a flip OFF a retired version
	// ONTO a live one is allowed (that is the migration path). The check is one cheap
	// PK read off the low-rate apply path, never the 1M hot path.
	isCreate := !found
	isFlip := found && curKindVersion != kindVersion
	if isCreate || isFlip {
		if retired, cerr := s.kindVersionRetired(ctx, kind, kindVersion); cerr == nil && retired {
			return nil, huma.Error422UnprocessableEntity(
				"kind " + in.Body.Kind + " version is retired: new resources cannot be created on it (existing resources keep reconciling; migrate onto a live version)")
		}
	}
	if isFlip {
		fr, ferr := s.commands.FlipResourceKindVersion(ctx, id, curKindVersion, kindVersion, in.Body.Spec, kind)
		if ferr != nil {
			if errors.Is(ferr, store.ErrInvalidSpec) {
				// Well-formed body, schema-invalid content → 422 (huma's convention:
				// 400 is for unparseable/mis-typed input, 422 for a parsed body that
				// violates the rules).
				return nil, huma.Error422UnprocessableEntity("new-kindVersion spec failed schema validation", ferr)
			}
			return nil, internalError(ctx, ferr)
		}
		if !fr.Flipped {
			// A concurrent flip already moved it past curKindVersion — treat as an
			// idempotent no-op and fall through to read back the current state.
			_ = fr
		}
		r, err := s.repo.GetResourceInfo(ctx, id)
		if err != nil {
			return nil, internalError(ctx, err)
		}
		return &resourceOutput{Status: http.StatusOK, Apply: applyResultConfigured, Body: rowToFull(fromGetResourceInfoRow(r))}, nil
	}
	res, err := s.commands.ApplySpecKindVersionWithConfig(ctx, kind, kindVersion, in.Body.Name, in.Body.Spec, in.Body.Labels, in.Body.ProviderConfigRef)
	if err != nil {
		// A concurrent apply to the same (kind,name) that lost a unique/PK
		// race even after one retry → 409, not a leaked Postgres 500.
		if errors.Is(err, store.ErrConflict) {
			return nil, huma.Error409Conflict("concurrent apply to the same resource; retry")
		}
		// A provider_config_ref naming a config that doesn't exist is a client
		// error, not a server fault — surface it as 422 so the caller fixes the
		// reference rather than retrying a doomed apply.
		if errors.Is(err, store.ErrConfigNotFound) {
			return nil, huma.Error422UnprocessableEntity("provider_config_ref names a providerconfig that does not exist: " + in.Body.ProviderConfigRef)
		}
		// A provider_config_ref to a config for a DIFFERENT (kind, kindVersion) than the
		// resource's is a client error — a resource may only attach a config on its own
		// (kind, version). Surface the store's descriptive message as 422 (well-formed
		// body, invalid reference), not a leaked 500.
		if errors.Is(err, store.ErrConfigKindMismatch) {
			return nil, huma.Error422UnprocessableEntity(err.Error())
		}
		// The store's data-gateway validator rejected the spec (schema violation).
		// The pre-check above normally catches this first with field-level detail,
		// but map it here too so a spec that only the gateway rejects surfaces as a
		// client 422 (well-formed body, schema-invalid content), never a leaked 500.
		if errors.Is(err, store.ErrInvalidSpec) {
			return nil, huma.Error422UnprocessableEntity("spec failed schema validation", err)
		}
		return nil, internalError(ctx, err)
	}
	// Read back the LIGHTWEIGHT info row, not GetResource: the apply response
	// only needs identity + state, and GetResource's pg_column_size(spec)
	// would read+decompress the just-written body (multi-MB for large specs)
	// purely to elide it. The client re-fetches the full spec on detail open.
	r, err := s.repo.GetResourceInfo(ctx, res.ID)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	// Map the upsert outcome to the kubectl-apply vocabulary: created /
	// configured (spec or labels changed) / unchanged (identical
	// re-apply, no generation bump). An unchanged apply emits no event —
	// nothing happened.
	applyResult := applyResultUnchanged
	status := http.StatusOK
	switch {
	case res.Created:
		applyResult, status = applyResultCreated, http.StatusCreated
	case res.Changed:
		applyResult = applyResultConfigured
	}
	if res.Changed {
		_ = s.repo.EmitEvent(ctx, store.Event{
			ResourceID: res.ID,
			Type:       store.EventSpecChanged,
			Actor:      "api",
			// res.Generation is the generation THIS apply authored (returned
			// atomically by the upsert). Using it instead of the re-read row's
			// generation avoids mislabeling when a concurrent apply/reconcile
			// commits between the write and the GetResourceInfo read-back.
			Detail: map[string]any{"generation": res.Generation},
		})
	}
	row := fromGetResourceInfoRow(r)
	// Echo the generation this apply authored (same reason as the event).
	row.Generation = res.Generation
	return &resourceOutput{Status: status, Apply: applyResult, Body: rowToFull(row)}, nil
}

// kindVersionRetired reports whether (kind, kindVersion) is retired — the
// freeze-new gate the apply/flip path consults before creating/flipping a
// resource ONTO a version. kindVersion is the caller's explicit version (>= 1; the
// apply/binding handlers reject a missing/0 before calling this). A missing
// kind_config row (a version with no operational config yet) is NOT retired
// (false): retirement is an explicit opt-in. One cheap PK read off the low-rate
// apply path. An error is returned so the caller can fail-open (do not block a
// create on a transient read error).
func (s *Server) kindVersionRetired(ctx context.Context, kind model.Kind, kindVersion int) (bool, error) {
	if s.kindConfigs == nil {
		return false, nil
	}
	kc, found, err := s.kindConfigs.GetKindConfig(ctx, kind, kindVersion)
	if err != nil || !found {
		return false, err
	}
	return kc.Retired, nil
}

func (s *Server) getResource(ctx context.Context, in *kindNamePath) (*resourceOutput, error) {
	// Single query by (kind, name) — no separate id-resolve probe. The row
	// carries the internal id, reused below for the two id-scoped follow-ups.
	r, err := s.readRepo.GetResource(ctx, model.Kind(in.Kind), in.Name)
	if err != nil {
		// Distinguish a genuinely absent resource (404) from a transient read
		// fault (500) — conflating them hid DB outages behind "not found", so a
		// blip made a live resource read as deleted. The store maps ErrNoRows to
		// ErrResourceNotFound; anything else is a real fault, wrapped + logged.
		if errors.Is(err, store.ErrResourceNotFound) {
			return nil, huma.Error404NotFound("resource not found")
		}
		return nil, internalError(ctx, err)
	}
	id := r.ID
	row := fromGetResourceRow(r)
	// Detail view: fold any stored Ready/custom condition rows with the
	// synthesized Synced/Ready axes. Best-effort — a condition read
	// failure shouldn't 500 the resource fetch.
	if conds, cErr := s.readRepo.ListResourceConditions(ctx, id); cErr == nil {
		row.Conditions = storedConditionsToDTO(conds)
	}
	full := rowToFull(row)
	// Surface the live work_queue claim (who's reconciling it, for how
	// long, heartbeat freshness, attempt #) so a long-running reconcile
	// isn't a flat "Reconciling". Best-effort and read-only; absent when
	// the resource is settled (no queued/claimed task).
	//
	// Read from the PRIMARY (s.repo), NOT the replica: work_queue is an
	// UNLOGGED table, so it is NEVER present on a streaming replica (a replica
	// even errors trying to read an unlogged relation during recovery). A
	// replica read therefore ALWAYS returns no rows → work would be null for
	// every resource. This is why the panel showed no Queued/Working card
	// (worker id + heartbeat) for a composer root's whole compose/rollup on a
	// replica-backed API. It is a single shard-pruned indexed point-read, off
	// any hot path, so the primary hit is negligible. When no replica is
	// configured s.repo == s.readRepo, so this is a no-op there.
	if wq, wErr := s.repo.GetWorkQueueByResource(ctx, id); wErr == nil {
		full.Work = workFromRows(wq)
	}
	return &resourceOutput{Status: http.StatusOK, Body: full}, nil
}

func (s *Server) reconcileResource(ctx context.Context, in *kindNamePath) (*reconcileOutput, error) {
	id, err := s.resolveID(ctx, in.Kind, in.Name)
	if err != nil {
		return nil, err
	}
	if err := s.commands.Reconcile(ctx, id); err != nil {
		return nil, internalError(ctx, err)
	}
	out := &reconcileOutput{Status: http.StatusAccepted}
	out.Body.Status = reconcileStatusReconciling
	return out, nil
}

// deleteResource is the K8s-style soft-delete entry point. The SQL
// helper request_resource_deletion stamps deletion_requested_at, sets
// the Deleting=True condition, and either hard-deletes the row (no
// Deleter registered for the kind) or schedules the finalizer drain.
// Returns 200 with {deleted:true} when the row existed, 404 otherwise.
func (s *Server) deleteResource(ctx context.Context, in *kindNamePath) (*deleteResourceOutput, error) {
	id, err := s.resolveID(ctx, in.Kind, in.Name)
	if err != nil {
		return nil, err
	}
	// The kind is right in the path, so pick the registered FinalizerName from it
	// directly (no extra info read).
	finalizer := ""
	if ks, ok := s.schemaForKind(model.Kind(in.Kind)); ok {
		finalizer = ks.FinalizerName
	}
	deleted, err := s.commands.RequestResourceDeletion(ctx, id, finalizer, "api")
	if err != nil {
		return nil, internalError(ctx, err)
	}
	if !deleted {
		return nil, huma.Error404NotFound("not found")
	}
	_ = s.repo.EmitEvent(ctx, store.Event{
		ResourceID: id,
		Type:       store.EventSpecChanged,
		Actor:      "api",
		Reason:     "deletion_requested",
	})
	out := &deleteResourceOutput{Status: http.StatusOK}
	out.Body.Deleted = true
	return out, nil
}

// quarantineResource sets a failed/stuck resource aside: freezes it from every
// scheduler (no retry/resync/cascade resurrection) and excludes it from its
// root's rollup, WITHOUT deleting it. The operator escape hatch for "one child
// keeps failing and is blocking the BOM — set it aside so the rest rolls up."
// 200 {quarantined:true} when it took effect, 404 if missing / already
// deleting / already quarantined.
func (s *Server) quarantineResource(ctx context.Context, in *kindNamePath) (*quarantineOutput, error) {
	id, err := s.resolveID(ctx, in.Kind, in.Name)
	if err != nil {
		return nil, err
	}
	ok, err := s.commands.QuarantineResource(ctx, id, "api")
	if err != nil {
		return nil, internalError(ctx, err)
	}
	if !ok {
		return nil, huma.Error404NotFound("not found, already deleting, or already quarantined")
	}
	out := &quarantineOutput{Status: http.StatusOK}
	out.Body.Quarantined = true
	return out, nil
}

// unquarantineResource clears a quarantine and re-arms the resource (the SQL
// re-runs schedule_eligible so a still-lagging row re-pends). 200
// {quarantined:false} on success, 404 if the row wasn't quarantined.
func (s *Server) unquarantineResource(ctx context.Context, in *kindNamePath) (*quarantineOutput, error) {
	id, err := s.resolveID(ctx, in.Kind, in.Name)
	if err != nil {
		return nil, err
	}
	ok, err := s.commands.UnquarantineResource(ctx, id, "api")
	if err != nil {
		return nil, internalError(ctx, err)
	}
	if !ok {
		return nil, huma.Error404NotFound("not found or not quarantined")
	}
	out := &quarantineOutput{Status: http.StatusOK}
	out.Body.Quarantined = false
	return out, nil
}

// listResourceDependencies returns each upstream this resource depends
// on, with the value flows declared on the dependency edge. The UI's
// "blocked on" panel uses this: upstream_ready=false means the dependent
// is still waiting on that upstream. Value flows are always returned so
// the UI can render the full wired data path — they carry no per-flow
// state, and the drain_outbox_batch substitute pass keeps
// re-applying those patches on every upstream status change.
func (s *Server) listResourceDependencies(ctx context.Context, in *kindNamePath) (*listDependenciesOutput, error) {
	// Single query by (kind, name) — the dependent is resolved inside the query.
	rows, err := s.readRepo.GetResourceDependencies(ctx, model.Kind(in.Kind), in.Name)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	type flowOnDisk struct {
		DepField string `json:"dep_field"`
		SrcField string `json:"src_field"`
	}
	out := &listDependenciesOutput{}
	out.Body.Dependencies = make([]upstreamDep, len(rows))
	for i, r := range rows {
		var flows []flowOnDisk
		if len(r.ValueFlows) > 0 {
			_ = json.Unmarshal(r.ValueFlows, &flows)
		}
		mappings := make([]valueMappingDTO, 0, len(flows))
		for _, f := range flows {
			mappings = append(mappings, valueMappingDTO{
				DependentField: f.DepField,
				SourceField:    f.SrcField,
			})
		}
		out.Body.Dependencies[i] = upstreamDep{
			Kind:              string(r.UpstreamKind),
			Name:              r.UpstreamName,
			UpstreamReady:     r.UpstreamReady,
			UpstreamHealthOK:  r.UpstreamHealthOk,
			UpstreamUpdatedAt: r.UpstreamUpdatedAt,
			ValueMappings:     mappings,
		}
	}
	return out, nil
}
