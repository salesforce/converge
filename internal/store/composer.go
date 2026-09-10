package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/model"
)

// refKey is the (kind, name) tuple used for ref→id lookups inside
// ApplyComposeResult.
type refKey struct {
	Kind model.Kind
	Name string
}

// flowEntry is the typed shape of one element in resource_deps.value_flows.
// No `substituted` flag — the dep-gate (`p.synced_gen < p.generation`)
// closes the bootstrap window.
type flowEntry struct {
	DepField string `json:"dep_field"`
	SrcField string `json:"src_field"`
}

// ComposeCounts is the result of ApplyComposeResult.
type ComposeCounts struct {
	// Children: split so a recompose reports how many rows were genuinely
	// new vs. an update to an existing one (vs. pruned). Upserted ==
	// Created+Updated, kept for the post-compose ANALYZE threshold.
	Created  int
	Updated  int
	Upserted int
	Deleted  int

	// Orphan-grace bookkeeping (orphan-grace pruning): Orphaned is how many
	// no-longer-produced children this compose stamped into the grace window
	// (instead of deleting) because their kind has orphan_grace_secs > 0;
	// Readopted is how many previously-orphaned children this compose re-emitted
	// and thus cleared the grace marks for (cancelling a pending teardown).
	Orphaned  int
	Readopted int

	// Edges: same split (new edge vs. value_flows changed vs. pruned).
	EdgesCreated int
	EdgesUpdated int
	Edges        int
	EdgesDeleted int

	// Provider configs the composer emitted: new vs. changed vs. pruned.
	ConfigsCreated int
	ConfigsUpdated int
	ConfigsDeleted int

	// CandidateIDs is every resource id this compose touched. The caller
	// is expected to call ScheduleEligible(CandidateIDs) AFTER the
	// compose tx commits.
	CandidateIDs []uuid.UUID
}

// ApplyComposeResult performs the diff/upsert/delete/edges work for one
// Composer pipeline output against a single parent's children. After
// children + edges are inserted, ApplyValueFlowsForDependents runs an
// in-tx bootstrap pass that fills any flow whose upstream is already
// ready.
//
// SINGLE-TRANSACTION CONTRACT (load-bearing — do not break it). This
// function MUST run inside one caller-owned transaction: callers
// construct the store via s.WithTx(tx) so EVERY sub-operation here —
// child upsert, child prune (hard delete + soft-delete/finalizer
// requests), edge prune, edge upsert, value-flow bootstrap, and the
// composed_gen stamp — executes on that same tx and commits together.
// There is intentionally NO nested Begin(): all the s.* helpers below run
// on s.db, which IS the tx.
//
// Why it must be atomic: the steps are ordered (upsert children → prune →
// upsert edges → flows → stamp), but a crash or error at ANY point is
// safe precisely because none of it is visible until the caller's Commit.
// A mid-process crash rolls the whole thing back to the pre-compose state
// (no half-composed graph: no orphan children, no dangling edges, and
// composed_gen stays unadvanced so the reaper re-runs the composer). A
// successful Commit lands the entire graph + the composed_gen stamp as a
// unit — so composed_gen and the children it accounts for can never
// disagree. The only non-transactional follow-up is the post-commit
// ScheduleEligible(CandidateIDs) in the caller; if that is lost to a
// crash the children are still durable (synced_gen < generation) and the
// reaper re-enqueues them — recoverable, never corrupt.
//
// Stamps resources.composed_gen = parentGeneration in-tx so the worker
// can skip the composer stage on a re-pend caused by "still waiting on
// descendants" (the rollup gate). Without this stamp, every reaper
// tick on a 1M-child root re-runs the entire compose pipeline.
//
// Each ValueFlow on a DepEdge says "fill child.spec.<DependentField>
// from source.status.<SourceField>." The flow is NOT schema-checked per field
// (there is no "does this pointer exist in the source status_schema" step): it
// is applied at runtime as a JSON-pointer copy (jsonb_set_many in the substitute
// pass), guarded by present-and-non-null, so a source field that is absent simply
// does not fire. The flowed VALUE itself is not re-validated on write — it is the
// producer's own status value, trusted as-is. The schema safety net is the
// DEPENDENT CHILD's spec validation below at EMIT time: every child is checked
// against its kind's spec_schema with exactly its flowed top-level fields relaxed
// (present + value unchecked, since the flow fills them later); every non-flowed
// field is enforced strictly, and a closed schema rejects an undeclared field
// terminally. So a flow into a well-defined kind is bounded by that kind's schema
// (minus the deferred fields); a flow into an opaque (additionalProperties:true)
// kind is trusted by design.
//
// ComposePolicy supplies the per-child-kind policy ApplyComposeResult needs when
// it prunes a vanished child: the orphan-grace window and the finalizer (which
// decide soft-vs-hard delete + the grace stamp). The KindManifestCache satisfies
// it; nil = no policy (immediate hard-delete, no grace).
type ComposePolicy interface {
	OrphanGraceForKind(kind model.Kind, kindVersion int) time.Duration
	FinalizerForKind(kind model.Kind, kindVersion int) string
}

func (s *Store) ApplyComposeResult(
	ctx context.Context,
	parentID uuid.UUID,
	parentGeneration int64,
	workID uuid.UUID,
	claimEpoch int64,
	manifestVersion int64,
	children []model.ChildSpec,
	edges []model.DepEdge,
	configs []model.ProviderConfigSpec,
	pol ComposePolicy,
) (ComposeCounts, error) {
	var counts ComposeCounts

	rootID, err := s.queries().GetResourceRootID(ctx, parentID)
	if err != nil {
		return counts, fmt.Errorf("load parent root_id: %w", err)
	}

	existing, err := s.queries().ListExistingChildren(ctx, toUUID(parentID))
	if err != nil {
		return counts, fmt.Errorf("load existing children: %w", err)
	}
	existingByRef := make(map[refKey]dbq.ListExistingChildrenRow, len(existing))
	for _, e := range existing {
		existingByRef[refKey{e.Kind, e.Name}] = e
	}

	// Per-child set of dep_field paths whose value comes from a flowed
	// upstream. Used by the diff check below: if the existing spec only
	// differs from the composer's output in flowed fields, we skip the
	// upsert. Otherwise every recompose churns generation by overwriting
	// the runtime-filled flows, which the substitute pass then refills,
	// producing an infinite reconcile loop.
	flowedPaths := make(map[refKey][][]string, len(children))
	for _, edge := range edges {
		paths := flowedPaths[refKey{edge.From.Kind, edge.From.Name}]
		for _, vf := range edge.Values {
			paths = append(paths, pointerSegments(vf.DependentField))
		}
		flowedPaths[refKey{edge.From.Kind, edge.From.Name}] = paths
	}

	// Build upsert decls (skip rows whose spec+labels haven't changed,
	// ignoring flowed fields the runtime fills). The spec compare still
	// needs the marshaled bytes (specsMatchIgnoringFlowed parses the
	// existing JSON once and diffs with flowed paths cleared). Labels are
	// compared NATIVELY as map[string]string — no canonicalJSON
	// re-marshal of both sides; the existing labels are parsed once into a
	// map. labelsBytes is only built for children that actually need an
	// upsert.
	produced := make(map[refKey]struct{}, len(children))
	var decls []ChildDecl
	// reAdopt collects ids of children that are re-emitted this compose but whose
	// spec is UNCHANGED (so they skip the upsert below and never reach
	// UpdateChildrenHotSQL's mark-clear) AND are currently in the orphan-grace
	// window (a FINITE frozen_until). They get their grace marks cleared after the
	// upsert (re-adoption), without a generation bump — see the ClearOrphanMarks
	// call below. A QUARANTINED child (frozen_until = 'infinity') is NEVER
	// re-adopted by a re-emit — only an explicit un-quarantine lifts it — so it
	// is excluded here and by ClearOrphanMarks' finite-only guard.
	var reAdopt []uuid.UUID
	for _, ch := range children {
		key := refKey{ch.Kind, ch.Name}
		produced[key] = struct{}{}
		// kind_version is REQUIRED and explicit (>= 1) on every emitted child: a
		// composer MUST stamp the web-API version each child is applied at — there is
		// no implicit v1 default. A 0/unset child version is a composition BUG, so
		// fail terminally (stops re-running the composer until the emit is fixed),
		// exactly like an invalid child spec below.
		if ch.KindVersion < 1 {
			return counts, model.Terminal(fmt.Errorf("composed child %s/%s: kind_version is required and must be >= 1 (got %d)", ch.Kind, ch.Name, ch.KindVersion))
		}
		specBytes, err := json.Marshal(ch.Spec)
		if err != nil {
			return counts, fmt.Errorf("marshal child spec %s/%s: %w", ch.Kind, ch.Name, err)
		}
		// Validate the child against ITS kind's declared schema — defense-in-depth
		// at the data gateway: a composer emits typed structs (structurally sound)
		// but can still violate schema CONSTRAINTS the Go type doesn't encode
		// (minLength/enum/required), e.g. a child with account_id:"". Without this
		// the bad child lands in work_queue and only fails later at the worker's
		// decode — or runs a tool with a bad input. An invalid child is a
		// composition BUG, not transient, so wrap terminal: the reconcile fails
		// terminally and stops re-running the composer until the spec is fixed.
		//
		// FLOWED fields are the exception: a child is emitted with its flowed
		// fields absent (e.g. a VPC whose account_id flows from the upstream
		// account's status), to be filled by the substitute pass before the
		// child is ever scheduled. Those fields keep their full constraints for a
		// user create at the API edge, but at emit time they are legitimately
		// absent — so validate the child with the flowed paths skipped. Every
		// non-flowed field (e.g. cidr) is still strictly enforced.
		if s.validator != nil {
			if verr := s.validator.ValidateSpecPartial(ch.Kind, ch.KindVersion, specBytes, topLevelFields(flowedPaths[key])); verr != nil {
				return counts, model.Terminal(fmt.Errorf("%w: composed child %s/%s: %w", ErrInvalidSpec, ch.Kind, ch.Name, verr))
			}
		}
		e, exists := existingByRef[key]
		// RE-ADOPTION: re-emitting an existing child that is currently in the
		// orphan-grace window (a FINITE frozen_until) cancels its pending teardown
		// — clear the mark (below, via ClearOrphanMarks). Collected here, BEFORE
		// the upsert-skip branch, so it fires regardless of which write path the
		// child takes: unchanged (skipped), spec-changed, OR labels-only changed.
		// Re-adoption MUST be path-independent — the spec UPDATE no longer
		// touches frozen_until, so a labels-only or unchanged re-emit would
		// otherwise leave the child stuck Orphaned. ClearOrphanMarks is gated
		// `frozen_until IS NOT NULL AND <> 'infinity'` (cheap no-op for the common
		// never-orphaned case, and NEVER un-quarantines), so collecting every
		// produced-and-orphaned child is safe. A quarantined child ('infinity') is
		// excluded here too — a re-emit must not lift an operator's set-aside.
		if exists && e.FrozenUntil.Valid && e.FrozenUntil.InfinityModifier != pgtype.Infinity {
			reAdopt = append(reAdopt, e.ID)
		}
		if exists &&
			int(e.KindVersion) == ch.KindVersion &&
			specsMatchIgnoringFlowed(e.Spec, specBytes, flowedPaths[key]) &&
			labelsMatch(e.Labels, ch.Labels) {
			// Unchanged spec+labels AND same kindVersion → skip the upsert (no generation
			// churn). A KIND VERSION change (v1→v2 conversion by a versioned composer) is
			// NOT skippable even with an identical spec — the flip must bump the
			// kindVersion + generation so the child re-reconciles under the new kindVersion's
			// reactions/schema. So a kindVersion mismatch falls through to the upsert.
			continue
		}
		labelsBytes := json.RawMessage(`{}`)
		if len(ch.Labels) > 0 {
			labelsBytes, err = json.Marshal(ch.Labels)
			if err != nil {
				return counts, fmt.Errorf("marshal child labels %s/%s: %w", ch.Kind, ch.Name, err)
			}
		}
		var declID uuid.UUID
		if exists {
			counts.Updated++
			declID = e.ID // update-by-id; zero id ⇒ insert with a fresh id
		} else {
			counts.Created++
		}
		decls = append(decls, ChildDecl{
			ID:          declID,
			Kind:        ch.Kind,
			KindVersion: ch.KindVersion,
			Name:        ch.Name,
			Spec:        specBytes,
			Labels:      labelsBytes,
		})
	}

	upserted, err := s.BatchUpsertChildren(ctx, parentID, rootID, decls)
	if err != nil {
		return counts, fmt.Errorf("batch upsert children: %w", err)
	}
	counts.Upserted = len(upserted) // == Created+Updated

	// Prune children the Composer no longer produces. ORPHAN-GRACE: a kind with
	// orphan_grace_secs > 0 does NOT tear a dropped child down immediately —
	// guarding against a buggy composer that drops a child by mistake and
	// re-emits it next cycle (a mistaken drop would otherwise destroy real infra
	// for finalizer kinds, or the row for leaf kinds, before the next compose
	// fixes it). Decision tree per dropped child:
	//   - already in the soft-delete drain (deletion_requested_at) → skip
	//     (idempotent; don't re-stamp/re-enqueue every recompose).
	//   - already frozen (frozen_until set — orphan grace OR quarantine) → skip:
	//     an orphan's grace deadline must stay ABSOLUTE from the first drop (don't
	//     slide it forward), and a quarantined row must not be re-stamped into an
	//     orphan (the operator's set-aside stands; MarkChildrenOrphaned also guards
	//     'infinity' defensively).
	//   - kind has grace > 0 → STAMP the grace window (frozen_until = now() +
	//     grace) and leave the row fully in the DAG; the reaper's
	//     sweep_expired_orphans tears it down only if the window elapses without a
	//     re-emit (soft-delete for finalizer kinds, hard delete for leaf kinds).
	//     Applies to ALL kinds — a dropped finalizer-kind child (real infra) is
	//     exactly the case grace must protect.
	//   - kind has grace == 0 → today's IMMEDIATE behavior: finalizer registered →
	//     request_resource_deletion (soft-delete drain); no finalizer (leaf/test
	//     kinds) → fast batch hard delete.
	// staleSet keeps the ref→id loop below O(existing) (an O(1) membership
	// test per row) instead of O(existing × pruned).
	var hardStale []uuid.UUID
	var graceStale []uuid.UUID
	var graceSecs []int32
	staleSet := make(map[uuid.UUID]struct{})
	for _, e := range existing {
		if _, stillProduced := produced[refKey{e.Kind, e.Name}]; stillProduced {
			continue
		}
		staleSet[e.ID] = struct{}{} // no longer a live child for edge resolution

		if e.DeletionRequestedAt.Valid {
			continue // already draining — request_resource_deletion is idempotent but skip the churn
		}
		if e.FrozenUntil.Valid {
			continue // already frozen (orphan grace or quarantine) — keep the original absolute frozen_until
		}

		var grace time.Duration
		if pol != nil {
			grace = pol.OrphanGraceForKind(e.Kind, int(e.KindVersion))
		}
		if grace > 0 {
			// Defer teardown: stamp the grace window, leave the row in the DAG.
			graceStale = append(graceStale, e.ID)
			graceSecs = append(graceSecs, durSecs(grace))
			counts.Orphaned++
			continue
		}

		// grace == 0 → prune immediately (today's behavior).
		var finalizer string
		if pol != nil {
			finalizer = pol.FinalizerForKind(e.Kind, int(e.KindVersion))
		}
		if finalizer == "" {
			hardStale = append(hardStale, e.ID) // no Deleter → fast hard delete
			continue
		}
		// Soft delete: runs the kind's Deleter before the row is removed.
		if _, err := s.RequestResourceDeletion(ctx, e.ID, finalizer, "composer"); err != nil {
			return counts, fmt.Errorf("soft-delete vanished child %s/%s: %w", e.Kind, e.Name, err)
		}
		counts.Deleted++
	}
	if len(graceStale) > 0 {
		// Sort ascending to keep the project's ascending-id lock discipline.
		sortUUIDsAscending(graceStale, graceSecs)
		if err := s.MarkChildrenOrphaned(ctx, graceStale, graceSecs); err != nil {
			return counts, fmt.Errorf("stamp orphan grace: %w", err)
		}
	}
	if len(hardStale) > 0 {
		if err := s.DeleteChildren(ctx, hardStale); err != nil {
			return counts, fmt.Errorf("delete vanished children: %w", err)
		}
		counts.Deleted += len(hardStale)
	}

	// Re-adopt every re-emitted child that was in the grace window (collected in
	// reAdopt above, path-independently): clear its frozen_until so the re-emit
	// cancels the pending teardown. ClearOrphanMarks is gated `frozen_until IS NOT
	// NULL AND <> 'infinity'` (cheap no-op for never-orphaned ids; never
	// un-quarantines) and does NOT bump generation (an unchanged re-adopt must not
	// churn). The spec UPDATE no longer touches frozen_until, so this single call
	// is the ONLY re-adoption path — covering
	// unchanged-, spec-changed-, and labels-only-re-emits alike. Inside the
	// single compose tx, so the clear commits atomically with the rest.
	if len(reAdopt) > 0 {
		sortUUIDsAscending(reAdopt, nil)
		cleared, err := s.ClearOrphanMarks(ctx, reAdopt)
		if err != nil {
			return counts, fmt.Errorf("clear orphan marks (re-adopt): %w", err)
		}
		counts.Readopted += cleared
	}

	// Resolve refs to ids for edge insert. O(1) staleSet lookup per row.
	refToID := make(map[refKey]uuid.UUID, len(existing)+len(upserted))
	for _, e := range existing {
		if _, gone := staleSet[e.ID]; gone {
			continue
		}
		refToID[refKey{e.Kind, e.Name}] = e.ID
	}
	for _, u := range upserted {
		refToID[refKey{u.Kind, u.Name}] = u.ID
	}

	// Load existing edges (with their value_flows) up front so the edge
	// diff is MINIMAL, exactly like the child diff: only new or
	// flow-changed edges go into depDecls. Without this we shipped every
	// produced edge to PG each recompose (e.g. 3589) and leaned on the
	// server-side IS DISTINCT FROM gate to no-op them — correct, but it
	// transferred the whole set and made counts.Edges report "produced"
	// rather than "actually changed". Now Go computes the delta.
	type edgeKey struct{ dep, dcy uuid.UUID }
	existingDeps, err := s.ListExistingDepsByOwner(ctx, parentID)
	if err != nil {
		return counts, fmt.Errorf("load existing deps: %w", err)
	}
	existingFlowByEdge := make(map[edgeKey]json.RawMessage, len(existingDeps))
	for _, e := range existingDeps {
		existingFlowByEdge[edgeKey{e.DependentID, e.DependencyID}] = e.ValueFlows
	}

	// Build the upsert delta + the produced-edge key set (for pruning).
	producedEdges := make(map[edgeKey]struct{}, len(edges))
	var depDecls []DepDecl
	dependentIDsWithFlows := make(map[uuid.UUID]struct{})
	for _, edge := range edges {
		// Both endpoints MUST resolve to a child this compose produced (or a
		// surviving prior child). An unresolved ref means the composer
		// emitted an edge to a name it didn't compose, or — with global
		// (kind,name) uniqueness — a name already owned by a DIFFERENT parent
		// (so BatchUpsertChildrenSQL's owner-guarded upsert skipped it and it
		// never entered refToID). Failing loud beats silently dropping the
		// edge and corrupting the dep graph; the root surfaces as Failed with
		// a real reason instead of looking healthy with missing wiring.
		depID, ok := refToID[refKey{edge.From.Kind, edge.From.Name}]
		if !ok {
			return counts, fmt.Errorf("compose edge dependent %s/%s does not resolve to a composed child (name collision across owners, or edge to an un-emitted child?)", edge.From.Kind, edge.From.Name)
		}
		depDcyID, ok := refToID[refKey{edge.To.Kind, edge.To.Name}]
		if !ok {
			return counts, fmt.Errorf("compose edge dependency %s/%s does not resolve to a composed child (name collision across owners, or edge to an un-emitted child?)", edge.To.Kind, edge.To.Name)
		}

		// Self-edge (A→A): a resource can't depend on itself — the upstream gate
		// would wait for its own synced_gen to catch up to its own generation,
		// which never advances because the dep blocks it: a permanent stall.
		// Terminal (a composition BUG; re-running the composer reproduces it).
		if depID == depDcyID {
			return counts, model.Terminal(fmt.Errorf("compose edge %s/%s → %s/%s is a self-edge (a resource cannot depend on itself)", edge.From.Kind, edge.From.Name, edge.To.Kind, edge.To.Name))
		}

		// NOTE: there is no authoring-time value-flow FIELD validation here.
		// Manifests carry JSON Schemas, not a resolvable field-path type, so a
		// malformed flow field surfaces at substitution time (a missed
		// substitution); the worker validates its own specs.

		// Proposed flows as NATIVE typed slice (the composer's own form).
		// We compare against the existing edge using this native slice —
		// no JSON round-trip of the proposed side just to diff. The one
		// json.Marshal happens only if the edge actually needs an upsert.
		proposed := make([]flowEntry, len(edge.Values))
		for i, vf := range edge.Values {
			proposed[i] = flowEntry{DepField: vf.DependentField, SrcField: vf.SourceField}
		}
		if len(proposed) > 0 {
			dependentIDsWithFlows[depID] = struct{}{}
		}

		key := edgeKey{depID, depDcyID}
		producedEdges[key] = struct{}{}

		// Skip the upsert for an edge that already exists with identical
		// value_flows (the edge analogue of the child skip). The existing
		// flows are decoded ONCE into the same []flowEntry shape and
		// compared natively (slices.Equal) — order-sensitive, which is
		// correct (jsonb preserves array order). No marshal of the
		// proposed side, no DeepEqual.
		existingRaw, exists := existingFlowByEdge[key]
		if exists && slices.Equal(decodeFlowEntries(existingRaw), proposed) {
			continue
		}

		encoded, err := json.Marshal(proposed) // only for edges we actually write
		if err != nil {
			return counts, fmt.Errorf("marshal value_flows for %s→%s: %w", edge.From, edge.To, err)
		}
		if exists {
			counts.EdgesUpdated++
		} else {
			counts.EdgesCreated++
		}
		depDecls = append(depDecls, DepDecl{
			DependentID:  depID,
			DependencyID: depDcyID,
			ValueFlows:   encoded,
		})
	}

	// Cycle guard: the resolved edge set MUST be acyclic. A cycle (A→B→A, or
	// longer) would wedge the system AFTER commit — the upstream gate
	// (schedule_eligible) waits for every dependency to be synced, but in a cycle
	// no member can ever synced-advance because each waits on the next, so the
	// cascade re-enqueues them forever (a CPU livelock; data stays consistent but
	// the subtree never reaches Ready and needs manual intervention). Nothing
	// downstream detects it — the recursive CTEs over resource_deps cap by DEPTH,
	// not cycle. So reject it HERE, before BatchUpsertResourceDeps persists it:
	// the tx rolls back, the root fails with the offending cycle named, and the
	// bad wiring never enters the graph. Terminal — a cycle is a composition BUG,
	// deterministic across retries. producedEdges is exactly this root's
	// post-apply edge set (the prune below removes everything not in it), and
	// composed edges only ever connect this root's own children (both endpoints
	// resolved through refToID), so checking producedEdges is complete — no need
	// to walk the global graph. O(V+E) over a small per-root fan-out.
	adj := make(map[uuid.UUID][]uuid.UUID, len(producedEdges))
	for e := range producedEdges {
		adj[e.dep] = append(adj[e.dep], e.dcy) // dependent → dependency
	}
	if cycle := detectDepCycle(adj); cycle != nil {
		idToRef := make(map[uuid.UUID]refKey, len(refToID))
		for ref, id := range refToID {
			idToRef[id] = ref
		}
		return counts, model.Terminal(fmt.Errorf("compose produced a dependency cycle: %s", formatCycle(cycle, idToRef)))
	}

	// Prune edges the new compose no longer emits (mirror of the child
	// prune). A leftover edge would otherwise keep gating the dependent on
	// a now-unrelated upstream and keep re-substituting its status into
	// the dependent's spec.
	var staleDep, staleDcy []uuid.UUID
	for _, e := range existingDeps {
		if _, stillProduced := producedEdges[edgeKey{e.DependentID, e.DependencyID}]; stillProduced {
			continue
		}
		staleDep = append(staleDep, e.DependentID)
		staleDcy = append(staleDcy, e.DependencyID)
	}
	if len(staleDep) > 0 {
		if err := s.DeleteResourceDeps(ctx, staleDep, staleDcy); err != nil {
			return counts, fmt.Errorf("delete stale deps: %w", err)
		}
		counts.EdgesDeleted = len(staleDep)
	}

	if err := s.BatchUpsertResourceDeps(ctx, depDecls); err != nil {
		return counts, fmt.Errorf("upsert deps: %w", err)
	}
	counts.Edges = counts.EdgesCreated + counts.EdgesUpdated // == len(depDecls)

	// Bootstrap value-flow substitution: if the upstream of a freshly
	// composed flow is already ready, write its status into the
	// dependent's spec. Runs in-tx so the bump_generation trigger fires
	// for any updated dependent before we hand the candidate ids back
	// for ScheduleEligible. Out-of-tx the dep-gate would have caught up
	// with the upstream's synced_gen but the dependent's spec wouldn't
	// reflect the flowed value yet.
	if len(dependentIDsWithFlows) > 0 {
		ids := make([]uuid.UUID, 0, len(dependentIDsWithFlows))
		for id := range dependentIDsWithFlows {
			ids = append(ids, id)
		}
		if err := s.ApplyValueFlowsForDependents(ctx, ids); err != nil {
			return counts, fmt.Errorf("apply value flows for dependents: %w", err)
		}
	}

	// CandidateIDs is only the children this compose actually TOUCHED:
	// upserted (spec changed/new → generation bumped) plus dependents
	// whose flows were just bootstrapped (ApplyValueFlowsForDependents
	// bumped their generation). Unchanged children are already settled
	// (synced_gen >= generation) and schedule_eligible's gate would filter
	// them out anyway — handing it all 1M surviving ids just makes it
	// probe 1M rows for nothing every recompose. Dedup via a set since a
	// freshly-flowed dependent may also be in `upserted`.
	candidateSet := make(map[uuid.UUID]struct{}, len(upserted)+len(dependentIDsWithFlows))
	for _, u := range upserted {
		candidateSet[u.ID] = struct{}{}
	}
	for id := range dependentIDsWithFlows {
		candidateSet[id] = struct{}{}
	}
	if len(candidateSet) > 0 {
		counts.CandidateIDs = make([]uuid.UUID, 0, len(candidateSet))
		for id := range candidateSet {
			counts.CandidateIDs = append(counts.CandidateIDs, id)
		}
	}

	// ── Provider configs ──────────────────────────────────────────────
	// Diff the configs this compose emits against the ones this root
	// already OWNS: upsert the new/changed delta (owner-guarded so a
	// cross-owner name collision can't clobber), prune the vanished.
	// Owned by parentID so ON DELETE CASCADE GCs them when the root is
	// deleted. Configs are inert data providers READ — NOT scheduled —
	// so this is independent of children/edges and contributes nothing to
	// CandidateIDs. Editing a DEFAULT config here fires the is_default-gated
	// providerconfigs_changed NOTIFY, live-reconfiguring workers; a CUSTOM
	// config applies to its consumers on their next schedule (snapshot clone).
	// One indexed by-owner read runs every compose (so a drop-to-zero still
	// prunes); compose is per-ROOT (few), not the per-child hot path, so the
	// extra probe is proportional to the existing child/edge diffs here.
	if err := s.applyComposedConfigs(ctx, parentID, configs, &counts); err != nil {
		return counts, err
	}

	// Stamp composed_gen so a future re-pend (e.g. rollup waiting on
	// descendants) skips the composer. FENCED: this is the LAST write of the
	// compose tx and is gated on the (work_id) claim STILL bearing claimEpoch at
	// parentGeneration under the same manifestVersion. rows_affected=0 means the
	// claim was reaped / reassigned mid-compose (its epoch was bumped) — return an
	// error so the caller rolls the WHOLE compose tx back, rather than commit a
	// stale-generation child graph + composed_gen over a newer generation's work.
	stamped, err := s.queries().StampComposedGen(ctx, dbq.StampComposedGenParams{
		ID:              parentID,
		ComposedGen:     parentGeneration,
		WorkID:          workID,
		ClaimEpoch:      claimEpoch,
		ManifestVersion: manifestVersion,
	})
	if err != nil {
		return counts, fmt.Errorf("stamp composed_gen: %w", err)
	}
	if stamped == 0 {
		return counts, fmt.Errorf("%w: compose claim lost mid-flight (work_id=%s gen=%d) — rolling back compose",
			ErrComposeClaimLost, workID, parentGeneration)
	}

	return counts, nil
}

// applyComposedConfigs diffs the provider configs one compose emitted against
// the configs the root already owns, upserting the new/changed delta and
// pruning the vanished — the config analogue of the child/edge diffs. All
// owned by ownerID (the composing root). Counts are accumulated into counts.
func (s *Store) applyComposedConfigs(ctx context.Context, ownerID uuid.UUID, configs []model.ProviderConfigSpec, counts *ComposeCounts) error {
	existing, err := s.ListProviderConfigsByOwner(ctx, ownerID)
	if err != nil {
		return fmt.Errorf("load existing configs: %w", err)
	}
	existingByName := make(map[string]ExistingProviderConfig, len(existing))
	for _, e := range existing {
		existingByName[e.Name] = e
	}

	produced := make(map[string]struct{}, len(configs))
	// claimedDefault tracks the name that owns each kind's single DEFAULT — DB
	// state plus what this compose has already claimed — so a SECOND default for
	// the same kind (another name in this batch, or a default another owner/user
	// already holds) is demoted to a custom override instead of tripping
	// uq_providerconfigs_default_per_kind, which would abort the whole compose tx
	// and re-pend it forever (a 23505 poison pill). The legitimate case — a
	// composer setting its kind's default when none exists, or re-applying its
	// own — is unaffected.
	// The default is per-(kind, kindVersion), so the collision key is (kind, kindVersion): a
	// vpc/v1 default and a vpc/v2 default coexist.
	claimedDefault := make(map[model.KindVersion]string)
	var decls []ProviderConfigDecl
	for _, c := range configs {
		if c.Name == "" {
			return fmt.Errorf("composed provider config has empty name (kind %q)", c.Kind)
		}
		if _, dup := produced[c.Name]; dup {
			return fmt.Errorf("composer emitted duplicate provider config name %q", c.Name)
		}
		produced[c.Name] = struct{}{}
		// kind_version is REQUIRED and explicit (>= 1) on every emitted config: a
		// composer MUST name the (kind, kind_version) each config configures — no
		// implicit v1 default (a 0 is a composition bug, rejected before the tx).
		if c.KindVersion < 1 {
			return fmt.Errorf("composed provider config %q for %s: kind_version is required and must be >= 1 (got %d)", c.Name, c.Kind, c.KindVersion)
		}
		cmaj := c.KindVersion

		if c.IsDefault {
			key := model.KindVersion{Kind: c.Kind, Version: cmaj}
			winner, ok := claimedDefault[key]
			if !ok {
				existing, found, err := s.defaultConfigName(ctx, c.Kind, cmaj)
				if err != nil {
					return fmt.Errorf("check existing default for %q/v%d: %w", c.Kind, cmaj, err)
				}
				if found {
					winner, ok = existing, true
				}
			}
			if ok && winner != c.Name {
				slog.Warn("composer-emitted default conflicts with the (kind,kindVersion)'s existing default; storing as a custom override",
					"name", c.Name, "kind", c.Kind, "kind_version", cmaj, "existing_default", winner, "owner", ownerID)
				c.IsDefault = false
			} else {
				claimedDefault[key] = c.Name
			}
		}

		specBytes, err := json.Marshal(c.Spec)
		if err != nil {
			return fmt.Errorf("marshal provider config %q: %w", c.Name, err)
		}
		// Skip the upsert when an owned config of this name already matches on
		// every column (kind/kindVersion/is_default/spec) — same no-churn discipline as
		// the child diff. specsMatchIgnoringFlowed with nil paths is a pure
		// key-order-insensitive JSON compare.
		if e, ok := existingByName[c.Name]; ok &&
			e.Kind == c.Kind &&
			e.KindVersion == cmaj &&
			e.IsDefault == c.IsDefault &&
			specsMatchIgnoringFlowed(e.Spec, specBytes, nil) {
			continue
		}
		if _, ok := existingByName[c.Name]; ok {
			counts.ConfigsUpdated++
		} else {
			counts.ConfigsCreated++
		}
		decls = append(decls, ProviderConfigDecl{
			Name:        c.Name,
			Kind:        c.Kind,
			KindVersion: cmaj,
			IsDefault:   c.IsDefault,
			Spec:        specBytes,
		})
	}
	if err := s.BatchUpsertProviderConfigs(ctx, ownerID, decls); err != nil {
		return fmt.Errorf("upsert composed configs: %w", err)
	}

	// Prune configs the new compose no longer emits.
	var stale []uuid.UUID
	for _, e := range existing {
		if _, stillProduced := produced[e.Name]; stillProduced {
			continue
		}
		stale = append(stale, e.ID)
	}
	if err := s.DeleteProviderConfigs(ctx, stale); err != nil {
		return fmt.Errorf("prune composed configs: %w", err)
	}
	counts.ConfigsDeleted = len(stale)
	return nil
}

// sortUUIDsAscending sorts ids ascending IN PLACE, keeping the optional
// parallel int32 slice (e.g. per-id grace seconds) aligned to the same
// permutation. nil parallel sorts ids alone. Used before the orphan-grace
// bulk writes so they lock rows in the project's global ascending-id order
// (the discipline that keeps the cascade trigger and these writes deadlock-
// free); the pruned/re-adopted sets are tiny per compose, so a plain
// insertion-style sort via the stdlib is fine.
func sortUUIDsAscending(ids []uuid.UUID, parallel []int32) {
	if parallel == nil {
		slices.SortFunc(ids, bytesCompareUUID)
		return
	}
	idx := make([]int, len(ids))
	for i := range idx {
		idx[i] = i
	}
	slices.SortFunc(idx, func(a, b int) int {
		return bytesCompareUUID(ids[a], ids[b])
	})
	sortedIDs := make([]uuid.UUID, len(ids))
	sortedPar := make([]int32, len(parallel))
	for newPos, oldPos := range idx {
		sortedIDs[newPos] = ids[oldPos]
		sortedPar[newPos] = parallel[oldPos]
	}
	copy(ids, sortedIDs)
	copy(parallel, sortedPar)
}

// bytesCompareUUID orders two UUIDs by their 16-byte big-endian value — the
// same byte order Postgres uses for uuid comparison, so an ascending Go sort
// matches the DB's row-lock order.
func bytesCompareUUID(a, b uuid.UUID) int {
	for i := 0; i < 16; i++ {
		switch {
		case a[i] < b[i]:
			return -1
		case a[i] > b[i]:
			return 1
		}
	}
	return 0
}

// detectDepCycle returns a cycle in the dependency adjacency (dependent →
// dependencies) as the ordered node ids forming the loop (e.g. [A, B, A]), or
// nil if the graph is acyclic. Standard DFS three-colouring: white = unvisited,
// grey = on the current recursion stack, black = fully explored. An edge to a
// grey node closes a cycle; we reconstruct it from the stack. O(V+E) — the
// per-root composed edge set is small. Self-edges are already rejected upstream,
// but this also catches them (A in adj[A] → A is grey when revisited).
func detectDepCycle(adj map[uuid.UUID][]uuid.UUID) []uuid.UUID {
	const (
		white = 0
		grey  = 1
		black = 2
	)
	color := make(map[uuid.UUID]int, len(adj))
	var stack []uuid.UUID

	var visit func(n uuid.UUID) []uuid.UUID
	visit = func(n uuid.UUID) []uuid.UUID {
		color[n] = grey
		stack = append(stack, n)
		for _, m := range adj[n] {
			switch color[m] {
			case white:
				if c := visit(m); c != nil {
					return c
				}
			case grey:
				// Found a back-edge to m, which is on the stack: the cycle is the
				// stack slice from m to the top, plus m again to close it.
				for i, s := range stack {
					if s == m {
						cyc := append([]uuid.UUID{}, stack[i:]...)
						return append(cyc, m)
					}
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[n] = black
		return nil
	}

	for n := range adj {
		if color[n] == white {
			if c := visit(n); c != nil {
				return c
			}
		}
	}
	return nil
}

// formatCycle renders a cycle's node ids as "kind/name → kind/name → …" for the
// error message, using the id→ref map. An id missing from the map (shouldn't
// happen — every cycle node is a composed child) falls back to its uuid.
func formatCycle(cycle []uuid.UUID, idToRef map[uuid.UUID]refKey) string {
	parts := make([]string, len(cycle))
	for i, id := range cycle {
		if ref, ok := idToRef[id]; ok {
			parts[i] = string(ref.Kind) + "/" + ref.Name
		} else {
			parts[i] = id.String()
		}
	}
	return strings.Join(parts, " → ")
}

// topLevelFields collapses flowed dep-field paths to the set of distinct
// top-level field names, for ValidateSpecPartial's skip list. Every flowed
// field today is a top-level spec key (account_id, vpc_id, …); a nested path
// (len > 1) is skipped here — the schema's `required` set is top-level, so
// relaxing a nested path is meaningless, and a nested field has no top-level
// required entry to trip. If nested-required flows ever exist, partial
// validation would need a deeper relax; until then this keeps the contract
// honest rather than silently mis-relaxing.
func topLevelFields(paths [][]string) []string {
	if len(paths) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(paths))
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		if len(p) != 1 {
			continue
		}
		if _, dup := seen[p[0]]; dup {
			continue
		}
		seen[p[0]] = struct{}{}
		out = append(out, p[0])
	}
	return out
}

func pointerSegments(p string) []string {
	if p == "" || p == "/" {
		return nil
	}
	p = strings.TrimPrefix(p, "/")
	if p == "" {
		return nil
	}
	return strings.Split(p, "/")
}

// specsMatchIgnoringFlowed reports whether existing and proposed are
// equal when the flowed-field paths are ignored. The composer emits a
// "clean" spec each run; the runtime fills the flowed fields from
// upstream status. Comparing equality strictly would force a re-upsert
// every reconcile, which bumps generation and re-triggers reconcile —
// an infinite loop. We compare with the flowed paths cleared on both
// sides.
//
// Both sides are unmarshaled into `any` (JSON object → map[string]any,
// array → []any, number → float64) and compared with reflect.DeepEqual.
// Going through json.Unmarshal normalizes BOTH sides identically (key
// order, nil/empty, number representation), so DeepEqual on the decoded
// values is a true semantic compare — no re-marshal to canonical bytes
// needed. JSON ops per call: 2 unmarshals, 0 marshals. A parse failure on
// either side returns false (never treat unparseable specs as matching).
func specsMatchIgnoringFlowed(existing, proposed json.RawMessage, paths [][]string) bool {
	var ev, pv any
	if err := json.Unmarshal(existing, &ev); err != nil {
		return false
	}
	if err := json.Unmarshal(proposed, &pv); err != nil {
		return false
	}
	for _, path := range paths { // no-op when paths is empty
		deleteAtPath(ev, path)
		deleteAtPath(pv, path)
	}
	return reflect.DeepEqual(ev, pv)
}

func deleteAtPath(v any, path []string) {
	if len(path) == 0 {
		return
	}
	cur := v
	for _, seg := range path[:len(path)-1] {
		m, ok := cur.(map[string]any)
		if !ok {
			return
		}
		next, ok := m[seg]
		if !ok {
			return
		}
		cur = next
	}
	if m, ok := cur.(map[string]any); ok {
		delete(m, path[len(path)-1])
	}
}

// decodeFlowEntries decodes the existing edge's value_flows JSONB into
// the native []flowEntry the composer produces, so the diff is a typed
// slices.Equal of []flowEntry — no JSON round-trip of the proposed side,
// no reflect.DeepEqual. The existing flows only exist as DB JSON, so this
// single unmarshal of the existing side is unavoidable; the proposed side
// stays native. Empty / absent / "[]" all decode to a nil/empty slice,
// which slices.Equal treats as equal to a proposed empty slice.
func decodeFlowEntries(raw json.RawMessage) []flowEntry {
	if len(raw) == 0 {
		return nil
	}
	var v []flowEntry
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil
	}
	return v
}

// labelsMatch reports whether the existing labels JSON equals the
// proposed labels map. Compared NATIVELY: the existing JSON is parsed
// once into a map[string]string and compared with maps.Equal against the
// proposed map — no re-marshal. Empty/absent existing labels ('{}' or "")
// equal a nil/empty proposed map.
func labelsMatch(existing json.RawMessage, proposed map[string]string) bool {
	var ev map[string]string
	if len(existing) > 0 {
		if err := json.Unmarshal(existing, &ev); err != nil {
			return false
		}
	}
	return maps.Equal(ev, proposed)
}
