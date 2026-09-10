package test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/sdk-go/converge"
)

// Reverse-dependency cascade delete (kro-style). Deleting a resource MARKS its whole
// teardown tree for deletion — the resource, everything it OWNS (composition
// subtree), and everything that DEPENDS on it (resource_deps), transitively — runs
// each node's finalizer teardown, and removes each row only once its finalizers are
// done AND it has no remaining owned child AND no remaining dependent. So a parent
// always waits for its children and dependents; teardown converges bottom-up. A node
// whose teardown can never finish keeps blocking its parent (STRICT).
//
// The subtree is built by the REAL composer reconcile (a spec-driven
// controllableComposer that stably produces the children + dep edges, so it never
// prunes them), then a real in-process worker + drainer + reaper drive teardown —
// exercising cascade_mark_for_deletion, the drain's gated hard-delete, and
// sweep_deletable exactly as in production (only the Connect broker hop is elided).

// cascadeDeletable is a finalizer kind whose teardown reaction can be made to fail
// terminally for specific resource ids, recording each id's teardown-success time.
// Keyed by RESOURCE ID because delete tasks carry spec=NULL (no name to read).
type cascadeDeletable struct {
	mu           sync.Mutex
	kind         model.Kind
	failTerminal map[uuid.UUID]bool
	teardownAt   map[uuid.UUID]time.Time
}

func newCascadeDeletable(kind model.Kind) *cascadeDeletable {
	return &cascadeDeletable{kind: kind, failTerminal: map[uuid.UUID]bool{}, teardownAt: map[uuid.UUID]time.Time{}}
}

func (w *cascadeDeletable) finalizerName() string { return "test.io/" + string(w.kind) }

func (w *cascadeDeletable) setTerminal(id uuid.UUID) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failTerminal[id] = true
}

func (w *cascadeDeletable) succeededAt(id uuid.UUID) time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.teardownAt[id]
}

type errTeardown string

func (e errTeardown) Error() string { return string(e) }

var _ converge.Provider = (*cascadeDeletable)(nil)

// Kind is the single (kind, version) this finalizer kind serves.
func (w *cascadeDeletable) Kind() converge.KindVersion {
	return converge.KindVersion{Kind: converge.Kind(w.kind), Version: 1}
}

// OnConfig is a no-op: this test kind reads no default providerconfig.
func (*cascadeDeletable) OnConfig(converge.ProviderConfig) {}

// Ready is always true: a pure in-process test controller has no downstream to dial.
func (*cascadeDeletable) Ready() bool { return true }

// Work dispatches on req.Reaction: "work" (spec change → status) and "teardown" (delete
// requested → record the teardown time, or fail terminally for a marked resource id).
func (w *cascadeDeletable) Work(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	if req.Reaction == "teardown" {
		id := req.Resource.ID
		w.mu.Lock()
		terminal := w.failTerminal[id]
		w.mu.Unlock()
		if terminal {
			return converge.Outcome{}, converge.Terminal(errTeardown("teardown always fails (test)"))
		}
		w.mu.Lock()
		w.teardownAt[id] = time.Now()
		w.mu.Unlock()
		return converge.Outcome{}, nil
	}
	out, _ := json.Marshal(map[string]string{"name": nameFromSpec(req.Resource.Spec)})
	return converge.Outcome{Status: out}, nil
}

func (w *cascadeDeletable) Manifest() model.KindManifest {
	return model.KindManifest{
		Kind:          w.kind,
		KindVersion:   1,
		FinalizerName: w.finalizerName(),
		Reactions: []model.ReactionDecl{
			{Name: "work", Trigger: model.TriggerSpecChange, Emits: model.OutcomeMask{model.OutcomeStatus}},
			{Name: "teardown", Trigger: model.TriggerDeleteRequested, Emits: model.OutcomeMask{model.OutcomeFinalizer}, Finalizer: w.finalizerName()},
		},
	}
}

func cascadeRootSpec(delKind model.Kind, children []string, deps map[string][]string) json.RawMessage {
	type childSpec struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	}
	spec := map[string]any{}
	cs := make([]childSpec, len(children))
	for i, n := range children {
		cs[i] = childSpec{Kind: string(delKind), Name: n}
	}
	spec["children"] = cs
	if len(deps) > 0 {
		d := map[string][]childSpec{}
		for child, on := range deps {
			for _, dep := range on {
				d[child] = append(d[child], childSpec{Kind: string(delKind), Name: dep})
			}
		}
		spec["deps_on"] = d
	}
	b, _ := json.Marshal(spec)
	return b
}

// setupCascadeSubtree stands up a composer root whose spec stably produces the given
// finalizer-kind children + dep edges, waits for the engine to create them, and
// returns the root id + an eager name→id snapshot.
func setupCascadeSubtree(t *testing.T, ctx context.Context, pool *pgxpool.Pool, delKind model.Kind, dw *cascadeDeletable, rootName string, children []string, deps map[string][]string) (uuid.UUID, func(string) uuid.UUID) {
	t.Helper()
	comp := newControllableComposer()
	reg := newTReg()
	reg.AddKind(comp, comp.Manifest())
	reg.AddKind(dw, dw.Manifest())

	te := startEngineWithRegistry(t, ctx, pool, reg)
	require.NoError(t, te.Start(ctx))
	t.Cleanup(func() { _ = te.Stop(ctx) })

	rootID, err := te.CreateRoot(ctx, comp.Manifest().Kind, rootName, cascadeRootSpec(delKind, children, deps), nil)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		var n int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM resources WHERE root_id=$1`, rootID).Scan(&n))
		return n == len(children)
	}, 30*time.Second, 200*time.Millisecond, "composer must create the subtree before we delete it")

	return rootID, childIDResolver(t, ctx, pool, rootID)
}

// TestReverseCascadeOrdering: delete a root; its whole subtree is marked and torn
// down, a dependent finalizes no later than its dependency, and the root goes last.
func TestReverseCascadeOrdering(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	pool := setupPostgres(t, ctx)

	const delKind model.Kind = "cascade-node"
	dw := newCascadeDeletable(delKind)
	// vm → subnet → vpc (vm depends on subnet depends on vpc).
	rootID, idOf := setupCascadeSubtree(t, ctx, pool, delKind, dw, "casc-root",
		[]string{"vpc", "subnet", "vm"},
		map[string][]string{"subnet": {"vpc"}, "vm": {"subnet"}})
	vpcID, subnetID, vmID := idOf("vpc"), idOf("subnet"), idOf("vm")

	repo := store.New(pool)
	_, err := repo.RequestResourceDeletion(ctx, rootID, "", "test")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return !rowExists(t, ctx, pool, rootID) &&
			!rowExists(t, ctx, pool, vpcID) &&
			!rowExists(t, ctx, pool, subnetID) &&
			!rowExists(t, ctx, pool, vmID)
	}, 90*time.Second, 500*time.Millisecond, "root + whole subtree must be torn down")

	vm := dw.succeededAt(vmID)
	subnet := dw.succeededAt(subnetID)
	vpc := dw.succeededAt(vpcID)
	require.False(t, vm.IsZero() || subnet.IsZero() || vpc.IsZero(), "every child's finalizer teardown must run (no skipped finalizers)")
	require.True(t, !vm.After(subnet), "vm (dependent) finalizes no later than subnet (its dependency)")
	require.True(t, !subnet.After(vpc), "subnet finalizes no later than vpc (its dependency)")
	t.Logf("reverse-topo teardown order held; root removed last")
}

// TestCascadeMarksDependentsOnAnyDelete: deleting a mid-graph resource marks its
// dependents (and owned children) for deletion too — the tree is marked on ANY
// delete, not only a root delete.
func TestCascadeMarksDependentsOnAnyDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	pool := setupPostgres(t, ctx)

	const delKind model.Kind = "cascade-node2"
	dw := newCascadeDeletable(delKind)
	// subnet depends on vpc. Delete vpc directly → subnet (its dependent) must also
	// be marked + torn down, and vpc waits for subnet.
	_, idOf := setupCascadeSubtree(t, ctx, pool, delKind, dw, "casc-root2",
		[]string{"vpc", "subnet"}, map[string][]string{"subnet": {"vpc"}})
	vpcID, subnetID := idOf("vpc"), idOf("subnet")

	repo := store.New(pool)
	_, err := repo.RequestResourceDeletion(ctx, vpcID, dw.finalizerName(), "test")
	require.NoError(t, err)

	// The dependent subnet is marked for deletion (cascade reached it).
	require.Eventually(t, func() bool {
		return isDeleting(t, ctx, pool, subnetID)
	}, 15*time.Second, 200*time.Millisecond, "deleting a dependency must mark its dependent for deletion")

	// Both are eventually torn down, subnet (dependent) before vpc's row is removed.
	require.Eventually(t, func() bool {
		return !rowExists(t, ctx, pool, vpcID) && !rowExists(t, ctx, pool, subnetID)
	}, 90*time.Second, 500*time.Millisecond, "both the dependency and its dependent tear down")
	require.True(t, !dw.succeededAt(subnetID).After(dw.succeededAt(vpcID)),
		"the dependent subnet finalizes no later than its dependency vpc")
	t.Logf("cascade marked + tore down the dependent when its dependency was deleted")
}

// TestStrictWaitTerminalDependentBlocksParent: a dependent whose teardown can never
// finish keeps its dependency (parent) blocked in Deleting indefinitely (STRICT — no
// dead-letter unblock). The dependency must NOT be removed while the dependent lives.
func TestStrictWaitTerminalDependentBlocksParent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := setupPostgres(t, ctx)

	const delKind model.Kind = "cascade-node3"
	dw := newCascadeDeletable(delKind)
	rootID, idOf := setupCascadeSubtree(t, ctx, pool, delKind, dw, "casc-root3",
		[]string{"vpc", "subnet"}, map[string][]string{"subnet": {"vpc"}})
	vpcID, subnetID := idOf("vpc"), idOf("subnet")
	dw.setTerminal(subnetID) // subnet's teardown can never complete

	repo := store.New(pool)
	_, err := repo.RequestResourceDeletion(ctx, rootID, "", "test")
	require.NoError(t, err)

	// Give the system time to run what it can. subnet stays (teardown never done),
	// and BECAUSE subnet still depends on vpc, vpc must NOT be removed (strict wait).
	require.Eventually(t, func() bool { return isDeleting(t, ctx, pool, subnetID) }, 15*time.Second, 200*time.Millisecond, "subnet marked")
	time.Sleep(6 * time.Second)
	require.True(t, rowExists(t, ctx, pool, subnetID), "the un-deletable subnet stays (its teardown never completes)")
	require.True(t, rowExists(t, ctx, pool, vpcID), "vpc (dependency) must stay BLOCKED while its dependent subnet lives (strict wait)")
	t.Logf("strict wait held: a stuck dependent keeps its dependency blocked")
}

// TestDeleteOverridesQuarantine: a QUARANTINED node inside a deletion tree must be
// marked + torn down with the rest (delete overrides the set-aside), not left behind.
// Guards that cascade_mark includes frozen_until rows: skipping them would leak the
// quarantined node AND (because the sweep's gate reads the un-marked dependent as
// "not deleting") let its dependency be removed out from under it.
func TestDeleteOverridesQuarantine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pool := setupPostgres(t, ctx)

	const delKind model.Kind = "cascade-node4"
	dw := newCascadeDeletable(delKind)
	// subnet depends on vpc; then QUARANTINE the subnet before deleting the root.
	rootID, idOf := setupCascadeSubtree(t, ctx, pool, delKind, dw, "casc-root4",
		[]string{"vpc", "subnet"}, map[string][]string{"subnet": {"vpc"}})
	vpcID, subnetID := idOf("vpc"), idOf("subnet")

	repo := store.New(pool)
	ok, err := repo.QuarantineResource(ctx, subnetID, "test")
	require.NoError(t, err)
	require.True(t, ok)
	// Confirm it's frozen (quarantined) before the delete.
	require.Eventually(t, func() bool { return isQuarantined(t, ctx, pool, subnetID) }, 10*time.Second, 200*time.Millisecond, "subnet quarantined")

	// Delete the whole tree. Delete must OVERRIDE the quarantine: the subnet is
	// marked (frozen_until cleared), torn down, and its dependency vpc + the root go
	// too — nothing leaks, no dangling reference.
	_, err = repo.RequestResourceDeletion(ctx, rootID, "", "test")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return !rowExists(t, ctx, pool, rootID) &&
			!rowExists(t, ctx, pool, vpcID) &&
			!rowExists(t, ctx, pool, subnetID)
	}, 60*time.Second, 500*time.Millisecond,
		"delete must OVERRIDE quarantine: the quarantined subnet + its dependency + root all torn down (no leak)")

	// And ordering still held: the (previously quarantined) dependent subnet's
	// teardown ran no later than its dependency vpc's — no dangling reference.
	require.True(t, !dw.succeededAt(subnetID).After(dw.succeededAt(vpcID)),
		"the (un-quarantined) subnet finalizes no later than its dependency vpc")
	t.Logf("delete overrode quarantine: whole tree torn down in order, nothing leaked")
}

// ── direct-SQL assertion helpers ──

func childIDResolver(t *testing.T, ctx context.Context, pool *pgxpool.Pool, rootID uuid.UUID) func(string) uuid.UUID {
	t.Helper()
	ids := map[string]uuid.UUID{}
	rows, err := pool.Query(ctx,
		`SELECT r.id, m.name FROM resources r JOIN resource_meta m ON m.id = r.id WHERE r.root_id = $1`, rootID)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var id uuid.UUID
		var name string
		require.NoError(t, rows.Scan(&id, &name))
		ids[name] = id
	}
	require.NoError(t, rows.Err())
	return func(name string) uuid.UUID {
		id, ok := ids[name]
		require.True(t, ok, "child %q not found in subtree snapshot", name)
		return id
	}
}

func rowExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) bool {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM resources WHERE id=$1`, id).Scan(&n))
	return n > 0
}

func isDeleting(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) bool {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM resources WHERE id=$1 AND deletion_requested_at IS NOT NULL`, id).Scan(&n))
	return n > 0
}

func isQuarantined(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) bool {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM resources WHERE id=$1 AND frozen_until = 'infinity'`, id).Scan(&n))
	return n > 0
}
