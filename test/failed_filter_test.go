package test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/model"
)

// TestFailedFilterReturnsFailedResources validates the `phase` readiness
// filter end-to-end at the store layer: a resource whose reconcile hard-
// failed (drainer stamped failure_gen = generation → phase='Failed') must
// be returned by phase='Failed', and a healthy sibling (phase='Ready')
// must be excluded. This is the backend behind the UI's "failed" chip
// (?phase=Failed → ListResourcesPage).
//
// A "doomed" child whose worker never succeeds settles into phase='Failed'
// (failure_gen = generation). A healthy sibling is phase='Ready'.
func TestFailedFilterReturnsFailedResources(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	composer := newControllableComposer()
	worker := newFlakyWorker(model.Kind(account.Kind))
	worker.SetFailsBefore("doomed", 1000) // never succeeds within the test
	reg := newTReg()
	reg.AddKind(composer, composer.Manifest())
	reg.AddKind(worker, worker.Manifest())
	eng := startEngineWithRegistry(t, ctx, pool, reg)
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	// One healthy child + one doomed child under the same root.
	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "filter-root",
		buildSpec(
			controllableChildSpec{Kind: string(account.Kind), Name: "healthy"},
			controllableChildSpec{Kind: string(account.Kind), Name: "doomed"},
		), nil)
	require.NoError(t, err)

	q := dbq.New(pool)
	ownerArg := pgtype.UUID{Bytes: rootID, Valid: true}

	// Wait until: healthy child phase=Ready, doomed child phase=Failed.
	require.Eventually(t, func() bool {
		children, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: ownerArg, Limit: 1000000})
		if err != nil || len(children) != 2 {
			return false
		}
		var healthyReady, doomedFailed bool
		for _, c := range children {
			switch c.Name {
			case "healthy":
				healthyReady = c.Phase == "Ready"
			case "doomed":
				doomedFailed = c.Phase == "Failed"
			}
		}
		return healthyReady && doomedFailed
	}, 25*time.Second, 200*time.Millisecond, "healthy child phase=Ready + doomed child phase=Failed")

	// phase=Failed → exactly the doomed child. The page filter is a SET
	// (phase = ANY(phases)); a single-element set is the one-chip case.
	failedRows, err := q.ListResourcesPage(ctx, dbq.ListResourcesPageParams{
		OwnerIds: []uuid.UUID{rootID},
		Phases:   []string{"Failed"},
		Limit:    100,
	})
	require.NoError(t, err)
	require.Len(t, failedRows, 1, "phase=Failed must return exactly the doomed child")
	require.Equal(t, "doomed", failedRows[0].Name)
	require.Equal(t, "Failed", failedRows[0].Phase)
	require.False(t, failedRows[0].IsReady)

	// The healthy child must NOT appear under phase=Failed.
	for _, r := range failedRows {
		require.NotEqual(t, "healthy", r.Name, "healthy child must be excluded by phase=Failed")
	}

	// Count variant must agree with the page.
	failedCount, err := q.CountResourcesFiltered(ctx, dbq.CountResourcesFilteredParams{
		OwnerIds: []uuid.UUID{rootID},
		Phases:   []string{"Failed"},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), failedCount, "phase=Failed count must match the page (1 doomed child)")

	// Multi-phase UNION: selecting {Ready, Failed} must return BOTH children
	// (the union of the two phase buckets), not a sliver. This is the
	// backend behind the UI's multi-select phase chips — the page is the
	// union, computed server-side. Count must agree.
	unionRows, err := q.ListResourcesPage(ctx, dbq.ListResourcesPageParams{
		OwnerIds: []uuid.UUID{rootID},
		Phases:   []string{"Ready", "Failed"},
		Limit:    100,
	})
	require.NoError(t, err)
	require.Len(t, unionRows, 2, "phase IN {Ready,Failed} must return both children (the union)")
	unionCount, err := q.CountResourcesFiltered(ctx, dbq.CountResourcesFilteredParams{
		OwnerIds: []uuid.UUID{rootID},
		Phases:   []string{"Ready", "Failed"},
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), unionCount, "phase IN {Ready,Failed} count must match the union page")

	// Sanity: unfiltered (no phases) returns both children, so the filter
	// genuinely narrowed rather than the query being empty for some
	// unrelated reason.
	allRows, err := q.ListResourcesPage(ctx, dbq.ListResourcesPageParams{
		OwnerIds: []uuid.UUID{rootID},
		Limit:    100,
	})
	require.NoError(t, err)
	require.Len(t, allRows, 2, "unfiltered page should return both children")

	t.Logf("failed_only isolated the doomed child (1 of 2); {Ready,Failed} unioned both")
}

// TestRootsFailedFilter validates the `phase` filter on the ROOTS page.
// Unified semantics: a resource is "failed" iff phase='Failed', i.e.
// failure_gen = generation (its OWN reconcile hard-failed for the live
// spec). This is the single scalar definition the DB/API/UI all share —
// no more "NOT is_ready AND ∃ False condition" inference.
//
// We seed the rows directly so the test is deterministic and exercises
// exactly the phase classification, independent of provider timing:
//   - failedRoot:  failure_gen = generation → phase='Failed' → matches.
//   - readyRoot:   synced + healthy → phase='Ready' → excluded.
//   - stuckRoot:   gen ahead of synced, failure_gen=0 → phase='Reconciling'
//     → excluded (it's progressing, not failed).
func TestRootsFailedFilter(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	// Start the engine purely for its migration side-effect (schema
	// creation). We don't submit any roots through it — the rows are
	// seeded directly so the failed_only predicate is tested in isolation.
	eng := startEngineWithRegistry(t, ctx, pool, defaultRegistry())
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	q := dbq.New(pool)

	// readyRoot: synced and healthy.
	readyRow, err := q.UpsertResource(ctx, dbq.UpsertResourceParams{
		ID: uuid.New(), Kind: "failtest", KindVersion: 1, Name: "ready-root", Spec: []byte(`{}`), Labels: []byte(`{}`),
	})
	require.NoError(t, err)
	readyRoot := readyRow.ID
	_, err = pool.Exec(ctx, `UPDATE resources SET synced_gen = generation WHERE id = $1`, readyRoot)
	require.NoError(t, err)

	// failedRoot: its own reconcile hard-failed → failure_gen = generation.
	// (We also store the Synced=False condition the drainer would have
	// written, to confirm it's carried as the message but doesn't drive
	// classification.)
	failedRow, err := q.UpsertResource(ctx, dbq.UpsertResourceParams{
		ID: uuid.New(), Kind: "failtest", KindVersion: 1, Name: "failed-root", Spec: []byte(`{}`), Labels: []byte(`{}`),
	})
	require.NoError(t, err)
	failedRoot := failedRow.ID
	_, err = pool.Exec(ctx, `UPDATE resources SET generation = 2, synced_gen = 1, failure_gen = 2 WHERE id = $1`, failedRoot)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO resource_conditions (resource_id, type, status, reason, message, observed_generation)
		VALUES ($1, 'Synced', 'False', 'ReconcileError', 'composer failed', 2)`, failedRoot)
	require.NoError(t, err)

	// stuckRoot: gen ahead of synced but failure_gen=0 → phase='Reconciling',
	// must be excluded (it's still progressing, not failed).
	stuckRow, err := q.UpsertResource(ctx, dbq.UpsertResourceParams{
		ID: uuid.New(), Kind: "failtest", KindVersion: 1, Name: "stuck-root", Spec: []byte(`{}`), Labels: []byte(`{}`),
	})
	require.NoError(t, err)
	stuckRoot := stuckRow.ID
	_, err = pool.Exec(ctx, `UPDATE resources SET generation = 2, synced_gen = 1 WHERE id = $1`, stuckRoot)
	require.NoError(t, err)

	// Sanity: confirm the seeded is_ready states.
	rr, err := q.GetResourceInfo(ctx, readyRoot)
	require.NoError(t, err)
	require.True(t, rr.IsReady, "ready-root should be is_ready")
	fr, err := q.GetResourceInfo(ctx, failedRoot)
	require.NoError(t, err)
	require.False(t, fr.IsReady, "failed-root should be not-ready")

	failedPhase := "Failed"
	roots, err := q.ListRootResourcesPage(ctx, dbq.ListRootResourcesPageParams{
		Phase: &failedPhase,
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, roots, 1, "roots phase=Failed must return exactly the failed root")
	require.Equal(t, "failed-root", roots[0].Name)
	require.Equal(t, "Failed", roots[0].Phase)
	require.False(t, roots[0].IsReady)

	cnt, err := q.CountRootResourcesFiltered(ctx, dbq.CountRootResourcesFilteredParams{
		Phase: &failedPhase,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), cnt, "roots phase=Failed count must match the page")

	// Without the filter, all three roots are returned — proving the
	// filter genuinely narrowed rather than the query being empty.
	all, err := q.ListRootResourcesPage(ctx, dbq.ListRootResourcesPageParams{Limit: 100})
	require.NoError(t, err)
	require.Len(t, all, 3, "unfiltered roots page should return all three roots")

	t.Logf("roots failed_only correctly isolated the failed root (own False condition)")
}

// TestRootsKindVersionPairFilter proves the (kind, kind_version) PAIR filter behind
// the Resources-page kind/version chips: seed roots at vpc/v1, vpc/v2, account/v1,
// account/v2, then assert the kv_kinds/kv_pairs filter returns EXACTLY the requested
// pairs — including a cross-kind mix (vpc/v1 + account/v2) — and that the count
// matches the page.
func TestRootsKindVersionPairFilter(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	eng := startEngineWithRegistry(t, ctx, pool, defaultRegistry())
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	q := dbq.New(pool)
	seed := func(kind string, kv int32, name string) {
		_, err := q.UpsertResource(ctx, dbq.UpsertResourceParams{
			ID: uuid.New(), Kind: kind, KindVersion: kv, Name: name, Spec: []byte(`{}`), Labels: []byte(`{}`),
		})
		require.NoError(t, err)
	}
	seed("vpc", 1, "vpc-a")
	seed("vpc", 2, "vpc-b")
	seed("account", 1, "acc-a")
	seed("account", 2, "acc-b")

	names := func(rows []dbq.ListRootResourcesPageRow) []string {
		out := make([]string, len(rows))
		for i, r := range rows {
			out[i] = r.Name
		}
		return out
	}

	// Single pair: vpc/v1 only.
	rows, err := q.ListRootResourcesPage(ctx, dbq.ListRootResourcesPageParams{
		KvKinds: []string{"vpc"}, KvPairs: []string{"vpc:1"}, Limit: 100,
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"vpc-a"}, names(rows), "vpc/v1 pair returns only vpc-a")

	// Two versions of one kind: vpc/v1 + vpc/v2.
	rows, err = q.ListRootResourcesPage(ctx, dbq.ListRootResourcesPageParams{
		KvKinds: []string{"vpc"}, KvPairs: []string{"vpc:1", "vpc:2"}, Limit: 100,
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"vpc-a", "vpc-b"}, names(rows), "both vpc versions")

	// CROSS-KIND, DIFFERENT VERSIONS — the case the single-version filter could not
	// express: vpc/v1 + account/v2. Must return vpc-a and acc-b only (NOT vpc-b or
	// acc-a).
	rows, err = q.ListRootResourcesPage(ctx, dbq.ListRootResourcesPageParams{
		KvKinds: []string{"vpc", "account"}, KvPairs: []string{"vpc:1", "account:2"}, Limit: 100,
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"vpc-a", "acc-b"}, names(rows),
		"cross-kind mixed-version pair filter returns exactly vpc/v1 + account/v2")

	// The count mirrors the page for the mixed filter.
	cnt, err := q.CountRootResourcesFiltered(ctx, dbq.CountRootResourcesFilteredParams{
		KvKinds: []string{"vpc", "account"}, KvPairs: []string{"vpc:1", "account:2"},
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), cnt, "count matches the mixed pair-filter page")

	// No pair filter → all four roots (proves the filter narrowed, not emptied).
	all, err := q.ListRootResourcesPage(ctx, dbq.ListRootResourcesPageParams{Limit: 100})
	require.NoError(t, err)
	require.Len(t, all, 4, "unfiltered page returns all four roots")

	t.Logf("(kind, kind_version) pair filter isolates exact pairs incl. cross-kind mixed versions")
}
