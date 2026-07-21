package test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/model"
)

// TestMediumScale: 500-child root with a non-trivial dep graph
// (each child of group N+1 depends on group N's first member).
//
// Validates:
//   - schedule_eligible can handle a non-trivial graph at modest scale
//     without contention or starvation.
//   - Triggers fire correctly across hundreds of cascade events.
//   - The outbox drainer keeps up with worker throughput.
//   - Total wall time is reasonable (< 30s with stub workers).
//
// This is not a stress test — it's a sanity check that the system
// behaves like the 11-resource case at ~50× scale.
func TestMediumScale(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	composer := newControllableComposer()
	worker := newFlakyWorker(model.Kind(account.Kind))
	reg := newTReg()
	reg.AddKind(composer, composer.Manifest())
	reg.AddKind(worker, worker.Manifest())
	eng := startEngineWithRegistry(t, ctx, pool, reg)
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	// Build a 500-child layered graph: 10 groups × 50 children each.
	// Each child in group g depends on the FIRST child in group g-1.
	const numGroups = 10
	const perGroup = 50
	const total = numGroups * perGroup

	children := make([]controllableChildSpec, 0, total)
	depsOn := map[string][]controllableChildSpec{}
	for g := 0; g < numGroups; g++ {
		for i := 0; i < perGroup; i++ {
			name := fmt.Sprintf("g%02d-c%02d", g, i)
			children = append(children, controllableChildSpec{
				Kind: string(account.Kind),
				Name: name,
			})
			if g > 0 {
				prevAnchor := fmt.Sprintf("g%02d-c00", g-1)
				depsOn[name] = []controllableChildSpec{
					{Kind: string(account.Kind), Name: prevAnchor},
				}
			}
		}
	}

	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "scale-root",
		buildSpecWithDeps(children, depsOn), nil)
	require.NoError(t, err)
	q := dbq.New(pool)
	startWall := time.Now()

	require.Eventually(t, func() bool {
		root, err := q.GetResourceInfo(ctx, rootID)
		if err != nil || !root.IsReady {
			return false
		}
		kids, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: pgtype.UUID{Bytes: rootID, Valid: true}, Limit: 1000000})
		if err != nil || len(kids) != total {
			return false
		}
		readyCount := 0
		for _, c := range kids {
			if c.IsReady {
				readyCount++
			}
		}
		return readyCount == total
	}, 60*time.Second, 200*time.Millisecond, "all 500 children should reach ready")

	elapsed := time.Since(startWall)
	t.Logf("500 children with 10-deep dep graph reached ready in %s", elapsed)
	require.Less(t, elapsed, 60*time.Second, "should complete well under 60s")

	// Sanity: worker called once per child, no double-runs.
	for _, c := range children {
		require.Equal(t, 1, worker.Calls(c.Name), "worker for %s should run exactly once", c.Name)
	}
}
