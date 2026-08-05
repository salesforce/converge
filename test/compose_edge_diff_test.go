package test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// TestComposeEdgeDiff exercises the edge half of the compose diff at the
// store layer (deterministic, no engine timing): a recompose must
//   - UPSERT an edge whose value_flows changed (added/removed a flow), and
//   - PRUNE an edge the new compose no longer emits,
//
// mirroring the child diff, so no stale edge or stale value_flow survives a
// composer/code change.
func TestComposeEdgeDiff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	// startAllRolesEngine is only used here for its migration side-effect
	// (schema creation); we drive ApplyComposeResult directly.
	eng := startAllRolesEngine(t, ctx, pool)
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	repo := store.New(pool)

	// Seed a composite root to own the children.
	rootID, err := eng.CreateRoot(ctx, "compose-edge-root-kind", "edge-root",
		json.RawMessage(`{}`), nil)
	require.NoError(t, err)

	ref := func(name string) model.ResourceRef {
		return model.ResourceRef{Kind: "leaf", Name: name}
	}
	child := func(name string) model.ChildSpec {
		return model.ChildSpec{Kind: "leaf", KindVersion: 1, Name: name, Spec: map[string]string{"name": name}}
	}
	children := []model.ChildSpec{child("a"), child("b"), child("c")}

	// apply runs one ApplyComposeResult in its own tx (mirrors the worker).
	// ApplyComposeResult is FENCED on the claim's claim_epoch at gen — claim the
	// root first, then pass its work_id + the row's claim_epoch. fenceClaimRoot
	// leaves the freshly-enqueued row at the default epoch 1, so the fence matches
	// on claimEpoch=1 (manifest 0 = inert).
	const composeWorker = "compose-edge-test-worker"
	apply := func(gen int64, edges []model.DepEdge) store.ComposeCounts {
		wq := fenceClaimRoot(t, ctx, pool, rootID, composeWorker, gen)
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		counts, err := repo.WithTx(tx).ApplyComposeResult(ctx, rootID, gen, wq, 1, 0, children, edges, nil, nil)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		return counts
	}

	// depFlows returns the value_flows JSON for the a→b edge (or "" if the
	// edge is gone), straight from resource_deps.
	depFlows := func() string {
		deps, err := repo.ListExistingDepsByOwner(ctx, rootID)
		require.NoError(t, err)
		// resolve a's + b's ids
		var aID, bID uuid.UUID
		rows, err := pool.Query(ctx, `SELECT r.id, m.name FROM resources r JOIN resource_meta m ON m.id = r.id WHERE r.owner_id=$1`, rootID)
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			var id uuid.UUID
			var name string
			require.NoError(t, rows.Scan(&id, &name))
			if name == "a" {
				aID = id
			} else if name == "b" {
				bID = id
			}
		}
		for _, d := range deps {
			if d.DependentID == aID && d.DependencyID == bID {
				return string(d.ValueFlows)
			}
		}
		return ""
	}

	// ── gen 1: a→b with one flow (/x) → CREATED, not updated ──
	c1 := apply(1, []model.DepEdge{{
		From: ref("a"), To: ref("b"),
		Values: []model.ValueFlow{{DependentField: "/x", SourceField: "/x"}},
	}})
	require.JSONEq(t, `[{"dep_field":"/x","src_field":"/x"}]`, depFlows(),
		"initial edge a→b has one flow")
	require.Equal(t, 1, c1.EdgesCreated, "new edge counts as created")
	require.Equal(t, 0, c1.EdgesUpdated, "new edge is not an update")

	// ── gen 2: a→b now has TWO flows (added /y) → must UPSERT the edge,
	// and count EXACTLY 1 upsert (the changed edge), not "produced". ──
	c2 := apply(2, []model.DepEdge{{
		From: ref("a"), To: ref("b"),
		Values: []model.ValueFlow{
			{DependentField: "/x", SourceField: "/x"},
			{DependentField: "/y", SourceField: "/y"},
		},
	}})
	require.JSONEq(t,
		`[{"dep_field":"/x","src_field":"/x"},{"dep_field":"/y","src_field":"/y"}]`,
		depFlows(), "changed flows must be upserted, not ignored (was DO NOTHING)")
	require.Equal(t, 1, c2.Edges, "exactly the one changed edge counted as upserted")
	require.Equal(t, 0, c2.EdgesCreated, "edge already existed → not created")
	require.Equal(t, 1, c2.EdgesUpdated, "flow change counts as updated")
	require.Equal(t, 0, c2.EdgesDeleted, "no prune when the edge still exists")

	// ── gen 2b: re-apply the SAME two flows → 0 upserts (minimal diff) ──
	c2b := apply(2, []model.DepEdge{{
		From: ref("a"), To: ref("b"),
		Values: []model.ValueFlow{
			{DependentField: "/x", SourceField: "/x"},
			{DependentField: "/y", SourceField: "/y"},
		},
	}})
	require.Equal(t, 0, c2b.Edges, "unchanged edge must NOT be re-upserted (was: all produced)")
	require.Equal(t, 0, c2b.EdgesDeleted, "unchanged edge must not be pruned")

	// ── gen 3: compose emits NO edges → a→b must be PRUNED ──
	c3 := apply(3, nil)
	require.Equal(t, "", depFlows(), "edge a→b must be pruned when no longer emitted")
	require.Equal(t, 0, c3.Edges, "nothing upserted when no edges produced")
	require.Equal(t, 1, c3.EdgesDeleted, "exactly the one stale edge pruned")

	// Children themselves survive (the edge prune must not touch rows).
	kids, err := repo.GetChildrenByOwner(ctx, rootID, 1000000)
	require.NoError(t, err)
	require.Len(t, kids, 3, "pruning an edge must not delete the children")

	t.Logf("edge diff: flow-change upserted, stale edge pruned, children intact")
}
