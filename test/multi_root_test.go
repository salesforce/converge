package test

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/sdk-go/converge"
)

// TestConcurrentRootsIndependent submits two roots concurrently, each
// composing its own child set. Resource names are GLOBALLY unique (one
// UNIQUE (kind, name) index), so — exactly as real composers do (ClassicBOM
// prefixes by deployment_instance) — this test's composer prefixes each
// child name with the root name, giving disjoint child sets per root.
//
// Both roots reach ready independently. Verifies that two roots'
// fan-outs coexist under the global unique index and that the work queue
// interleaves their work without deadlock or starvation.
func TestConcurrentRootsIndependent(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	composer := newControllableComposer()
	// Prefix child names with the root name so the two roots emit disjoint,
	// globally-unique names (the model real composers follow). Without the
	// prefix, both roots would emit "a".."e" and collide on UNIQUE(kind,name).
	composer.produce = func(rootName string, spec controllableSpec) []converge.ChildSpec {
		out := make([]converge.ChildSpec, 0, len(spec.Children))
		for _, ch := range spec.Children {
			name := rootName + "/" + ch.Name
			out = append(out, converge.ChildSpec{
				Kind:        converge.Kind(ch.Kind),
				KindVersion: 1,
				Name:        name,
				Spec:        map[string]string{"name": name},
				Labels:      map[string]string{"composed_by": "controllable"},
			})
		}
		return out
	}
	worker := newFlakyWorker(model.Kind(account.Kind))
	reg := newTReg()
	reg.AddKind(composer, composer.Manifest())
	reg.AddKind(worker, worker.Manifest())
	eng := startEngineWithRegistry(t, ctx, pool, reg)
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	// Two roots, each declaring the same 5 logical child names (a..e). The
	// composer namespaces them per root → 10 distinct rows total.
	spec := buildSpec(
		controllableChildSpec{Kind: string(account.Kind), Name: "a"},
		controllableChildSpec{Kind: string(account.Kind), Name: "b"},
		controllableChildSpec{Kind: string(account.Kind), Name: "c"},
		controllableChildSpec{Kind: string(account.Kind), Name: "d"},
		controllableChildSpec{Kind: string(account.Kind), Name: "e"},
	)

	var wg sync.WaitGroup
	var rootA, rootB struct {
		id  pgtype.UUID
		err error
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		id, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "concurrent-A", spec, nil)
		rootA.id = pgtype.UUID{Bytes: id, Valid: true}
		rootA.err = err
	}()
	go func() {
		defer wg.Done()
		id, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "concurrent-B", spec, nil)
		rootB.id = pgtype.UUID{Bytes: id, Valid: true}
		rootB.err = err
	}()
	wg.Wait()
	require.NoError(t, rootA.err)
	require.NoError(t, rootB.err)

	q := dbq.New(pool)

	// Both roots and all their children reach ready.
	require.Eventually(t, func() bool {
		for _, parentID := range []pgtype.UUID{rootA.id, rootB.id} {
			rootRow, err := q.GetResourceInfo(ctx, parentID.Bytes)
			if err != nil || !rootRow.IsReady {
				return false
			}
			children, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: parentID, Limit: 1000000})
			if err != nil || len(children) != 5 {
				return false
			}
			for _, c := range children {
				if !c.IsReady {
					return false
				}
			}
		}
		return true
	}, 20*time.Second, 100*time.Millisecond, "both roots' children ready")

	// Each root owns exactly its own 5 namespaced children; no name
	// repeats within an owner, and the two sets are disjoint globally.
	for prefix, parentID := range map[string]pgtype.UUID{"concurrent-A": rootA.id, "concurrent-B": rootB.id} {
		children, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: parentID, Limit: 1000000})
		require.NoError(t, err)
		seen := map[string]bool{}
		for _, c := range children {
			require.False(t, seen[c.Name], "duplicate child name %s under one owner", c.Name)
			require.Equal(t, prefix+"/", c.Name[:len(prefix)+1], "child %s must be namespaced by its root", c.Name)
			seen[c.Name] = true
		}
	}

	// Each namespaced name is globally unique → worker ran exactly once per name.
	for _, root := range []string{"concurrent-A", "concurrent-B"} {
		for _, n := range []string{"a", "b", "c", "d", "e"} {
			name := root + "/" + n
			require.Equal(t, 1, worker.Calls(name),
				"globally-unique name %s should have run exactly once", name)
		}
	}

	t.Log("two concurrent roots reconciled independently under global name uniqueness")
}
