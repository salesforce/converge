package test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/model"
)

// TestRollupLatencyReactive asserts that a ClassicBOM root's rollup fires
// REACTIVELY — within one drain cycle of its last descendant syncing —
// rather than waiting for the reaper's poll + RetryAfter age gate.
//
// This guards the cascade_on_ready_change change that schedules the
// rollup-having root through schedule_eligible the moment its final
// descendant catches up (synced_gen >= generation). Before that change,
// the owner was deliberately excluded from the cascade and only the
// time-driven reaper re-pended it, which added several seconds of pure
// polling latency between "all children ready" and "root ready".
//
// The assertion is on the GAP between last-child-ready and root-ready,
// not absolute time, so it's insensitive to how long the children
// themselves take. RetryAfter is set deliberately high (30s) so that if
// the reactive cascade path regressed, the reaper could NOT mask it
// within the test window — the test would have to wait ~30s and time
// out. With the reactive path, the gap is sub-second.
func TestRollupLatencyReactive(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	// RetryAfter=30s: the reaper backstop is intentionally pushed out of
	// the measurement window so the only way the root can become ready
	// quickly is the reactive cascade. If the cascade path breaks, the
	// root stays not-ready until the 30s gate opens and the test's
	// per-step deadlines fail loudly.
	eng := startEngineWithConfig(t, ctx, pool, defaultRegistry(), engineOpts{
		RetryAfter: 30 * time.Second,
	})
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	// One FD, three teams → 1 fd-tgw account + 1 TGW + 3 team accounts +
	// 3 VPCs + 3 routes = 11 children.
	bom := smallBOM("project-lat", map[string][]string{
		"alpha-fd": {"team-alpha", "team-beta", "team-gamma"},
	})
	specBytes, err := json.Marshal(bom)
	require.NoError(t, err)

	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "project-lat", specBytes, nil)
	require.NoError(t, err)

	const expectedChildren = 11
	q := dbq.New(pool)
	ownerArg := pgtype.UUID{Bytes: rootID, Valid: true}

	// Phase 1: wait until every CHILD is ready (root may or may not be
	// ready yet). Record the instant the last child flipped.
	var lastChildReadyAt time.Time
	childDeadline := time.Now().Add(30 * time.Second)
	for {
		require.False(t, time.Now().After(childDeadline),
			"timed out waiting for all %d children to become ready", expectedChildren)

		children, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: ownerArg, Limit: 1000000})
		require.NoError(t, err)
		if len(children) == expectedChildren {
			allReady := true
			for _, c := range children {
				if !c.IsReady {
					allReady = false
					break
				}
			}
			if allReady {
				lastChildReadyAt = time.Now()
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Phase 2: from the moment all children are ready, the root must flip
	// ready reactively. Allow a generous 5s ceiling — the reactive path
	// is sub-second; this is well under the 30s reaper RetryAfter, so a
	// pass proves the cascade (not the reaper) scheduled the rollup.
	const reactiveCeiling = 5 * time.Second
	rootDeadline := lastChildReadyAt.Add(reactiveCeiling)
	var rootReadyAt time.Time
	for {
		root, err := q.GetResourceInfo(ctx, rootID)
		require.NoError(t, err)
		if root.IsReady {
			rootReadyAt = time.Now()
			break
		}
		if time.Now().After(rootDeadline) {
			dumpResourceState(t, ctx, pool, rootID)
			t.Fatalf("root did not become ready within %s of its last child "+
				"(reactive rollup regressed — reaper RetryAfter is 30s so this "+
				"is NOT a reaper-masked pass)", reactiveCeiling)
		}
		time.Sleep(20 * time.Millisecond)
	}

	gap := rootReadyAt.Sub(lastChildReadyAt)
	t.Logf("reactive rollup latency: root ready %s after last child ready", gap)
	require.Less(t, gap, reactiveCeiling,
		"root-ready gap %s should be well under the reaper RetryAfter (30s)", gap)
}
