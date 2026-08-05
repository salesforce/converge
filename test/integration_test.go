// Package test holds Converge's integration tests. This file is an
// end-to-end test against the provider model.
package test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

func setupPostgres(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("orchestrator_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pgContainer.Terminate(context.Background()) })

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	// Explicit MaxConns, CPU-INDEPENDENT. pgxpool's default is max(4, runtime.NumCPU()),
	// which on a ≤4-CPU host (a shared CI runner) yields only 4 connections — fewer than
	// this all-roles-in-one-process fleet's long-lived LISTEN subscribers (drainer
	// outbox_ready + rollup_recheck, reactor lifecycle_ready, dispatcher work_ready,
	// mesh/topology cluster_changed, …), each of which PINS a connection for its lifetime.
	// With only 4, the LISTENers exhaust the pool and every other loop blocks forever in
	// pgxpool.Acquire (a silent wedge, no error). 25 covers the LISTENers plus working
	// headroom regardless of the runner's CPU count. (The chaos/HA/replica suites set
	// their own larger caps for the same reason.)
	poolCfg, err := pgxpool.ParseConfig(connStr)
	require.NoError(t, err)
	poolCfg.MaxConns = 25
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })

	return pool
}

// TestEndToEndClassicBOM creates a ClassicBOM root resource and waits for
// every composed child to reach is_ready=true. Validates: the ClassicBOM
// Composer ran, children were created with the right kinds, the
// dispatch loop ran each Worker, the cascade triggers fired correctly
// across the dep graph, and observed_generation propagated.
func TestEndToEndClassicBOM(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	eng := startAllRolesEngine(t, ctx, pool)
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	// Small BOM: one FD with three teams. Expected children:
	//   1 fd-tgw account + 1 TGW + 3 team accounts + 3 VPCs + 3 routes = 11
	bom := smallBOM("project-a", map[string][]string{
		"alpha-fd": {"team-alpha", "team-beta", "team-gamma"},
	})
	specBytes, err := json.Marshal(bom)
	require.NoError(t, err)

	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "project-a", specBytes, nil)
	require.NoError(t, err)

	const expectedChildren = 11
	q := dbq.New(pool)

	deadline := time.Now().Add(30 * time.Second)
	settled := false
	for time.Now().Before(deadline) {
		root, err := q.GetResourceInfo(ctx, rootID)
		if err == nil && root.IsReady {
			children, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: pgtype.UUID{Bytes: rootID, Valid: true}, Limit: 1000000})
			if err == nil && len(children) == expectedChildren {
				allReady := true
				for _, c := range children {
					if !c.IsReady {
						allReady = false
						break
					}
				}
				if allReady {
					settled = true
					break
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !settled {
		dumpResourceState(t, ctx, pool, rootID)
		t.Fatalf("all resources should reach ready")
	}

	// Verify final shape.
	root, err := q.GetResourceInfo(ctx, rootID)
	require.NoError(t, err)
	require.True(t, root.IsReady)
	require.GreaterOrEqual(t, root.SyncedGen, int64(1), "root observed_generation should be set")

	// The classicbom root has a StatusRollup; reaching Ready required the
	// rollup stage to run + advance synced_gen, so a rollup-succeeded
	// event must be in its feed (distinct from compose-succeeded).
	evs, err := store.New(pool).ListEvents(ctx, rootID, time.Time{}, 0, 100)
	require.NoError(t, err)
	var rollups, composes int
	for _, e := range evs {
		switch e.Type {
		case "rollup-succeeded":
			rollups++
		case "compose-succeeded":
			composes++
		}
	}
	require.GreaterOrEqual(t, rollups, 1, "classicbom root should emit at least one rollup-succeeded event")
	require.GreaterOrEqual(t, composes, 1, "classicbom root should emit at least one compose-succeeded event")

	t.Logf("SUCCESS: ClassicBOM root + %d children all ready (compose=%d rollup=%d events)",
		expectedChildren, composes, rollups)
}
