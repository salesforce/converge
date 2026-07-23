// Package test holds Converge's integration tests. This file is an
// end-to-end test against the provider model.
package test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"runtime"
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

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })

	// DIAGNOSTIC (env-gated, off by default): probe the connection-pool exhaustion
	// hypothesis. pgxpool's default MaxConns is max(4, runtime.NumCPU()), so a 2-vCPU
	// CI runner gets only 4 connections shared across every engine loop — a suspected
	// silent wedge (a background sweeper blocks forever in pgxpool.Acquire with no error
	// logged). Set POOL_DEBUG=1 to log the sizing once + live pool.Stat() every 3s so
	// the fill-up (AcquiredConns==MaxConns with waiters) is visible in the run log.
	if os.Getenv("POOL_DEBUG") == "1" {
		slog.Warn("POOL_DEBUG: connection pool sizing",
			"max_conns", pool.Config().MaxConns, "num_cpu", runtime.NumCPU())
		go func() {
			tk := time.NewTicker(3 * time.Second)
			defer tk.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tk.C:
					s := pool.Stat()
					slog.Warn("POOL_DEBUG: pool.Stat",
						"acquired", s.AcquiredConns(), "idle", s.IdleConns(),
						"total", s.TotalConns(), "max", s.MaxConns(),
						"acquire_waiting", s.EmptyAcquireCount(), "canceled_acquire", s.CanceledAcquireCount())
				}
			}
		}()
	}

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
