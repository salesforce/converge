package test

// TestDevRun is the "just dev-inproc" entrypoint: it spins up Postgres in a
// testcontainer (the same way integration tests do), boots the
// Converge (control plane + worker node + HTTP API + embedded
// UI), optionally submits a ClassicBOM file, and blocks until Ctrl+C.
//
// Why a test instead of a separate binary? Two reasons:
//   - The testcontainers + pgxpool wiring is already proven correct
//     here; reusing it removes a class of "works in dev but not in
//     test" drift.
//   - We can lean on `go test`'s stdin/SIGINT handling for clean
//     shutdown (Postgres container terminates via t.Cleanup).
//
// It's gated behind -dev so a vanilla `go test ./test/` doesn't
// hang waiting on Ctrl+C. Run via `just dev-inproc`.
//
// Flags:
//   -dev          enable this test (default: skip)
//   -bom <path>   path to a ClassicBOM JSON to auto-submit at boot
//   -addr :port   HTTP listen address (default :18080)
//   -fake-delay   simulated work latency per resource (default 200ms)
//
// On exit (Ctrl+C), the testcontainer is torn down via t.Cleanup, so
// the next `just dev-inproc` starts from an empty database. If you want to
// keep state between runs, use docker directly.

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/fault"
	"github.com/salesforce/converge/examples/demos/classic/networking"
	"github.com/salesforce/converge/internal/api"
	"github.com/salesforce/converge/internal/engine"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/test/internal/demoruntime"
)

var (
	devEnabled  = flag.Bool("dev", false, "Run the long-running dev Converge (used by `just dev-inproc`)")
	devBOM      = flag.String("bom", "", "Path to a ClassicBOM JSON file to auto-submit at boot")
	devAddr     = flag.String("addr", ":18080", "HTTP listen address")
	devFakeWork = flag.Duration("fake-delay", 200*time.Millisecond, "Simulated worker latency")
)

func TestDevRun(t *testing.T) {
	if !*devEnabled {
		t.Skip("dev runner disabled; run via `just dev-inproc` (sets -dev)")
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	// SIGINT/SIGTERM cancels everything. testing.T.Cleanup teardown
	// happens after this returns, taking the testcontainer with it.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool := setupPostgres(t, ctx)

	reg := newTReg()
	reg.Add(demoruntime.ClassicBOM(0))
	reg.Add(demoruntime.Account(*devFakeWork, 0, fault.Injector{}))
	reg.Add(networking.AllRuntimes(*devFakeWork, fault.Injector{})...)

	// Migrate + seed the manifests (which derive kind_config via the manifest
	// trigger) BEFORE the control plane reads kind_config at construction.
	// startInProcWorker re-seeds idempotently when it starts the worker.
	migrateForSeed(t, ctx, pool)
	require.NoError(t, reg.seed(ctx, pool))
	cp, err := engine.NewControlPlane(ctx, &engine.ControlPlaneConfig{
		Pool: pool,
	})
	require.NoError(t, err)
	require.NoError(t, cp.Start(ctx))
	wn := startInProcWorker(t, ctx, pool, reg, nil, 0, nil)

	srv := api.NewServer(api.DepsFromPools(pool, nil))
	srv.SetDeclaredSchemas(declaredManifests(reg))
	httpServer := &http.Server{Addr: *devAddr, Handler: srv.Handler()}

	go func() {
		slog.Info("api+ui listening", "url", "http://localhost"+*devAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server error", "error", err)
		}
	}()

	// Optionally auto-submit a ClassicBOM. Useful for "just dev-inproc-bom" so
	// the UI immediately has data to render.
	if *devBOM != "" {
		go func() {
			// Wait briefly for HTTP server to come up before submitting.
			time.Sleep(500 * time.Millisecond)
			if err := submitManifestFromFile(ctx, pool, *devBOM); err != nil {
				slog.Error("auto-submit failed", "path", *devBOM, "err", err)
				return
			}
			slog.Info("auto-submitted manifest", "path", *devBOM)
		}()
	}

	slog.Info("converge running — Ctrl+C to stop")
	<-ctx.Done()

	slog.Info("shutting down")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
	_ = wn.Stop(context.Background())
	_ = cp.Stop(context.Background())
}

// submitManifestFromFile reads a ResourceManifest JSON file ({kind,
// name, labels, spec}) and upserts the root it describes. The kind,
// name, and labels come from the manifest itself, so the same path
// handles any kind (a classicbom BOM or any demo kind)
// without the caller naming the kind. Equivalent to applying the file
// through POST /api/resources; goes straight through the store (the
// real mutation gateway).
func submitManifestFromFile(ctx context.Context, pool *pgxpool.Pool, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	var m model.ResourceManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("parse manifest: %w", err)
	}
	res, err := store.New(pool).ApplySpec(ctx, m.Kind, m.Name, m.Spec, m.Labels)
	if err != nil {
		return fmt.Errorf("submit manifest: %w", err)
	}
	slog.Info("manifest applied", "id", idShort(res.ID), "kind", m.Kind, "name", m.Name)
	return nil
}

func idShort(id uuid.UUID) string {
	s := id.String()
	if len(s) <= 8 {
		return s
	}
	return s[:8]
}

// declaredManifests collects the registry's kind MANIFESTS for the API server's
// SetDeclaredSchemas: the API validates submitted specs against these. Built from
// the tReg's public kinds()/manifest() accessors so it touches no harness internals.
func declaredManifests(reg *tReg) []model.KindManifest {
	return reg.manifestList()
}
