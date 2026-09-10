package test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/internal/model"
)

// TestContinuousFillSlowTaskDoesNotBlockSlots verifies the worker dispatcher's
// continuous-fill behavior: one slow (hung) task must occupy only its
// own slot while the dispatcher keeps refilling the other slots as their
// tasks finish — a hung task never pins a whole cohort. Here we submit far
// more children than MaxParallel and assert the overwhelming majority reach
// ready WHILE the hung one blocks.
func TestContinuousFillSlowTaskDoesNotBlockSlots(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	composer := newControllableComposer()
	worker := newFlakyWorker(model.Kind(account.Kind))

	const (
		numChildren = 12
		maxParallel = 4
		hungName    = "child-00" // the one task that blocks forever
	)
	// Make child-00 hang inside the work reaction until ctx cancels.
	worker.SetHang(hungName, true)

	reg := newTReg()
	reg.AddKind(composer, composer.Manifest())
	reg.AddKind(worker, worker.Manifest())

	eng := startEngineWithConfig(t, ctx, pool, reg, engineOpts{
		HeartbeatEvery:    500 * time.Millisecond,
		SweeperStaleAfter: 30 * time.Second, // longer than the test → hung task is NOT reaped
		RetryAfter:        2 * time.Second,
		WorkerMaxParallel: maxParallel,
	})
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	// One root composing numChildren independent account children, all of
	// the same kind so they all run through one dispatcher sharing maxParallel
	// slots. No deps between them → every child is immediately eligible.
	children := make([]controllableChildSpec, numChildren)
	for i := range children {
		children[i] = controllableChildSpec{
			Kind: string(account.Kind),
			Name: fmt.Sprintf("child-%02d", i),
		}
	}
	_, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "fill-root", buildSpec(children...), nil)
	require.NoError(t, err)

	// The decisive assertion: while child-00 is still hung (never
	// completes), every OTHER child must reach ready. If the dispatcher
	// were still batch-blocking, a hung task in a claimed cohort would
	// cap completions below numChildren-1. We require all numChildren-1
	// non-hung children ready, with the hung one still blocking.
	wantReady := numChildren - 1
	require.Eventually(t, func() bool {
		var ready int64
		err := pool.QueryRow(ctx, `
			SELECT count(*) FROM resources r JOIN resource_meta m ON m.id = r.id
			WHERE r.owner_id IS NOT NULL AND r.is_ready AND m.name <> $1`,
			hungName,
		).Scan(&ready)
		return err == nil && int(ready) >= wantReady
	}, 30*time.Second, 200*time.Millisecond,
		"all non-hung children should reconcile while one task is hung (continuous-fill)")

	// Sanity: the hung child must NOT be ready — it's still blocking in
	// the work reaction. This confirms we proved "others finished DESPITE
	// the hang", not "the hang resolved".
	var hungReady bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT r.is_ready FROM resources r JOIN resource_meta m ON m.id = r.id WHERE m.name = $1`, hungName).Scan(&hungReady))
	require.False(t, hungReady, "hung child must still be blocked, not ready")

	// And it must have been claimed + actually entered the work reaction (so
	// the slot really is occupied, not merely unscheduled).
	require.GreaterOrEqual(t, worker.Calls(hungName), 1,
		"hung child should have been claimed and entered the work reaction")

	t.Logf("continuous-fill OK: %d/%d non-hung children ready while %q blocks a slot",
		wantReady, numChildren, hungName)
}

var _ = (*pgxpool.Pool)(nil)
