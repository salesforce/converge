package test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// TestTaskDeadlineCancelsAndRetries proves the per-kind TaskDeadline:
// a task that doesn't finish within the window is cancelled (its handler
// ctx fires) and recorded as a TRANSIENT (retryable) failure, so the
// existing requeue machinery runs it again.
//
// The worker HANGS in RunWork (blocks on <-ctx.Done()), so it never
// completes on its own — the ONLY thing that can move it is the
// dispatcher's deadline firing. The reaper's stale window is set far
// longer than the test, so a passing result isolates the deadline path
// (not stale-claim reclamation) as the cause of cancellation + retry.
func TestTaskDeadlineCancelsAndRetries(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	repo := store.New(pool)

	const childName = "deadliner"

	composer := newControllableComposer()
	worker := newFlakyWorker(model.Kind(account.Kind))
	// Hang forever in RunWork; only the deadline can end the task.
	worker.SetHang(childName, true)
	// Short per-task deadline → the dispatcher cancels the hung task fast.
	worker.SetTaskDeadline(1 * time.Second)

	reg := newTReg()
	reg.AddKind(composer, composer.Manifest())
	reg.AddKind(worker, worker.Manifest())

	eng := startEngineWithConfig(t, ctx, pool, reg, engineOpts{
		HeartbeatEvery: 500 * time.Millisecond,
		// Far longer than the test: the reaper must NOT be the thing that
		// frees the task. If the test passes, the DEADLINE drove it.
		SweeperStaleAfter: 5 * time.Minute,
		// Short so the requeue after a deadline failure is observable.
		RetryAfter:        500 * time.Millisecond,
		WorkerMaxParallel: 4,
	})
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "deadline-root",
		buildSpec(controllableChildSpec{Kind: string(account.Kind), Name: childName}), nil)
	require.NoError(t, err)
	_ = rootID

	// Find the child's id so we can read its events.
	var childID uuid.UUID
	require.Eventually(t, func() bool {
		return pool.QueryRow(ctx,
			`SELECT id FROM resource_meta WHERE name = $1`, childName).Scan(&childID) == nil
	}, 15*time.Second, 200*time.Millisecond, "composed child should exist")

	// 1) The hung task must be recorded as a TRANSIENT (non-terminal)
	//    work-failed — that's the deadline firing, not the reaper (whose
	//    stale window hasn't elapsed) and not a terminal stop.
	var deadlineFail *store.EventRow
	require.Eventually(t, func() bool {
		evs, err := repo.ListEvents(ctx, childID, time.Time{}, 0, 200)
		if err != nil {
			return false
		}
		for i := range evs {
			if evs[i].Type != "work-failed" {
				continue
			}
			var d map[string]any
			if json.Unmarshal(evs[i].Detail, &d) != nil {
				continue
			}
			// Non-terminal failure (so it stays retryable).
			if term, ok := d["terminal"].(bool); ok && !term {
				deadlineFail = &evs[i]
				return true
			}
		}
		return false
	}, 20*time.Second, 200*time.Millisecond, "deadline must record a transient work-failed event")
	require.Contains(t, deadlineFail.Message, "deadline exceeded",
		"the failure message should name the deadline as the cause")

	// 2) It must be RETRIED: each retry re-enters RunWork and hangs again,
	//    so the call count climbs past 1. (It never succeeds — proving the
	//    loop is the deadline→fail→requeue→deadline cycle, bounded only by
	//    RetryAfter, with the reaper's 5m window never reached.)
	require.Eventually(t, func() bool {
		return worker.Calls(childName) >= 2
	}, 25*time.Second, 200*time.Millisecond,
		"a deadline-failed task must be retried (RunWork entered more than once)")

	// 3) The child must NOT be ready and NOT terminal — it's stuck in the
	//    retry cycle, exactly what a repeatedly-timing-out task should do.
	var isReady bool
	var failureTerminal bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT is_ready, failure_terminal FROM resources WHERE id = $1`, childID).
		Scan(&isReady, &failureTerminal))
	require.False(t, isReady, "a perpetually-timing-out task is never ready")
	require.False(t, failureTerminal, "a deadline failure must be transient (retryable), not terminal")

	t.Logf("deadline OK: hung task cancelled + retried %d× as a transient failure (reaper stale window untouched)",
		worker.Calls(childName))
}

// TestShutdownDrainsHungDeadlineTask proves graceful shutdown is clean
// even with a deadline-bounded task hung mid-flight: cancelling the
// engine ctx must make Stop() return PROMPTLY (the dispatcher frees the
// slot) AND must wait out the deadline-orphan dispatch goroutine so it
// can't still be touching the pool after Stop() returns (the pool is
// closed right after, in production). The handler hangs on <-ctx.Done(),
// so it only unblocks when the loop ctx cancellation propagates to its
// taskCtx — exactly the shutdown path runOne must drain.
func TestShutdownDrainsHungDeadlineTask(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	const childName = "shutdown-hang"
	composer := newControllableComposer()
	worker := newFlakyWorker(model.Kind(account.Kind))
	worker.SetHang(childName, true)
	// A long deadline relative to the test: we want the task STILL hung
	// (not yet timed out) when we trigger shutdown, so the orphan is live
	// at Stop() time — the exact race the audit flagged.
	worker.SetTaskDeadline(30 * time.Second)

	reg := newTReg()
	reg.AddKind(composer, composer.Manifest())
	reg.AddKind(worker, worker.Manifest())

	// Own ctx for the engine so we can cancel it independently of the test ctx.
	engCtx, engCancel := context.WithCancel(ctx)
	eng := startEngineWithConfig(t, engCtx, pool, reg, engineOpts{
		HeartbeatEvery:    500 * time.Millisecond,
		SweeperStaleAfter: 5 * time.Minute,
		RetryAfter:        500 * time.Millisecond,
		WorkerMaxParallel: 4,
	})
	require.NoError(t, eng.Start(engCtx))

	_, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "shutdown-root",
		buildSpec(controllableChildSpec{Kind: string(account.Kind), Name: childName}), nil)
	require.NoError(t, err)

	// Wait until the worker has actually entered the hung RunWork — only
	// then is there a live orphan to drain at shutdown.
	require.Eventually(t, func() bool { return worker.Calls(childName) >= 1 },
		20*time.Second, 100*time.Millisecond, "hung task should enter RunWork")

	// Trigger shutdown and time Stop(). It must return promptly: cancelling
	// engCtx propagates to the task's taskCtx, the hung handler's
	// <-ctx.Done() unblocks, the orphan drains, and Stop() (Worker then
	// Control wg.Wait) returns. A regression (orphan never waited / never
	// unblocked) would blow the deadline below.
	engCancel()
	stopped := make(chan struct{})
	go func() {
		_ = eng.Stop(context.Background())
		close(stopped)
	}()
	select {
	case <-stopped:
		// clean shutdown
	case <-time.After(15 * time.Second):
		t.Fatal("eng.Stop() hung with a deadline-bounded task in flight — shutdown did not drain cleanly")
	}
	t.Log("shutdown OK: Stop() returned promptly and drained the hung deadline task")
}
