package test

import (
	"context"
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

// TestTerminalFailureStopsRetrying validates the terminal-vs-transient
// distinction: a child whose worker returns model.Terminal(err) must
//   - reach phase='Failed' with failure_terminal=true, AND
//   - STOP being re-queued (the scheduler/reaper skip it),
//
// while a TRANSIENT-failing sibling keeps getting retried. The contrast in
// call counts is the assertion: the terminal child is called a small,
// bounded number of times; the transient child is called many more.
func TestTerminalFailureStopsRetrying(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	composer := newControllableComposer()
	worker := newFlakyWorker(model.Kind(account.Kind))
	worker.SetTerminal("terminal", true)     // never-retryable
	worker.SetFailsBefore("transient", 1000) // transient: retried forever
	reg := newTReg()
	reg.AddKind(composer, composer.Manifest())
	reg.AddKind(worker, worker.Manifest())

	// Short retry window so the transient child is re-queued several times
	// within the test, making the call-count contrast clear.
	eng := startEngineWithConfig(t, ctx, pool, reg, engineOpts{RetryAfter: 1 * time.Second})
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "terminal-root",
		buildSpec(
			controllableChildSpec{Kind: string(account.Kind), Name: "terminal"},
			controllableChildSpec{Kind: string(account.Kind), Name: "transient"},
		), nil)
	require.NoError(t, err)

	q := dbq.New(pool)
	ownerArg := pgtype.UUID{Bytes: rootID, Valid: true}

	// Both reach phase='Failed'; the terminal one carries failure_terminal.
	var terminalID, transientID [16]byte
	require.Eventually(t, func() bool {
		children, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: ownerArg, Limit: 1000000})
		if err != nil || len(children) != 2 {
			return false
		}
		var bothFailed bool
		failedCount := 0
		for _, c := range children {
			if c.Phase == "Failed" {
				failedCount++
			}
			switch c.Name {
			case "terminal":
				terminalID = c.ID
			case "transient":
				transientID = c.ID
			}
		}
		bothFailed = failedCount == 2
		return bothFailed
	}, 25*time.Second, 200*time.Millisecond, "both children reach phase=Failed")

	// Confirm the scalar: terminal child has failure_terminal=true; the
	// transient child has failure_terminal=false.
	var termFlag, transFlag bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT failure_terminal FROM resources WHERE id=$1`, terminalID).Scan(&termFlag))
	require.NoError(t, pool.QueryRow(ctx, `SELECT failure_terminal FROM resources WHERE id=$1`, transientID).Scan(&transFlag))
	require.True(t, termFlag, "terminal child must have failure_terminal=true")
	require.False(t, transFlag, "transient child must have failure_terminal=false")

	// Let the reaper run through several sweep cycles (Interval=5s, then
	// IdleInterval=10s after empty ticks), then compare call counts. The
	// terminal child must have stopped being re-queued (stays at its
	// initial attempt); the transient child keeps being retried (more).
	terminalCallsEarly := worker.Calls("terminal")
	require.Eventually(t, func() bool {
		// The transient child is re-pended by the reaper after each
		// RetryAfter window; wait until it's been retried at least twice.
		return worker.Calls("transient") >= 3
	}, 30*time.Second, 500*time.Millisecond,
		"transient child must keep being retried by the reaper")

	terminalCallsLate := worker.Calls("terminal")
	transientCallsLate := worker.Calls("transient")
	t.Logf("terminal calls: early=%d late=%d | transient calls late=%d",
		terminalCallsEarly, terminalCallsLate, transientCallsLate)

	// The key assertion: the terminal child was NOT re-queued while the
	// transient one was retried repeatedly.
	require.LessOrEqual(t, terminalCallsLate, 2,
		"terminal child must stop being re-queued (≤2 calls), got %d", terminalCallsLate)
	require.Greater(t, transientCallsLate, terminalCallsLate,
		"transient child must be retried strictly more than the terminal one")
}
