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
	"github.com/salesforce/converge/sdk-go/converge"
)

// A worker (untrusted, esp. a remote one over the broker) must not be able to
// return an arbitrarily large status: it would bloat the work_outbox row, the
// drain transaction, and resources.status — a resource-exhaustion vector on the
// control plane + DB. The reaction engine caps the status at 50 MiB right before
// AppendOutbox (the single chokepoint for EVERY worker path). An oversize status
// is deterministic (a provider bug / attack), so the task fails TERMINALLY: the
// resource lands phase='Failed', failure_terminal=true, and is NOT retry-looped.

type fatStatusWorker struct{ kind model.Kind }

var _ converge.Provider = fatStatusWorker{}

// Kind is the single (kind, version) this worker serves.
func (w fatStatusWorker) Kind() converge.KindVersion {
	return converge.KindVersion{Kind: converge.Kind(w.kind), Version: 1}
}

// OnConfig is a no-op: this test worker reads no default providerconfig.
func (fatStatusWorker) OnConfig(converge.ProviderConfig) {}

// Ready is always true: a pure in-process test controller has no downstream to dial.
func (fatStatusWorker) Ready() bool { return true }

// Work runs the kind's one "work" reaction, returning an oversize (51 MiB) status to
// prove the reaction engine's 50 MiB cap fails the task terminally.
func (w fatStatusWorker) Work(_ context.Context, _ converge.ReactionRequest) (converge.Outcome, error) {
	// 51 MiB of JSON string content — comfortably over the 50 MiB cap.
	big := make([]byte, 51<<20)
	for i := range big {
		big[i] = 'a'
	}
	out, _ := json.Marshal(map[string]string{"blob": string(big)})
	return converge.Outcome{Status: out}, nil
}

func (w fatStatusWorker) Manifest() model.KindManifest {
	return model.KindManifest{
		Kind:        w.kind,
		KindVersion: 1,
		Reactions: []model.ReactionDecl{
			{Name: "work", Trigger: model.TriggerSpecChange, Emits: model.OutcomeMask{model.OutcomeStatus}},
		},
	}
}

func TestOversizeStatusFailsTerminally(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	const leaf model.Kind = "fat-status-leaf"
	composer := newControllableComposer()
	worker := fatStatusWorker{kind: leaf}
	reg := newTReg()
	reg.AddKind(composer, composer.Manifest())
	reg.AddKind(worker, worker.Manifest())

	// Short retry window: if the cap DIDN'T terminal-fail, a transient fail would
	// re-pend and the test would still see it keep flipping — the terminal flag is
	// the load-bearing assertion.
	eng := startEngineWithConfig(t, ctx, pool, reg, engineOpts{RetryAfter: 1 * time.Second})
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "fat-status-root",
		buildSpec(controllableChildSpec{Kind: string(leaf), Name: "fat"}), nil)
	require.NoError(t, err)

	q := dbq.New(pool)
	var childID [16]byte
	require.Eventually(t, func() bool {
		children, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: pgtype.UUID{Bytes: rootID, Valid: true}, Limit: 1000000})
		if err != nil || len(children) != 1 {
			return false
		}
		childID = children[0].ID
		return children[0].Phase == "Failed"
	}, 30*time.Second, 200*time.Millisecond, "oversize-status child must reach phase=Failed")

	// It failed TERMINALLY (deterministic overflow), not transiently — so it won't
	// be retry-looped, and the failure reason names the cap.
	var terminal bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT failure_terminal FROM resources WHERE id=$1`, childID).Scan(&terminal))
	require.True(t, terminal, "oversize status must be a TERMINAL failure (no retry-loop)")

	// And the giant blob never landed in resources.status.
	var statusLen int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT coalesce(length(status::text), 0) FROM resources WHERE id=$1`, childID).Scan(&statusLen))
	require.Less(t, statusLen, 50<<20, "the oversize status must NOT have been written to the DB")
}
