package test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/internal/api"
	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/model"
)

// TestResourceWorkBlock validates the Tier-1 operability surface: while a
// worker is actively reconciling a resource (here, a deliberately HUNG
// worker so the claim stays in flight), GET /api/resources/{id} returns a
// populated `work` block — which worker holds it, that it's claimed, a
// fresh heartbeat, and attempt count — so a long reconcile isn't a flat
// "Reconciling".
func TestResourceWorkBlock(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	composer := newControllableComposer()
	worker := newFlakyWorker(model.Kind(account.Kind))
	worker.SetHang("hung", true) // the work handler blocks → the claim stays in flight
	reg := newTReg()
	reg.AddKind(composer, composer.Manifest())
	reg.AddKind(worker, worker.Manifest())
	eng := startEngineWithRegistry(t, ctx, pool, reg)
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	srv := api.NewServer(api.DepsFromPools(pool, nil))
	srv.SetDeclaredSchemas(declaredManifests(eng.Registry))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "work-root",
		buildSpec(controllableChildSpec{Kind: string(account.Kind), Name: "hung"}), nil)
	require.NoError(t, err)

	q := dbq.New(pool)

	// Find the hung child once it's composed. Address it by its public
	// (kind, name) — the detail endpoint is name-keyed now.
	var childKind, childName string
	require.Eventually(t, func() bool {
		kids, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: pgtype.UUID{Bytes: rootID, Valid: true}, Limit: 1000000})
		if err != nil {
			return false
		}
		for _, c := range kids {
			if c.Name == "hung" {
				childKind, childName = string(c.Kind), c.Name
				return true
			}
		}
		return false
	}, 30*time.Second, 250*time.Millisecond, "hung child composed")

	type workBlock struct {
		TaskType     string `json:"task_type"`
		WorkerID     string `json:"worker_id"`
		Claimed      bool   `json:"claimed"`
		Attempts     int32  `json:"attempts"`
		Elapsed      string `json:"elapsed"`
		HeartbeatAge string `json:"heartbeat_age"`
	}
	type detail struct {
		Phase string     `json:"phase"`
		Work  *workBlock `json:"work"`
	}

	// Poll the detail endpoint until the worker has claimed the hung child
	// (work block present + claimed). The worker is blocked in RunWork, so
	// the claim persists and the heartbeat goroutine keeps it fresh.
	var got detail
	require.Eventually(t, func() bool {
		resp, err := http.Get(ts.URL + "/api/v1/resources/" + childKind + "/" + childName)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false
		}
		got = detail{}
		if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
			return false
		}
		return got.Work != nil && got.Work.Claimed
	}, 30*time.Second, 250*time.Millisecond, "work block present + claimed")

	require.Equal(t, "Reconciling", got.Phase, "hung child is mid-reconcile")
	require.NotNil(t, got.Work)
	require.Equal(t, "reconcile", got.Work.TaskType)
	require.NotEmpty(t, got.Work.WorkerID, "must surface the claiming worker (the log key)")
	require.True(t, got.Work.Claimed)
	require.GreaterOrEqual(t, got.Work.Attempts, int32(1))
	require.NotEmpty(t, got.Work.Elapsed, "elapsed must be rendered")
	require.NotEmpty(t, got.Work.HeartbeatAge, "heartbeat age must be rendered")

	t.Logf("work block: worker=%s elapsed=%s heartbeat_age=%s attempts=%d",
		got.Work.WorkerID, got.Work.Elapsed, got.Work.HeartbeatAge, got.Work.Attempts)
}
