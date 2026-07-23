package test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/examples/demos/classic/fault"
	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/kindschema"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/sdk-go/converge"
	"github.com/salesforce/converge/test/internal/demoruntime"
)

// TestValueFlowReSubstitutesOnUpstreamStatusChange validates that when
// an upstream's status changes, every dependent's flowed spec field is
// re-substituted from the new status — not only on the first
// substitution. This is the "k8s-style controller-authoritative
// convergence" behavior: a flowed field tracks the latest upstream
// value, regardless of how many times the upstream churns.
//
// Topology (built by the inline composer below):
//
//	root (classicbom)
//	  ├── parent (account)            spec: {team_name: "alpha"}
//	  └── dependent (account)         spec: {team_name: ""}
//	         depends_on parent
//	         ValueFlow: dep.spec.team_name ← parent.status.account_id
//
// account.worker writes status.account_id = "acc-stub-" + spec.team_name.
//
// Expected:
//  1. parent runs, status.account_id = "acc-stub-alpha".
//  2. dependent's spec.team_name is substituted to "acc-stub-alpha";
//     dependent runs, status.account_id = "acc-stub-acc-stub-alpha".
//  3. We update parent's spec to {team_name: "beta"}. parent re-runs;
//     new status.account_id = "acc-stub-beta".
//  4. cascade_on_status_change → apply_value_flows_for_source rewrites
//     dependent's spec.team_name from "acc-stub-alpha" to "acc-stub-beta".
//  5. dependent re-pends and re-runs; new status.account_id = "acc-stub-acc-stub-beta".
//
// Before the substituted-flag fix, step (4) was skipped because the
// flow had already been substituted once — dependent stayed wired to
// the stale "acc-stub-alpha".
func TestValueFlowReSubstitutesOnUpstreamStatusChange(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	composer := &valueFlowComposer{}

	reg := newTReg()
	reg.AddKind(composer, composer.Manifest())
	reg.Add(demoruntime.Account(0, 0, fault.Injector{}))
	eng := startEngineWithRegistry(t, ctx, pool, reg)
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "vf-root",
		mustJSON(map[string]string{"parent_team": "alpha"}), nil)
	require.NoError(t, err)

	q := dbq.New(pool)

	// 1+2: both children reach ready. dependent's status reflects the
	// substituted team_name (= parent's status.account_id = "acc-stub-alpha").
	require.Eventually(t, func() bool {
		children, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: pgtype.UUID{Bytes: rootID, Valid: true}, Limit: 1000000})
		if err != nil || len(children) != 2 {
			return false
		}
		for _, c := range children {
			if !c.IsReady {
				return false
			}
		}
		return true
	}, 15*time.Second, 100*time.Millisecond, "both children should reach ready initially")

	parentID, dependentID := childIDByName(t, ctx, q, rootID)

	beforeStatus := getAccountStatus(t, ctx, q, dependentID)
	require.Equal(t, "acc-stub-acc-stub-alpha", beforeStatus.AccountID,
		"dependent's first run should observe parent.status.account_id flowed into its team_name")

	// 3: bump parent's spec → team_name=beta → triggers Worker re-run.
	// parent is an owned CHILD, so it can't be addressed by (kind,name)
	// via ApplySpec — patch it by id with the test-only helper.
	require.NoError(t, eng.PatchSpecByID(ctx, parentID,
		mustJSON(account.AccountSpec{TeamName: "beta"})))

	// 4+5: dependent must re-run after parent's status changes, AND its
	// spec.team_name must reflect the new flowed value.
	require.Eventually(t, func() bool {
		children, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: pgtype.UUID{Bytes: rootID, Valid: true}, Limit: 1000000})
		if err != nil || len(children) != 2 {
			return false
		}
		for _, c := range children {
			if !c.IsReady {
				return false
			}
		}
		st := getAccountStatus(t, ctx, q, dependentID)
		return st.AccountID == "acc-stub-acc-stub-beta"
	}, 15*time.Second, 100*time.Millisecond,
		"dependent must re-run with re-substituted spec: status.account_id should be acc-stub-acc-stub-beta")

	// Confirm the spec on disk actually got rewritten — the heart of
	// the bug. Read the dependent's spec back and assert team_name.
	got := getAccountSpec(t, ctx, q, dependentID)
	require.Equal(t, "acc-stub-beta", got.TeamName,
		"dependent's spec.team_name must be re-substituted from parent's new status.account_id")
}

// ─── valueFlowComposer ──────────────────────────────────────────────────
//
// A classicbom Composer that emits exactly two account children with one
// dep edge carrying a value flow. Used only by this test.

type valueFlowComposer struct {
	mu       sync.Mutex
	calls    int
	rootSpec map[string]string // last spec we composed against
}

type vfRootSpec struct {
	ParentTeam string `json:"parent_team"`
}

var _ converge.Provider = (*valueFlowComposer)(nil)

// Kind is the single (kind, version) this composer serves.
func (*valueFlowComposer) Kind() converge.KindVersion {
	return converge.KindVersion{Kind: converge.Kind(classicbom.Kind), Version: 1}
}

// OnConfig is a no-op: this test composer reads no default providerconfig.
func (*valueFlowComposer) OnConfig(converge.ProviderConfig) {}

// Ready is always true: a pure in-process test controller has no downstream to dial.
func (*valueFlowComposer) Ready() bool { return true }

// Work runs the "compose" reaction: it emits two account children with one dep edge
// carrying a value flow (dependent.spec.team_name ← parent.status.account_id).
func (c *valueFlowComposer) Work(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()

	var spec vfRootSpec
	if err := json.Unmarshal(req.Resource.Spec, &spec); err != nil {
		return converge.Outcome{}, fmt.Errorf("decode vf-root spec: %w", err)
	}
	parentTeam := spec.ParentTeam
	if parentTeam == "" {
		parentTeam = "alpha"
	}

	parentRef := converge.ResourceRef{Kind: converge.Kind(account.Kind), Name: "parent"}
	dependentRef := converge.ResourceRef{Kind: converge.Kind(account.Kind), Name: "dependent"}

	return converge.Outcome{
		Children: []converge.ChildSpec{
			{
				Kind:        converge.Kind(account.Kind),
				KindVersion: 1,
				Name:        "parent",
				Spec:        account.AccountSpec{TeamName: parentTeam},
			},
			{
				Kind:        converge.Kind(account.Kind),
				KindVersion: 1,
				Name:        "dependent",
				Spec:        account.AccountSpec{TeamName: ""}, // filled by value flow
			},
		},
		Edges: []converge.DepEdge{
			{
				From: dependentRef,
				To:   parentRef,
				Values: []converge.ValueFlow{
					{DependentField: "/team_name", SourceField: "/account_id"},
				},
			},
		},
	}, nil
}

// Manifest is the inline CRD a test seeds via reg.AddKind: a "compose" reaction
// (spec change → children/edges with a value flow) for kind=classicbom.
func (c *valueFlowComposer) Manifest() model.KindManifest {
	return model.KindManifest{
		Kind:        model.Kind(classicbom.Kind),
		KindVersion: 1,
		SpecSchema:  kindschema.Of[vfRootSpec](),
		Reactions: []model.ReactionDecl{
			{Name: "compose", Trigger: model.TriggerSpecChange, Emits: model.OutcomeMask{
				model.OutcomeChildren, model.OutcomeEdges, model.OutcomeStatus, model.OutcomeConditions,
			}},
		},
	}
}

// ─── helpers ────────────────────────────────────────────────────────────

func childIDByName(t *testing.T, ctx context.Context, q *dbq.Queries, rootID [16]byte) (parent, dependent [16]byte) {
	t.Helper()
	children, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: pgtype.UUID{Bytes: rootID, Valid: true}, Limit: 1000000})
	require.NoError(t, err)
	require.Len(t, children, 2)
	var pSet, dSet bool
	for _, c := range children {
		switch c.Name {
		case "parent":
			parent, pSet = c.ID, true
		case "dependent":
			dependent, dSet = c.ID, true
		}
	}
	require.True(t, pSet && dSet, "expected parent + dependent under root")
	return parent, dependent
}

// getResourceByID reads the full row by internal id, for white-box test
// inspection: resolve the (kind, name) via the id-based info read, then the
// by-name full read (which carries spec/status).
func getResourceByID(t *testing.T, ctx context.Context, q *dbq.Queries, id [16]byte) dbq.GetResourceRow {
	t.Helper()
	info, err := q.GetResourceInfo(ctx, id)
	require.NoError(t, err)
	r, err := q.GetResource(ctx, dbq.GetResourceParams{Kind: string(info.Kind), Name: info.Name})
	require.NoError(t, err)
	return r
}

func getAccountSpec(t *testing.T, ctx context.Context, q *dbq.Queries, id [16]byte) account.AccountSpec {
	t.Helper()
	r := getResourceByID(t, ctx, q, id)
	raw, err := json.Marshal(r.Spec)
	require.NoError(t, err)
	var s account.AccountSpec
	require.NoError(t, json.Unmarshal(raw, &s))
	return s
}

func getAccountStatus(t *testing.T, ctx context.Context, q *dbq.Queries, id [16]byte) account.AccountStatus {
	t.Helper()
	r := getResourceByID(t, ctx, q, id)
	raw, err := json.Marshal(r.Status)
	require.NoError(t, err)
	var s account.AccountStatus
	require.NoError(t, json.Unmarshal(raw, &s))
	return s
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
