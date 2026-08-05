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

	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// TestTopologyDrillReachesLeaves drills the topology hierarchy
// deployment_instance → functional_domain → team and asserts leaf resources
// come back at the bottom.
//
// Regression for the bug where TopologyLeaves' hand-written Scan() column
// order drifted from the SQL template after `phase` was added to the
// SELECT — the scan mis-mapped the phase string into a later column and
// errored, so the UI's drill showed "no resources under team".
// This pins the column contract end-to-end and the carried `phase`.
func TestTopologyDrillReachesLeaves(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	eng := startAllRolesEngine(t, ctx, pool)
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	// One FD ("alpha-fd") with two teams → composed children carry
	// deployment_instance / functional_domain / team labels the topology groups by.
	bom := smallBOM("project-topo", map[string][]string{
		"alpha-fd": {"team-alpha", "team-beta"},
	})
	specBytes, err := json.Marshal(bom)
	require.NoError(t, err)

	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "project-topo", specBytes, nil)
	require.NoError(t, err)

	repo := store.New(pool)

	// Wait until children exist (the composer ran).
	require.Eventually(t, func() bool {
		kids, err := repo.GetChildrenByOwner(ctx, rootID, 1000000)
		return err == nil && len(kids) > 0
	}, 30*time.Second, 250*time.Millisecond, "composer produced children")

	diName := bom.DeploymentInstance.Name

	// Aggregate at the top level (group by deployment_instance) → at least the
	// deployment_instance bucket.
	aggDI, err := repo.AggregateTopology(ctx, rootID, nil, "deployment_instance", "", 100)
	require.NoError(t, err)
	require.NotEmpty(t, aggDI, "deployment_instance-level aggregate must return buckets")

	// Drill deployment_instance → functional_domain → team, then fetch leaves
	// under the deepest path.
	path := []store.TopologyPathSegment{
		{Key: "deployment_instance", Value: diName},
		{Key: "functional_domain", Value: "alpha-fd"},
		{Key: "team", Value: "team-alpha"},
	}
	leaves, err := repo.TopologyLeaves(ctx, rootID, path, "", uuid.UUID{}, 200)
	require.NoError(t, err, "TopologyLeaves must not error (scan column contract)")
	require.NotEmpty(t, leaves, "deployment_instance→functional_domain→team drill must return leaf resources, not empty")

	// Every leaf must carry the deployment_instance/functional_domain/team labels
	// we drilled on, and a non-empty phase (the field whose scan position regressed).
	for _, leaf := range leaves {
		var labels map[string]string
		require.NoError(t, json.Unmarshal(leaf.Labels, &labels))
		require.Equal(t, diName, labels["deployment_instance"])
		require.Equal(t, "alpha-fd", labels["functional_domain"])
		require.Equal(t, "team-alpha", labels["team"])
		require.NotEmpty(t, leaf.Phase, "leaf must carry a phase")
	}

	t.Logf("deployment_instance→functional_domain→team drill returned %d leaves under team-alpha", len(leaves))
}
