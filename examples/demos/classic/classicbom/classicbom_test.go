package classicbom

import (
	"encoding/json"
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/examples/demos/classic/networking"
	"github.com/salesforce/converge/sdk-go/converge"
)

// TestRollupOutputIsOrderIndependent guards against the regression
// where RunRollup's TGW-stamping pass dropped tgw_id for any team
// whose descendant rows were processed AFTER the FD's TGW row.
//
// The framework doesn't promise a deterministic order in
// req.Descendants (no ORDER BY in ListDescendants). Two rollups of
// the same subtree could land descendants in different orders and
// produce status JSONs with byte-size deltas of several KB at scale.
// We reproduce the bug by shuffling the descendant slice and
// asserting the rollup output is byte-identical across permutations.
func TestRollupOutputIsOrderIndependent(t *testing.T) {
	descendants := buildSubtree(2, 5) // 2 FDs × 5 teams each

	// Run once with the natural ordering as the reference.
	ref, err := rollup{}.React(t.Context(), converge.ReactionRequest{Descendants: descendants})
	require.NoError(t, err)
	require.NotEmpty(t, ref.Status, "reference rollup must produce non-empty status")

	// 30 random permutations is plenty; the bug surfaced ~half the
	// time on a single shuffle in the live multi-pod run.
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 30; i++ {
		permuted := append([]converge.Resource(nil), descendants...)
		rng.Shuffle(len(permuted), func(a, b int) { permuted[a], permuted[b] = permuted[b], permuted[a] })

		got, err := rollup{}.React(t.Context(), converge.ReactionRequest{Descendants: permuted})
		require.NoError(t, err)
		require.Equal(t, string(ref.Status), string(got.Status),
			"rollup output must be byte-identical regardless of descendant order (permutation %d)", i)
	}
}

// TestRollupTGWPropagatesToEveryTeam confirms the actual fix: every
// team in an FD that has a TGW gets the FD's tgw_id, regardless of
// whether the team's descendant happened to come before or after the
// TGW descendant in req.Descendants.
func TestRollupTGWPropagatesToEveryTeam(t *testing.T) {
	descendants := buildSubtree(1, 4) // 1 FD × 4 teams

	// Force the worst-case order: TGW first, teams after. Pre-fix,
	// this caused every team's tgw_id to be dropped.
	worst := []converge.Resource{}
	for _, d := range descendants {
		if d.Kind == networking.KindTGW {
			worst = append(worst, d)
		}
	}
	for _, d := range descendants {
		if d.Kind != networking.KindTGW {
			worst = append(worst, d)
		}
	}

	resp, err := rollup{}.React(t.Context(), converge.ReactionRequest{Descendants: worst})
	require.NoError(t, err)

	var status ClassicBOMStatus
	require.NoError(t, json.Unmarshal(resp.Status, &status))
	require.Len(t, status.Teams, 4)
	for _, team := range status.Teams {
		require.NotEmpty(t, team.TGWID, "team %s/%s should have tgw_id propagated", team.FD, team.Team)
	}
}

// buildSubtree constructs a synthetic descendant slice mirroring the
// composer's output for a multi-FD BOM: per FD a TGW host account
// and a TGW; per team an account, VPC, and route. Each resource has
// a populated, ready status with a unique id so the rollup picks up
// values it would otherwise drop on the bug path.
func buildSubtree(fds, teamsPerFD int) []converge.Resource {
	out := []converge.Resource{}
	for f := 0; f < fds; f++ {
		fdName := fdName(f)
		// TGW host account (FD-level, team="")
		out = append(out, mkResource(account.Kind, "tgw-host-"+fdName, fdName, "",
			mustJSON(account.AccountStatus{AccountID: "acct-tgw-" + fdName})))
		// TGW (FD-level, team="")
		out = append(out, mkResource(networking.KindTGW, "tgw-"+fdName, fdName, "",
			mustJSON(networking.TGWStatus{TGWID: "tgw-id-" + fdName})))
		// Per-team resources.
		for ti := 0; ti < teamsPerFD; ti++ {
			team := teamName(ti)
			out = append(out, mkResource(account.Kind, "acct-"+fdName+"-"+team, fdName, team,
				mustJSON(account.AccountStatus{AccountID: "acct-" + fdName + "-" + team})))
			out = append(out, mkResource(networking.KindVPC, "vpc-"+fdName+"-"+team, fdName, team,
				mustJSON(networking.VPCStatus{VPCID: "vpc-" + fdName + "-" + team})))
			out = append(out, mkResource(networking.KindRoute, "route-"+fdName+"-"+team, fdName, team,
				mustJSON(networking.RouteStatus{RouteID: "route-" + fdName + "-" + team})))
		}
	}
	return out
}

func mkResource(kind converge.Kind, name, fd, team string, status []byte) converge.Resource {
	labels := map[string]string{"functional_domain": fd}
	if team != "" {
		labels["team"] = team
	}
	return converge.Resource{
		ID:      uuid.New(),
		Kind:    kind,
		Name:    name,
		Status:  status,
		IsReady: true,
		Labels:  labels,
	}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func fdName(i int) string {
	return "fd-" + string(rune('a'+i))
}

func teamName(i int) string {
	return "team-" + string(rune('a'+i))
}
