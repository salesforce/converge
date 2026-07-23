package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/salesforce/converge/internal/store"
)

// TestClusterMemberFromRow pins the Go-side soft-liveness verdict and the field
// projection for the cluster view. Liveness is computed here (not in SQL), so
// this fast no-DB test is the guard for the 90s NotReady threshold.
func TestClusterMemberFromRow(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	mk := func(heartbeatAgo time.Duration, shards *[2]int16) store.ClusterMember {
		return store.ClusterMember{
			MemberID:      "host-1-abcd",
			Role:          "worker",
			Shards:        shards,
			InFlight:      4,
			Version:       "v1",
			Hostname:      "host-1",
			PID:           7,
			StartedAt:     pgtype.Timestamptz{Time: now.Add(-2 * time.Hour), Valid: true},
			LastHeartbeat: pgtype.Timestamptz{Time: now.Add(-heartbeatAgo), Valid: true},
		}
	}

	// Fresh (30s ago) → Ready; in-window boundary stays Ready.
	fresh := clusterMemberFromRow(mk(30*time.Second, &[2]int16{0, 127}), now)
	if !fresh.Ready || fresh.Status != "Ready" {
		t.Errorf("30s-old heartbeat: got status=%q ready=%v, want Ready/true", fresh.Status, fresh.Ready)
	}
	if fresh.AgeSeconds != 30 {
		t.Errorf("AgeSeconds = %d, want 30", fresh.AgeSeconds)
	}
	if len(fresh.Shards) != 2 || fresh.Shards[0] != 0 || fresh.Shards[1] != 127 {
		t.Errorf("Shards = %v, want [0 127]", fresh.Shards)
	}
	if fresh.Uptime != "2h" {
		t.Errorf("Uptime = %q, want 2h", fresh.Uptime)
	}
	if fresh.InFlight != 4 || fresh.Version != "v1" || fresh.Hostname != "host-1" {
		t.Errorf("field projection mismatch: %+v", fresh)
	}

	// Just over the 90s threshold → NotReady.
	stale := clusterMemberFromRow(mk(91*time.Second, nil), now)
	if stale.Ready || stale.Status != "NotReady" {
		t.Errorf("91s-old heartbeat: got status=%q ready=%v, want NotReady/false", stale.Status, stale.Ready)
	}
	if stale.Shards != nil {
		t.Errorf("nil shard span should project to nil Shards, got %v", stale.Shards)
	}

	// Clock skew: a DB-stamped heartbeat in the FUTURE relative to the app
	// clock must clamp to age 0 (Ready, "just now") — never a negative
	// age_seconds or a stale row masquerading as Ready via age <= 90s.
	skewed := clusterMemberFromRow(mk(-10*time.Second, nil), now)
	if skewed.AgeSeconds != 0 {
		t.Errorf("future heartbeat: AgeSeconds = %d, want 0 (clamped)", skewed.AgeSeconds)
	}
	if !skewed.Ready || skewed.Status != "Ready" {
		t.Errorf("future heartbeat: got status=%q ready=%v, want Ready/true", skewed.Status, skewed.Ready)
	}
}

// TestConnectedWorkersFromColumn pins the decode of the connected-worker JSON
// array a broker writes to its dedicated cluster_members.workers column — the
// boundary contract broker.WorkersJSON writes and the cluster view reads.
// Workers and Config are DISTINCT columns: the workers snapshot never appears in
// the Config blob (no overloading, no dedup strip needed).
func TestConnectedWorkersFromColumn(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	// The exact array a broker's reporter beat writes to the workers column:
	// two workers, one serving several kinds.
	row := store.ClusterMember{
		MemberID: "broker-2-abcd",
		Role:     "broker",
		Config:   []byte(`{"connect_addr":"broker-2:9090","heartbeat_every":"5s"}`),
		Workers: []byte(`[
			{"worker_id": "fleet-a", "kinds": ["classicbom", "policy"], "inflight": 3, "max_inflight": 10},
			{"worker_id": "fleet-b", "kinds": ["policy"], "inflight": 0, "max_inflight": 10}
		]`),
		LastHeartbeat: pgtype.Timestamptz{Time: now, Valid: true},
	}
	ci := clusterMemberFromRow(row, now)

	if len(ci.Workers) != 2 {
		t.Fatalf("Workers count = %d, want 2", len(ci.Workers))
	}
	if ci.Workers[0].WorkerID != "fleet-a" || ci.Workers[1].WorkerID != "fleet-b" {
		t.Errorf("worker ids = %q,%q; want fleet-a,fleet-b", ci.Workers[0].WorkerID, ci.Workers[1].WorkerID)
	}
	if len(ci.Workers[0].Kinds) != 2 || ci.Workers[0].Kinds[0] != "classicbom" {
		t.Errorf("worker[0].Kinds = %v; want [classicbom policy]", ci.Workers[0].Kinds)
	}
	if ci.Workers[0].InFlight != 3 || ci.Workers[0].MaxInflight != 10 {
		t.Errorf("worker[0] slots = %d/%d; want 3/10", ci.Workers[0].InFlight, ci.Workers[0].MaxInflight)
	}

	// SEPARATION: workers come from their own column, so the Config blob carries
	// ONLY the runtime knobs — it never contains a workers key.
	var cfg map[string]json.RawMessage
	if err := json.Unmarshal(ci.Config, &cfg); err != nil {
		t.Fatalf("projected Config is not a JSON object: %v", err)
	}
	if _, dup := cfg["workers"]; dup {
		t.Error("Config blob contains a workers key — workers live in their own column, not config")
	}
	if _, ok := cfg["connect_addr"]; !ok {
		t.Error("Config blob is missing connect_addr")
	}

	// A control/react member (or a broker with none) has an empty "[]" workers
	// column → nil Workers, no error.
	noWorkers := clusterMemberFromRow(store.ClusterMember{
		Role:          "control",
		Config:        []byte(`{}`),
		Workers:       []byte(`[]`),
		LastHeartbeat: pgtype.Timestamptz{Time: now, Valid: true},
	}, now)
	if noWorkers.Workers != nil {
		t.Errorf("control member with [] workers → Workers = %v, want nil", noWorkers.Workers)
	}

	// A nil/empty column and a malformed blob are both safe no-ops (best-effort
	// observability — never an error to the cluster view).
	if got := connectedWorkersFromColumn(nil); got != nil {
		t.Errorf("nil column → %v, want nil", got)
	}
	if got := connectedWorkersFromColumn([]byte(`[not json`)); got != nil {
		t.Errorf("malformed column → %v, want nil", got)
	}
}

// TestShortDuration pins the operator-friendly elapsed/heartbeat-age
// rendering used in the resource `work` block.
func TestShortDuration(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{0, "0s"},
		{500 * time.Millisecond, "0s"},
		{8 * time.Second, "8s"},
		{90 * time.Second, "1m"},
		{12 * time.Minute, "12m"},
		{time.Hour, "1h"},
		{107 * time.Minute, "1h47m"}, // 1h47m
		{2 * time.Hour, "2h"},
		{-5 * time.Second, "0s"}, // clock skew / never-negative
	}
	for _, c := range cases {
		if got := shortDuration(c.d); got != c.want {
			t.Errorf("shortDuration(%v) = %q, want %q", c.d, got, c.want)
		}
	}
}

// findCond returns the synthesized condition of the given type, or nil.
func findCond(conds []conditionDTO, typ string) *conditionDTO {
	for i := range conds {
		if conds[i].Type == typ {
			return &conds[i]
		}
	}
	return nil
}

// TestSynthesizeConditions pins the two-axis synthesis, especially the
// regression where a not-yet-reconciled resource was painted "failed"
// because its synthesized Synced condition was False instead of Unknown.
func TestSynthesizeConditions(t *testing.T) {
	cases := []struct {
		name       string
		generation int64
		syncedGen  int64
		healthOK   bool
		phase      string
		stored     []conditionDTO
		wantSynced string // expected Synced status
		wantReady  string // expected Ready status
	}{
		{
			name:       "reconciling (never synced) is Unknown, not False",
			generation: 1, syncedGen: 0, healthOK: true, phase: "Reconciling",
			wantSynced: "Unknown", wantReady: "Unknown",
		},
		{
			name:       "synced + healthy is True/True",
			generation: 2, syncedGen: 2, healthOK: true, phase: "Ready",
			wantSynced: "True", wantReady: "True",
		},
		{
			name:       "synced but unhealthy is True/False",
			generation: 2, syncedGen: 2, healthOK: false, phase: "Degraded",
			wantSynced: "True", wantReady: "False",
		},
		{
			name:       "stale spec edit (synced behind) is Unknown, not False",
			generation: 3, syncedGen: 2, healthOK: true, phase: "Reconciling",
			wantSynced: "Unknown", wantReady: "Unknown",
		},
		{
			name:       "real reconcile error (phase=Failed) surfaces stored Synced=False",
			generation: 1, syncedGen: 0, healthOK: true, phase: "Failed",
			stored:     []conditionDTO{{Type: "Synced", Status: "False", Reason: "ReconcileError", Message: "boom"}},
			wantSynced: "False", wantReady: "Unknown",
		},
		{
			name:       "stored Ready=False honored while phase=Degraded",
			generation: 1, syncedGen: 1, healthOK: true, phase: "Degraded",
			stored:     []conditionDTO{{Type: "Ready", Status: "False", Reason: "ProbeFailed"}},
			wantSynced: "True", wantReady: "False",
		},
		{
			name: "recovered: stale stored False is NOT surfaced (phase back to Ready)",
			// The resource failed (stored Synced=False + Ready=False), then
			// recovered: synced, healthy, phase=Ready. The honest projection
			// must show the synthesized True/True, not the dead error.
			generation: 1, syncedGen: 1, healthOK: true, phase: "Ready",
			stored: []conditionDTO{
				{Type: "Synced", Status: "False", Reason: "ReconcileError", Message: "old boom"},
				{Type: "Ready", Status: "False", Reason: "ProbeFailed"},
			},
			wantSynced: "True", wantReady: "True",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := synthesizeConditions(tc.generation, tc.syncedGen, tc.healthOK, tc.phase, tc.stored)
			synced := findCond(got, "Synced")
			if synced == nil {
				t.Fatalf("no Synced condition synthesized")
			}
			if synced.Status != tc.wantSynced {
				t.Errorf("Synced status = %q, want %q", synced.Status, tc.wantSynced)
			}
			ready := findCond(got, "Ready")
			if ready == nil {
				t.Fatalf("no Ready condition synthesized")
			}
			if ready.Status != tc.wantReady {
				t.Errorf("Ready status = %q, want %q", ready.Status, tc.wantReady)
			}
			// A reconciling-but-not-failed resource must never carry a
			// False condition (that's what painted it red).
			if tc.wantSynced != "False" && tc.wantReady != "False" {
				for _, c := range got {
					if c.Status == "False" {
						t.Errorf("unexpected False condition %q on a non-failed resource", c.Type)
					}
				}
			}
		})
	}
}
