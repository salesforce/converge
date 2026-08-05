package api

import (
	"encoding/json"
	"time"

	"github.com/salesforce/converge/internal/store"
)

// projection_cluster.go: the cluster-view projections — a cluster_members
// registry row → the DTO (with the k8s-style soft liveness verdict), and the
// connected-worker JSON column decode.

// Cluster-member liveness badge values (the soft verdict surfaced on the DTO's
// Status field), named so the projection references them by symbol.
const (
	clusterStatusReady    = "Ready"
	clusterStatusNotReady = "NotReady"
)

// clusterMemberFromRow projects a store.ClusterMember registry row into the
// cluster-view DTO, computing the k8s-style soft liveness verdict from heartbeat
// age. `now` is captured once per request by the handler so every row in one
// response is judged against the same instant. Liveness is derived HERE (Go),
// not in SQL: there is no list filter/count that must agree with the badge (the
// reason resource `phase` lives in SQL), so the read query stays a trivial
// SELECT and the 90s threshold lives next to the wire shape.
func clusterMemberFromRow(r store.ClusterMember, now time.Time) clusterMemberInfo {
	// Config and Workers are DISTINCT columns: Config is the static runtime-knob
	// blob (connect_addr + resolved defaults), Workers is the live connected-worker
	// snapshot (populated on broker rows). Each is surfaced on its own field — no
	// overloading, no stripping.
	ci := clusterMemberInfo{
		MemberID:      r.MemberID,
		Role:          r.Role,
		Config:        coerceJSONInterface(r.Config),
		InFlight:      r.InFlight,
		Workers:       connectedWorkersFromColumn(r.Workers),
		Version:       r.Version,
		Hostname:      r.Hostname,
		PID:           r.PID,
		StartedAt:     r.StartedAt,
		LastHeartbeat: r.LastHeartbeat,
	}
	if r.Shards != nil {
		ci.Shards = []int16{r.Shards[0], r.Shards[1]}
	}
	if r.LastHeartbeat.Valid {
		age := now.Sub(r.LastHeartbeat.Time)
		// Clamp negative age to 0: last_heartbeat is the DB clock (stamped by
		// the UPSERT's now()) while `now` is this API process's clock, so a
		// DB-ahead skew makes a fresh beat compute as negative. A beat that
		// landed IS recent, so treat it as "just now" (Ready, age 0) rather
		// than letting "-10s ago" leak to the UI or a negative age slip under
		// the NotReady threshold. Mirrors shortDuration's negative clamp.
		if age < 0 {
			age = 0
		}
		ci.AgeSeconds = int64(age.Seconds())
		ci.Ready = age <= memberNotReadyAfterSeconds*time.Second
	}
	if ci.Ready {
		ci.Status = clusterStatusReady
	} else {
		ci.Status = clusterStatusNotReady
	}
	if r.StartedAt.Valid {
		ci.Uptime = shortDuration(now.Sub(r.StartedAt.Time))
	}
	return ci
}

// connectedWorkersFromColumn decodes the JSON array a broker writes to its
// cluster_members.workers column (broker.WorkersJSON) into the typed cluster DTO.
// The JSON shape is the boundary contract — the API decodes it with its own
// struct rather than importing internal/broker. Returns nil for an empty/"[]"
// column (a control/react member, or a broker with no workers) or a malformed
// blob — the cluster view then shows no workers for that row, never an error
// (the snapshot is best-effort observability).
func connectedWorkersFromColumn(workers []byte) []connectedWorkerInfo {
	if len(workers) == 0 {
		return nil
	}
	var out []connectedWorkerInfo
	if err := json.Unmarshal(workers, &out); err != nil {
		return nil
	}
	if len(out) == 0 {
		return nil // normalize "[]" → nil so omitempty drops the key on non-broker rows
	}
	return out
}
