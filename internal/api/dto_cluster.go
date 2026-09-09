package api

import (
	"encoding/json"

	"github.com/jackc/pgx/v5/pgtype"
)

// ── cluster members (running app instances — the cluster view) ─────────────

// memberNotReadyAfterSeconds is the k8s-style soft-liveness threshold: a
// member whose last heartbeat is older than this is rendered NotReady
// (≈3 missed 30s beats). The API computes `status` from this so the UI renders
// it VERBATIM and never re-derives liveness — same contract as resource
// `phase`. This is the DISPLAY threshold only; the ClusterMemberGC sweeper
// hard-deletes rows at a LONGER TTL (~5m), so a NotReady row stays listed
// (greyed) until GC reclaims it.
const memberNotReadyAfterSeconds = 90

// clusterMemberInfo is one row of the cluster-members registry for the cluster
// view: one running instance of the app, identified by its role. `status`
// ("Ready"/"NotReady") and `ready` are derived server-side from the heartbeat
// age vs memberNotReadyAfterSeconds — the single liveness signal the UI renders
// verbatim. Everything else is a direct projection of the heartbeat row.
// `shards` is the owned [lo, hi] inclusive span (2-element array) or null;
// `config` is the small booted runtime-config blob passed through verbatim.
type clusterMemberInfo struct {
	MemberID string          `json:"member_id"`
	Role     string          `json:"role"`
	Shards   []int16         `json:"shards"`
	Config   json.RawMessage `json:"config"`
	// InFlight is the member's live claimed-task count. Meaningful only for a
	// BROKER (tasks parked awaiting a worker); 0/irrelevant for control/react, so
	// the UI shows it for broker rows only. Kept because the reporter publishes it.
	InFlight int64 `json:"in_flight"`
	// Workers is the LIVE set of dumb workers connected to this member,
	// populated for BROKER rows only (the broker publishes it into its
	// cluster_members.config each heartbeat). Each entry is one connected worker
	// with the kinds it can execute. nil/empty on control/react rows (they accept
	// no worker streams) and on a broker with no worker connected right now.
	Workers       []connectedWorkerInfo `json:"workers,omitempty"`
	Version       string                `json:"version"`
	Hostname      string                `json:"hostname"`
	PID           int32                 `json:"pid"`
	Status        string                `json:"status"`
	Ready         bool                  `json:"ready"`
	AgeSeconds    int64                 `json:"age_seconds"`
	Uptime        string                `json:"uptime"`
	StartedAt     pgtype.Timestamptz    `json:"started_at"`
	LastHeartbeat pgtype.Timestamptz    `json:"last_heartbeat"`
}

// connectedWorkerInfo is one dumb worker connected to a broker, as shown in
// the cluster view — the analogue of a k8s node's pods. WorkerID is the identity
// the broker OBSERVED for the connection (its mTLS client cert's SPIFFE ID, a
// trusted service-mesh header, or the peer IP — never a self-report); IDSource
// says which and IDVerified whether it's cryptographically verified (false for a
// bare peer IP), so the view can badge an unverified worker. Kinds are the
// resource kinds it can execute (a worker may serve several). InFlight/MaxInflight
// are its live and ceiling concurrent-task counts. It is a projection of the
// broker's in-memory WorkStream streams, published via cluster_members.workers —
// never a DB row.
type connectedWorkerInfo struct {
	WorkerID   string   `json:"worker_id"`
	IDSource   string   `json:"id_source,omitempty"`
	IDVerified bool     `json:"id_verified,omitempty"`
	Kinds      []string `json:"kinds"`
	// KindVersions is PARALLEL to Kinds: the web-API kindVersion this worker serves each
	// Kinds[i] at (published by the broker in cluster_members.workers), so the UI
	// cluster view renders "vpc/v1", "vpc/v2". omitempty + a UI default of 1 keeps
	// a legacy broker's kinds-only payload rendering.
	KindVersions []int `json:"kind_versions,omitempty"`
	InFlight     int32 `json:"inflight"`
	MaxInflight  int32 `json:"max_inflight"`
}

type clusterInfoOutput struct {
	Body struct {
		Members []clusterMemberInfo `json:"members"`
	}
}
