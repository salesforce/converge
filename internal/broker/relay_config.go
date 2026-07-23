package broker

import "encoding/json"

// relay_config.go: the cluster_members.Config / .workers JSON helpers a broker
// uses to advertise its dial-able mesh address + its live connected-worker
// snapshot, and to read a peer's advertised address back. The JSON shape (not
// these Go types) is the broker↔API boundary contract.

// ConnectAddrConfigKey is the cluster_members.Config JSON key a broker advertises its
// dial-able MeshService address under, so peers can reach it for the mesh.
const ConnectAddrConfigKey = "connect_addr"

// ConfigWorker is the per-worker JSON shape a broker publishes into its
// cluster_members.workers column (a JSON array) and the API's cluster projection
// parses back. It mirrors ConnectedWorker with the wire field names the cluster
// view renders. The JSON contract — not this Go type — is the broker↔API
// boundary (the API declares its own matching struct).
type ConfigWorker struct {
	// WorkerID is the identity the broker OBSERVED for this connection (SPIFFE ID /
	// trusted mesh header / peer IP), never a self-report. id_source says which and
	// id_verified whether it's cryptographically verified, so the cluster view can
	// badge a peer-IP worker as unverified. omitempty on the two id_* fields keeps a
	// legacy broker's payload rendering (the UI defaults to unverified/unknown).
	WorkerID   string   `json:"worker_id"`
	IDSource   string   `json:"id_source,omitempty"`
	IDVerified bool     `json:"id_verified,omitempty"`
	Kinds      []string `json:"kinds"`
	// KindVersions is PARALLEL to Kinds: the web-API kindVersion this worker serves each
	// Kinds[i] at, so the cluster view renders "vpc/v1", "vpc/v2". omitempty for a
	// legacy broker that predates versioning (the UI defaults a missing entry to 1).
	KindVersions []int `json:"kind_versions,omitempty"`
	InFlight     int   `json:"inflight"`
	MaxInflight  int   `json:"max_inflight"`
}

// AdvertiseAddr returns the member Config JSON advertising addr under
// ConnectAddrConfigKey, merged over any existing config. cmd/converge calls this to
// build ClusterMemberInfo.Config when relay is on. Empty addr → base unchanged.
// (cluster_members supplies only dial addresses; worker interest/credit flows live
// over the route stream.)
func AdvertiseAddr(base json.RawMessage, addr string) json.RawMessage {
	if addr == "" {
		return base
	}
	return mergeConfig(base, ConnectAddrConfigKey, addr)
}

// WorkersJSON marshals the connected-worker snapshot to the JSON array a broker
// writes to its cluster_members.workers column each heartbeat. It's the LIVE
// per-beat state (its own column), NOT part of the static config blob. An empty
// snapshot marshals to "[]" (a broker whose last worker just left shows zero
// workers, never a stale set). On a marshal error it returns "[]" — advertising
// is best-effort observability, never a beat failure.
func WorkersJSON(workers []ConnectedWorker) json.RawMessage {
	cw := make([]ConfigWorker, len(workers))
	for i, w := range workers {
		kinds := make([]string, len(w.Kinds))
		for j, k := range w.Kinds {
			kinds[j] = string(k)
		}
		cw[i] = ConfigWorker{WorkerID: w.WorkerID, IDSource: w.IDSource, IDVerified: w.IDVerified, Kinds: kinds, KindVersions: w.KindVersions, InFlight: w.InFlight, MaxInflight: w.MaxInflight}
	}
	b, err := json.Marshal(cw)
	if err != nil {
		return json.RawMessage("[]")
	}
	return b
}

// mergeConfig sets key=val in the JSON object base (parsing base first so other
// keys survive), returning the re-marshalled object. On any marshal error it
// returns base unchanged — advertising is best-effort.
func mergeConfig(base json.RawMessage, key string, val any) json.RawMessage {
	m := map[string]any{}
	if len(base) > 0 {
		_ = json.Unmarshal(base, &m)
	}
	m[key] = val
	out, err := json.Marshal(m)
	if err != nil {
		return base
	}
	return out
}

func connectAddrFromConfig(cfg json.RawMessage) string {
	if len(cfg) == 0 {
		return ""
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(cfg, &m) != nil {
		return ""
	}
	raw, ok := m[ConnectAddrConfigKey]
	if !ok {
		return ""
	}
	var addr string
	if json.Unmarshal(raw, &addr) != nil {
		return ""
	}
	return addr
}
