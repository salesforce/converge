package api

import (
	"context"
	"time"
)

// listClusterInfo returns every cluster member (a running app instance, keyed
// by role) that has written a heartbeat, for the cluster view. Liveness
// (Ready/NotReady) is computed server-side from heartbeat age — like resource
// `phase`, the client renders `status` verbatim and never re-derives it. Rows
// are KEPT past the NotReady threshold; the ClusterMemberGC sweeper hard-deletes
// only well past TTL, so a recently-dead member still shows here (greyed) as
// NotReady. A cleanly-shut-down member deregisters (deletes its own row) and
// disappears immediately.
func (s *Server) listClusterInfo(ctx context.Context, _ *struct{}) (*clusterInfoOutput, error) {
	rows, err := s.readRepo.ListClusterMembers(ctx)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	rows = capList(ctx, "cluster_members", rows)
	now := time.Now()
	out := &clusterInfoOutput{}
	out.Body.Members = make([]clusterMemberInfo, len(rows))
	for i, r := range rows {
		out.Body.Members[i] = clusterMemberFromRow(r, now)
	}
	return out, nil
}
