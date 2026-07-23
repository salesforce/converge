package api

import (
	"fmt"
	"time"

	"github.com/salesforce/converge/internal/store"
)

// projection_work.go: the resource-detail "work" block — picking the live
// reconcile claim from a resource's work_queue rows — plus shortDuration, the
// compact elapsed/age renderer shared with the cluster view.

// workFromRows picks the resource's live reconcile claim (the
// long-running task an operator cares about) from its work_queue rows and
// projects it into the detail DTO, computing the human-readable elapsed /
// heartbeat-age. Returns nil when the resource has no queued/claimed task
// (settled) — so the `work` block is simply absent then. Prefers a
// 'reconcile' row; falls back to whatever's queued (delete/operate).
func workFromRows(rows []store.GetWorkQueueByResourceRow) *workDTO {
	if len(rows) == 0 {
		return nil
	}
	pick := rows[0]
	for _, r := range rows {
		// r.TaskType is dbq.TaskType (distinct from store.TaskType); compare
		// by string value.
		if string(r.TaskType) == string(store.TaskReconcile) {
			pick = r
			break
		}
	}

	now := time.Now()
	w := &workDTO{
		TaskType:    string(pick.TaskType),
		Attempts:    pick.Attempts,
		ClaimedAt:   pick.CreatedAt,
		HeartbeatAt: pick.HeartbeatAt,
		Generation:  pick.Generation,
	}
	if pick.BrokerID != nil && *pick.BrokerID != "" {
		w.BrokerID = *pick.BrokerID
		w.Claimed = true
		// Only surface the executing worker on a CLAIMED row — an unclaimed row's
		// worker_id is cleared at release/reclaim, but gating here keeps the DTO
		// self-consistent (never "queued" + a worker) regardless.
		if pick.WorkerID != nil && *pick.WorkerID != "" {
			w.WorkerID = *pick.WorkerID
		}
	}
	if pick.CreatedAt.Valid {
		w.Elapsed = shortDuration(now.Sub(pick.CreatedAt.Time))
	}
	if pick.HeartbeatAt.Valid {
		w.HeartbeatAge = shortDuration(now.Sub(pick.HeartbeatAt.Time))
	}
	return w
}

// shortDuration renders a duration as a compact operator-friendly string
// (e.g. "8s", "12m", "1h47m"). Negative/zero → "0s".
func shortDuration(d time.Duration) string {
	if d < time.Second {
		return "0s"
	}
	d = d.Round(time.Second)
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	if m == 0 {
		return fmt.Sprintf("%dh", h)
	}
	return fmt.Sprintf("%dh%dm", h, m)
}
