package runtime

import (
	"context"

	"github.com/google/uuid"
)

// Lease carries a claimed task's work_queue identity — the row id, its shard, the
// lease-holder's worker_id, and the web-API KIND VERSION it was claimed for — so a remote
// StageDispatcher (the broker fanout) can scoped-release the DB lease the instant
// it abandons the task (WorkID/ShardID/BrokerID) AND stamp the task's kindVersion on the
// wire for STRICT (kind, kindVersion) routing (KindVersion). It rides on ctx (set by runOne via
// WithLease), NOT in the pure manifest.StageDispatcher signature, so the
// SDK-facing seam stays free of work_queue internals.
type Lease struct {
	WorkID   uuid.UUID
	ShardID  int16
	BrokerID string
	// KindVersion is the claimed row's work_queue.kindVersion — denormalized from the
	// NOT-NULL resources.kind_version at schedule time, so it is always >= 1 (there is
	// no normalize-to-1 step). The broker reads it to route the resulting StageTask to a
	// worker/peer advertising the EXACT (kind, kindVersion); in-process executors ignore it.
	KindVersion int
	// ClaimEpoch is the strict monotonic fencing token this claim carries
	// (work_queue.claim_epoch). It rides the StageTask to the worker and is echoed on
	// the result; the fenced result write (AppendOutbox / StampComposedGen) lands only
	// if the row still bears this epoch. The broker carries it here so the fenced write
	// is owner-independent — the epoch, not the broker identity, is the gate.
	ClaimEpoch int64
	// ManifestVersion is the kind_manifest content hash this claim was enqueued under
	// (work_queue.manifest_version), carried so the compose/result fence checks it
	// without re-reading the lease. 0 = pre-manifest (inert).
	ManifestVersion int64
}

type leaseCtxKey struct{}

// withLease stamps the task's lease identity (incl. its kindVersion, claim epoch, and
// manifest version) on ctx (broker-side; in-process executors ignore it). Internal to
// runtime — runOne calls it.
func withLease(ctx context.Context, l Lease) context.Context {
	return context.WithValue(ctx, leaseCtxKey{}, l)
}

// LeaseFromContext returns the task's lease identity if present (set by the
// dispatcher's runOne). The broker's fanout reads it to scoped-release on
// abandonment; ok is false on the in-process path (no remote lease to release).
func LeaseFromContext(ctx context.Context) (Lease, bool) {
	l, ok := ctx.Value(leaseCtxKey{}).(Lease)
	return l, ok
}
