// Package shardutil holds the shard-space size constant and tiny
// helpers for assigning pod-to-shard ownership. It exists in its own
// package so worker, outbox, engine, cmd/converge, and tests
// can all share NumShards and the assignment logic without import
// cycles.
//
// ╔══════════════════════════════════════════════════════════════╗
// ║ NumShards = 256 — fixed at compile time, oversized for       ║
// ║ runtime pod-count flexibility                                ║
// ║ ──────────────────────────────────────────────────────────── ║
// ║ The shard space cardinality is fixed at 256 by both the Go   ║
// ║ const below and the `mod(.., 256)` divisor in the schema's   ║
// ║ shard_of() function + resources.shard_id GENERATED expr.     ║
// ║ Both must agree — engine.NewControlPlane runs a startup      ║
// ║ safety check (verifyShardModulus) that reads the schema and  ║
// ║ fail-fasts if it doesn't.                                    ║
// ║                                                              ║
// ║ You scale at RUNTIME by changing how many pods consume the   ║
// ║ 256 shards, not by changing NumShards. Assignment is always  ║
// ║ DYNAMIC and membership-driven: just change `replicas` and    ║
// ║ the cluster re-tiles itself — no static pin, no StatefulSet  ║
// ║ ordinal. Each pod registers in cluster_members and its       ║
// ║ Resharder asks the DB assign_member_shards() for its         ║
// ║ contiguous range, ranked among the LIVE pods of its role.    ║
// ║ ShardsForPod below is the REFERENCE tiling that              ║
// ║ assign_member_shards mirrors in SQL (and that the initial    ║
// ║ pre-registration assignment uses):                          ║
// ║                                                              ║
// ║   4 live pods of one role → each owns 64 shards (256/4)      ║
// ║   16 live pods            → each owns 16 shards              ║
// ║   64 live pods            → each owns 4 shards               ║
// ║   256 live pods           → each owns 1 shard                ║
// ║   500 live pods           → 244 pods get nothing (over cap)  ║
// ║                                                              ║
// ║ Why 256: oversized so you can re-distribute without          ║
// ║ touching the schema. To scale beyond 256 pods of one role    ║
// ║ you'd need a one-time migration (rewrite every row's         ║
// ║ shard_id) — at which point you'd bump to 1024 or 4096.       ║
// ║                                                              ║
// ║ Trade-offs of larger NumShards:                              ║
// ║   - Each shard's IN-list scan in WorkQueueTakeBatch /        ║
// ║     OutboxPopBatch grows. With NumShards=256 and a pod       ║
// ║     owning 64 shards, the planner does 64 small index range  ║
// ║     scans per query. Postgres handles this fine up to a few  ║
// ║     hundred elements; beyond that consider splitting the     ║
// ║     query.                                                   ║
// ║   - Drainer fan-out: outbox drainers do                      ║
// ║     `WHERE shard_id = ANY([myshards])`. More shards per pod  ║
// ║     = wider scan per tick.                                   ║
// ║                                                              ║
// ║ To CHANGE NumShards (rarely needed; one-time migration):     ║
// ║   1. Change the `256` divisor to NEW in BOTH the shard_of()  ║
// ║      function body and the resources.shard_id GENERATED       ║
// ║      expression in 00001_schema.sql. (work_queue/work_outbox  ║
// ║      shard_id are PLAIN columns set via shard_of(), so they   ║
// ║      follow automatically.) The expression is                 ║
// ║      abs(mod(hashtext(..), 256)) — mod-first to avoid the     ║
// ║      abs(INT_MIN) overflow; keep that form.                   ║
// ║   2. Re-tile the work_queue/work_outbox RANGE partitions so   ║
// ║      they cover [0, NEW) (the i*16..(i+1)*16 loop assumes      ║
// ║      256/16); pick a partition count that keeps each pod's     ║
// ║      contiguous range inside one partition.                   ║
// ║   3. Change NumShards = 256 to NumShards = NEW here.         ║
// ║   4. Drop + recreate the shard_id column (Postgres can't     ║
// ║      ALTER a STORED expression in place) — or run a fresh    ║
// ║      migration. This rewrites every row.                     ║
// ║   5. Rebuild + redeploy.                                     ║
// ╚══════════════════════════════════════════════════════════════╝
package shardutil

// NumShards is the cardinality of the shard space. See the package
// comment for the schema-sync contract.
const NumShards = 256

// ShardsForPod returns the shard ids that pod `podIdx` (0-based)
// owns when `podCount` pods range-partition `total` shards.
//
// Examples (total=32):
//
//	podIdx=0, podCount=3   → [0..10]   (11 shards)
//	podIdx=1, podCount=3   → [11..21]  (11 shards)
//	podIdx=2, podCount=3   → [22..31]  (10 shards)
//
//	podIdx=0, podCount=10  → [0..2]    (3 shards)
//	...
//	podIdx=9, podCount=10  → [29..31]  (3 shards)
//
//	podIdx=0, podCount=50  → [0]       (1 shard) — every shard
//	podIdx=31, podCount=50 → [31]      covered
//	podIdx=32, podCount=50 → []        (extra pods get nothing
//	                                    when total<podCount; they
//	                                    sit idle on this role)
//
// Range-partition is preferred over hash/round-robin (e.g. `j % N`)
// because round-robin orphans shards when podCount < total
// (e.g. 10 pods × 32 shards round-robin'd leaves shards 10..31
// unowned). Range-partition guarantees full coverage by picking
// disjoint contiguous slices.
//
// The empty-slice case (podIdx >= total) is intentional. The caller
// is expected to fall back to "no sharding" / idle on that role,
// not to receive a fake assignment that overlaps another pod.
func ShardsForPod(podIdx, podCount, total int) []int16 {
	if podCount <= 0 || total <= 0 || podIdx < 0 || podIdx >= podCount {
		return nil
	}
	start := podIdx * total / podCount
	end := (podIdx + 1) * total / podCount
	if start >= total {
		return nil
	}
	if end > total {
		end = total
	}
	out := make([]int16, 0, end-start)
	for s := start; s < end; s++ {
		out = append(out, int16(s))
	}
	return out
}
