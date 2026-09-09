-- Work queue claim + result append paths.

-- name: GetWorkQueueByResource :many
-- Read-only: the in-flight/pending work_queue rows for one resource, so
-- the API can surface "who is working this and for how long" on the
-- detail view (Tier-1 operability for long reconciles). A resource has at
-- most one row per task_type (UNIQUE (resource_id, task_type, shard_id)),
-- so this returns 0..few rows. broker_id is the claiming pod / lease holder
-- (the broker in a fanned-out deploy); worker_id is the dumb worker that
-- actually ran the stage (UI "running on <worker>"); heartbeat_at proves liveness;
-- attempts > 1 means it's been retried; created_at is when this attempt
-- was (re)queued. NOT on any hot path — a single indexed point read.
-- shard_id = shard_of(resource_id) prunes the Append to the single
-- partition holding this resource (shard_of is IMMUTABLE), so it's one
-- partition's index probe, not a scan of all 16.
SELECT id, resource_id, task_type, kind, kind_version, generation,
       attempts, broker_id, worker_id, heartbeat_at, created_at, shard_id
FROM work_queue
WHERE resource_id = $1 AND shard_id = shard_of($1);

-- name: WorkQueueTakeBatch :many
-- Claims up to limit pending tasks of (kind, task_type) from the
-- worker's assigned shards, atomically assigning broker_id so other
-- pods' dispatchers skip them.
--
-- NO ORDER BY: FIFO is not a correctness requirement here. Eligibility
-- is decided by schedule_eligible before a row lands in work_queue; a
-- re-pend rewrites created_at=now() anyway; and starvation is bounded
-- by the reaper backstop. Ordering by created_at forced the planner to
-- run one index range-scan per shard in shards[] and MergeAppend/Sort
-- them just to honour FIFO — the dominant cost in the #1 sink. Without
-- it, each shard scan stops at LIMIT under a plain Append + SKIP LOCKED.
-- The partial index idx_work_queue_pending drops created_at to match.
-- shard_lo/shard_hi bound the worker's CONTIGUOUS shard range. We use a
-- range predicate (shard_id BETWEEN lo AND hi) rather than
-- shard_id = ANY(array) on purpose: work_queue is RANGE-partitioned by
-- shard_id, and Postgres does runtime (execution-time) partition pruning
-- of the Append node for a bound range predicate, but NOT for
-- `= ANY($param_array)` (that always scans every partition → 16× the
-- relation locks → lock-manager fast-path spill — measured: the array
-- form made the partitioned 1M run SLOWER than non-partitioned). A
-- worker owns a contiguous slice (shardutil.ShardsForPod), so lo..hi is
-- exact. shard_id is carried through the join so the outer UPDATE prunes
-- to the same partition(s) the inner scan did, and returned so heartbeat
-- / increment / delete can prune by the row's exact shard.
--
-- CONCURRENCY CAP: if this kind has a kind_config row with max_inflight > 0, the
-- claim is bounded so the GLOBAL in-flight count (summed across ALL pods/shards)
-- stays under the cap — one shared pool of tokens, not a per-shard slice, so no
-- shard starves. The cap is read DIRECTLY from kind_config (the single source);
-- there is no projected copy.
--   * budget CTE: max_inflight - SUM(in_flight over ALL of the kind's
--     partials). The SUM is over the tiny kind_inflight table (a few rows per
--     capped kind — one per reaper range + transient worker hints), NOT a
--     scan of work_queue. For an UNCAPPED kind (no row, or max_inflight = 0)
--     the `max_inflight > 0` filter makes the CTE EMPTY → COALESCE picks plain
--     `lim` → zero behaviour change, just a PK probe on kind_config.
--   * picked: BYTE-FOR-BYTE the hot path — same idx_work_queue_pending, same
--     BETWEEN range predicate (partition pruning), FOR UPDATE SKIP LOCKED, NO
--     ORDER BY. Only the LIMIT shrinks for a capped kind near the global cap;
--     GREATEST(...,0) clamps a momentarily-negative pool to 0 rows.
--   * bump: increments THIS pod's partial (keyed by the claim's shard_lo) by
--     the rows ACTUALLY stamped — count(claimed), never the requested LIMIT
--     (under-fill would over-count). An optimistic hint that holds the cap
--     between recounts; recount_inflight reclaims and overwrites it with the
--     true per-range count on the reaper tick. Fires only for capped kinds.
WITH budget AS (
    SELECT c.max_inflight - COALESCE(SUM(i.in_flight), 0) AS headroom
    FROM kind_config c
    LEFT JOIN kind_inflight i ON i.kind = c.kind AND i.kind_version = c.kind_version
    WHERE c.kind = $1 AND c.kind_version = sqlc.arg('kind_version')::int AND c.max_inflight > 0
    GROUP BY c.max_inflight
), picked AS (
    -- The broker's HasSubscriber(kind, kind_version) gate has ALREADY decided a worker
    -- for this (kind, kind_version) is connected before this claim runs; kind_version is NOT in
    -- the pending-claim index (idx_work_queue_pending stays (kind, task_type,
    -- shard_id)) so this scan is BYTE-FOR-BYTE the sargable exact-equality hot
    -- path. kind_version IS filtered so a claim only ever takes rows of the kind_version it is
    -- claiming for — the broker never mixes kind versions in one channel.
    SELECT inner_q.id, inner_q.shard_id FROM work_queue inner_q
    WHERE inner_q.kind = $1
      AND inner_q.kind_version = sqlc.arg('kind_version')::int
      AND inner_q.task_type = sqlc.arg('task_type')::task_type
      AND inner_q.broker_id IS NULL
      AND inner_q.shard_id BETWEEN sqlc.arg('shard_lo')::smallint AND sqlc.arg('shard_hi')::smallint
    LIMIT GREATEST(LEAST(sqlc.arg('lim')::int,
                         COALESCE((SELECT headroom FROM budget), sqlc.arg('lim')::int)), 0)
    FOR UPDATE SKIP LOCKED
), claimed AS (
    -- worker_id = NULL: every FRESH claim starts un-attributed, so a row reused
    -- across attempts/generations can never show a prior worker's stamp (the
    -- strongest guard for the attribution-staleness bug — resets regardless of which
    -- path freed the row before).
    --
    -- claim_epoch = claim_epoch + 1: bump the fencing token in the SAME UPDATE that
    -- wins the claim, and RETURN the new value so the broker ships it in the
    -- StageTask. The result write later fences on this exact epoch, so a prior
    -- claim's late result (even from the same broker) carries an older epoch and
    -- no-ops. broker_id IS NULL + SKIP LOCKED still elects the winner; the epoch
    -- fences the WRITE, never the claim.
    UPDATE work_queue q
    SET broker_id = $2, worker_id = NULL, heartbeat_at = now(),
        claim_epoch = q.claim_epoch + 1
    FROM picked
    WHERE q.id = picked.id AND q.shard_id = picked.shard_id
    RETURNING q.id, q.resource_id, q.kind, q.kind_version, q.task_type, q.op_id, q.attempts,
              q.generation, q.spec, q.provider_config, q.provider_bundle, q.manifest_version,
              q.claim_epoch, q.shard_id
), bump AS (
    -- The cap is per-(kind, kind_version); the tally row is keyed (kind, kind_version, range_lo).
    INSERT INTO kind_inflight (kind, kind_version, range_lo, in_flight, refreshed_at)
    SELECT $1, sqlc.arg('kind_version')::int, sqlc.arg('shard_lo')::smallint, count(*)::int, now()
    FROM claimed
    WHERE EXISTS (SELECT 1 FROM kind_config WHERE kind = $1 AND kind_version = sqlc.arg('kind_version')::int AND max_inflight > 0)
    HAVING count(*) > 0
    ON CONFLICT (kind, kind_version, range_lo) DO UPDATE
        SET in_flight = kind_inflight.in_flight + EXCLUDED.in_flight,
            refreshed_at = now()
)
-- task_deadline_secs rides along from the (kind, kind_version) kind_config so the
-- per-task timeout is LIVE-editable (operator UpsertKindConfig takes effect on the
-- next claim, no restart) instead of frozen in the pod's in-memory registry. One
-- PK probe per BATCH (all rows are the same (kind, kind_version)) — off the per-task
-- path; an absent row → 0 (no deadline), the same default as an uncapped kind.
SELECT claimed.id, claimed.resource_id, claimed.kind, claimed.kind_version, claimed.task_type,
       claimed.op_id, claimed.attempts, claimed.generation, claimed.spec,
       claimed.provider_config, claimed.provider_bundle, claimed.manifest_version,
       claimed.claim_epoch, claimed.shard_id,
       COALESCE(kc.task_deadline_secs, 0)::int AS task_deadline_secs
FROM claimed
LEFT JOIN kind_config kc ON kc.kind = claimed.kind AND kc.kind_version = claimed.kind_version;

-- NOTE: WorkQueueHeartbeat is HAND-WRITTEN in internal/dbq/work_queue_heartbeat.go
-- — sqlc's analyzer can't infer a multi-column unnest($1::uuid[], $2::bigint[])
-- signature. It refreshes heartbeat_at for the (id, claim_epoch) pairs a live
-- worker ATTESTED within the liveness window (the broker relays a worker's
-- WorkHeartbeat there), FENCED on claim_epoch so a reaped/re-issued lease can't be
-- kept alive by its prior holder. Kept out of this file so `just sqlc` stays green.

-- NOTE: WorkQueueMarkWorkerBatch (the coalesced attribution write) is HAND-WRITTEN
-- in internal/dbq/work_queue_markworker.go — sqlc's analyzer can't infer a
-- multi-column unnest($2::uuid[], $3::text[], $4::smallint[]) signature. It stamps
-- worker_id for a batch of (id, worker_id, shard) rows scoped to this broker's
-- broker_id. Kept out of this file so `just sqlc` stays green.

-- name: WorkQueueIncrementAttempts :exec
UPDATE work_queue SET attempts = attempts + 1
WHERE id = $1 AND shard_id = $2;

-- name: WorkQueueReleaseBroker :execrows
-- Release every claim this pod holds on graceful shutdown, so the work is
-- re-claimable IMMEDIATELY by a surviving pod instead of waiting out the
-- reaper's stale window (the dying pod stops heartbeating the moment its
-- dispatcher stops, so without this the row sits broker_id-set until
-- reap_stale_work's stale_seconds elapse — the multi-minute stranding on a
-- rollout). broker_id is this process's globally-unique id
-- (hostname-pid-uuid8), so the match is exact and scoped to OUR claims; a
-- task that outlived a reshard is still ours by broker_id regardless of where
-- its shard now sits, so no shard bound is needed (the partial leased index
-- idx_work_queue_leased prunes the scan). Mirrors reap_stale_work's release:
-- it nulls broker_id only and leaves the kind_inflight cap tally to the
-- reaper's recount_inflight self-heal (the claim is the single counter edge).
-- Returns the count freed (logged on shutdown). Best-effort under a short
-- deadline — a DB blip just falls back to the reaper backstop. Also clears
-- worker_id so a released row shows no stale worker attribution in the UI.
-- Bumps claim_epoch as it frees the claim, so a result from the released claim
-- carries a stale epoch and no-ops once a survivor re-claims.
-- DEADLOCK-SAFE: pre-locks its rows in a `pick` CTE with FOR NO KEY UPDATE ...
-- SKIP LOCKED ordered (shard_id, id), so this best-effort release can never form a
-- 40P01 cycle with a concurrent work_queue writer (reap_stale_work, the drainer's
-- DELETE) locking the same rows in a different order — a hazard when the DB comes
-- back and pods slam work_queue at once. A row another writer already holds is left
-- for the reaper backstop (release is best-effort under a short deadline anyway).
WITH pick AS (
    SELECT w.id, w.shard_id FROM work_queue w
    WHERE w.broker_id = $1
    ORDER BY w.shard_id, w.id FOR NO KEY UPDATE OF w SKIP LOCKED
)
UPDATE work_queue q SET broker_id = NULL, worker_id = NULL, heartbeat_at = now(),
                        claim_epoch = claim_epoch + 1
FROM pick p WHERE q.id = p.id AND q.shard_id = p.shard_id;

-- name: WorkQueueReleaseTasks :execrows
-- Release a SPECIFIC subset of the claims this broker holds, by id list. This
-- is the broker-tier "a worker stream died" fast path: when a WorkStream stream
-- drops, the broker holding those leases (broker_id = brokerID) NULLs
-- broker_id for exactly that stream's in-flight tasks so a surviving worker
-- re-claims them in milliseconds instead of waiting out reap_stale_work's stale
-- window. Unlike WorkQueueReleaseBroker (whole-pod, shutdown), this is scoped to
-- the dead stream's task ids and keeps the broker's OTHER leases intact. Bounded
-- by shard_lo/shard_hi for partition pruning (mirrors WorkQueueHeartbeat). Like
-- the reaper, it nulls broker_id only and leaves the kind_inflight tally to
-- recount_inflight's self-heal. Also clears worker_id so the dropped stream's
-- worker attribution doesn't linger on a re-claimable row. Returns the count freed.
-- Bumps claim_epoch as it frees each claim, so the dropped stream's late result
-- carries a stale epoch and no-ops once a survivor re-claims.
-- DEADLOCK-SAFE: same ordered SKIP-LOCKED pre-lock as WorkQueueReleaseBroker /
-- WorkQueueHeartbeat — a worker-drop release fires on the churn/crash hot path
-- exactly when many pods contend work_queue, so it must never lock rows out of the
-- global (shard_id, id) order and cycle with reap_stale_work / the drainer.
WITH pick AS (
    SELECT w.id, w.shard_id FROM work_queue w
    JOIN unnest($2::uuid[]) AS t(id) ON w.id = t.id
    WHERE w.broker_id = $1 AND w.shard_id BETWEEN $3::smallint AND $4::smallint
    ORDER BY w.shard_id, w.id FOR NO KEY UPDATE OF w SKIP LOCKED
)
UPDATE work_queue q SET broker_id = NULL, worker_id = NULL, heartbeat_at = now(),
                        claim_epoch = claim_epoch + 1
FROM pick p WHERE q.id = p.id AND q.shard_id = p.shard_id;

-- name: AppendOutbox :exec
-- Generic outbox append. The drainer dispatches by task_type:
--   reconcile -> coalesced status + synced_gen + value-flow substitution
--   delete    -> remove finalizer string; hard-delete if last
--   operate   -> update resource_operations row
--
-- payload semantics:
--   reconcile -> new status JSON
--   operate   -> verb output JSON
--   delete    -> NULL
--
-- advance_synced_gen is true when the worker considers the kind's
-- pipeline fully reconciled (e.g. compose+work+rollup all ran).
-- False when the worker emitted intermediate status but rollup
-- couldn't run (descendants not ready); the cascade trigger fires
-- this row again when a descendant catches up.
--
-- health_ok is the Ready/health axis the worker observed. NULL when the
-- worker said nothing about health (the drainer leaves resources.health_ok
-- untouched). conditions is an optional JSONB array of
-- {type,status,reason,message} the drainer upserts into resource_conditions
-- on transition; NULL/[] means "nothing to write".
--
-- failed marks a hard reconcile failure: the drainer stamps
-- resources.failure_gen = observed_generation for it (→ phase='Failed').
-- Defaults false; healthy reconciles never set it. terminal marks that
-- failure non-retryable (→ failure_terminal; scheduler stops re-queuing).
--
-- The trailing notify_gated('outbox_ready') wakes the drainer. AppendOutbox is
-- a PER-ROW hot-path statement (~1M calls/run) — but notify_gated is GLOBALLY
-- rate-limited and LOCK-FREE for the ~99.99% of calls inside the coalesce
-- window (a single unlocked SELECT that returns early). Only ~20 calls/sec
-- fleet-wide reach the actual pg_notify, so the async-notification queue lock
-- (which a bare per-row pg_notify storm would serialize 1000 backends on)
-- stays uncontended. Kept in the same statement (CTE) so it commits atomically
-- with the row — the NOTIFY only delivers on COMMIT, so the woken drainer
-- always sees the appended row. See notify_gated().
--
-- FENCING TOKEN — the write lands ONLY if the row STILL bears the claim_epoch the
-- task was dispatched under. work_id = work_queue.id (stable across re-pends);
-- claim_epoch ($5) = the strict monotonic token stamped on the claim and carried
-- in the StageTask. This makes the result an OWNER-INDEPENDENT fenced write — ANY
-- broker holding the current-epoch token writes it (a worker that reconnected to a
-- different broker, or a peer that executed a forwarded task), no home-route hop:
--   * a NETWORK-ISOLATED worker that came back online late — its claim was reaped
--     (which bumps claim_epoch) and re-issued — carries the OLD epoch, so its stale
--     INSERT selects 0 rows and writes NOTHING (no stale status/output/failure
--     overwrite, no double drain). Its ctx is live (a network stall, not a
--     deadline), so the ctx.Err() guard does NOT catch it; this fence is what does.
--     This holds even when the SAME broker reaped and re-dispatched its own row —
--     the epoch moved, so identity would not have caught it but the epoch does.
--   * a stale gen-N completion arriving after schedule_eligible re-pointed the row
--     to gen-(N+1) matches neither the generation (reconcile) nor the epoch (the
--     re-point bumps both), so it is doubly rejected.
-- generation stays as the reconcile-only anti-staleness axis (belt-and-suspenders
-- with the epoch); for operate/delete the generation is NOT re-pointed, so the
-- generation predicate is relaxed for them and the epoch is the SOLE ownership
-- fence — a uniform, strictly monotonic guard for every task type. manifest_version
-- fences a mid-flight manifest change (inert when either side is 0). Cost on the 1M
-- reconcile happy path: ONE buffer-resident PK probe on work_queue(id, shard_id) —
-- the SAME row the dispatcher just claimed and the heartbeat keeps touching, and
-- the same probe the drainer's DELETE already issues. No scan, no new index,
-- sub-microsecond. A fenced-out write inserts 0 rows and skips the notify.
--
-- ON CONFLICT (work_id, shard_id) DO NOTHING closes the last narrow window the
-- fence alone can't: a task reaped+reassigned where the PREDECESSOR's result row is
-- still UNDRAINED. The reap-skip (reap_stale_work skips rows that have a work_outbox
-- row) and this fence both consult work_outbox at a DIFFERENT instant than the
-- predecessor's insert commits, so an MVCC-snapshot race can still let the
-- reassigned pod C — whose current-epoch fence EXISTS passes — try to insert
-- work_id=W while predecessor A's row sits undrained → a bare INSERT would 23505.
-- Both A and C produced the same result (same generation), so A's already-present
-- row is authoritative and C's is redundant: DO NOTHING drops it silently instead
-- of erroring. (The fence still rejects genuine STALE/zombie writes outright — DO
-- NOTHING only swallows the duplicate from the rare legit-owner-with-undrained-
-- predecessor overlap.)
-- shard_id is passed EXPLICITLY ($5), NOT computed as shard_of($2) here. This is
-- load-bearing for performance: the fence's `q.shard_id = shard_of($2)` form put
-- shard_of() (a function of a BIND param) in the partition-key predicate, which
-- the planner CANNOT use for partition pruning of a generic/prepared plan — so
-- pgx's prepared AppendOutbox locked ALL 16 work_queue partitions on EVERY call
-- (~1M/run), spilling the lock-manager fast path → measured LWLock:LockManager
-- contention TRIPLED (16→51 avg concurrent waiters) and AppendOutbox mean rose to
-- ~3.6ms. With a plain `q.shard_id = $5` smallint param, execution-time pruning
-- removes 15 of 16 subplans ("Subplans Removed: 15") even under the generic plan,
-- so each call locks/probes exactly ONE partition. The caller already has the
-- shard (task.ShardID = shard_of(resource_id), the partition key it was claimed
-- under), so $5 is authoritative and identical to shard_of($2).
WITH ins AS (
    INSERT INTO work_outbox (
        work_id, resource_id, op_id, task_type, succeeded,
        payload, error_message, observed_generation, advance_synced_gen,
        health_ok, conditions, failed, terminal, finalizer_name, manifest_version, shard_id
    )
    SELECT
        $1, $2, sqlc.narg('op_id')::uuid, sqlc.arg('task_type')::task_type, $3,
        sqlc.narg('payload')::jsonb, sqlc.narg('error_message')::text, $4,
        sqlc.arg('advance_synced_gen')::boolean,
        sqlc.narg('health_ok')::boolean, sqlc.narg('conditions')::jsonb,
        sqlc.arg('failed')::boolean, sqlc.arg('terminal')::boolean,
        sqlc.narg('finalizer_name')::text, sqlc.arg('manifest_version')::bigint, sqlc.arg('shard_id')::smallint
    WHERE EXISTS (
        -- FENCE: the row still bears the epoch this task was claimed under, at the
        -- observed generation (reconcile), under the SAME manifest version. The
        -- worker echoes claim_epoch + manifest_version from its StageTask; a re-pend
        -- or reap bumps claim_epoch, so a stale/zombie result matches nothing.
        -- manifest_version=0 on either side = the no-manifest path → that predicate
        -- is inert (0 matches 0; a 0 from the worker against a non-zero row is
        -- treated as "don't fence on version" so a worker that never learned a
        -- version can't be starved).
        SELECT 1 FROM work_queue q
         WHERE q.id = $1 AND q.shard_id = sqlc.arg('shard_id')::smallint
           AND q.claim_epoch = sqlc.arg('claim_epoch')::bigint
           AND (sqlc.arg('task_type')::task_type <> 'reconcile'::task_type
                OR q.generation = $4)
           AND (sqlc.arg('manifest_version')::bigint = 0
                OR q.manifest_version = 0
                OR q.manifest_version = sqlc.arg('manifest_version')::bigint)
    )
    ON CONFLICT (work_id, shard_id) DO NOTHING
    RETURNING 1
)
SELECT notify_gated('outbox_ready') FROM ins;

-- name: DrainOutboxBatch :one
-- shard_lo/shard_hi bound the drainer's contiguous shard range (range
-- predicate → runtime partition pruning on work_outbox).
SELECT drain_outbox_batch($1::int, $2::smallint, $3::smallint)::int AS drained;

-- name: DrainRollupRechecks :one
-- Fresh-snapshot re-pend of straddled rollup roots queued by the cascade trigger
-- (drain_rollup_rechecks / rollup_recheck). shard_lo/shard_hi bound the drainer
-- band's contiguous range. Returns how many queued roots it processed.
SELECT drain_rollup_rechecks($1::int, $2::smallint, $3::smallint)::int AS rechecked;

-- name: DrainScheduleRechecks :one
-- Band-scoped batch schedule of the children a wide compose armed in
-- schedule_recheck (OPT-A) — the deferred, drain-paced replacement for the
-- post-commit ScheduleEligible that deadlocked. shard_lo/shard_hi bound the
-- drainer band's contiguous range. Returns how many rows it processed (0 once a
-- compose's children are all scheduled — the steady state).
SELECT drain_schedule_rechecks($1::int, $2::smallint, $3::smallint)::int AS scheduled;

-- name: SeedKindConfig :exec
-- INSERT-IF-ABSENT a kind's operational settings (cap + resync), with values
-- mirroring the applied manifest (CRD). DO NOTHING on conflict so an operator's
-- runtime edit (or an existing row) is never clobbered — the manifest is the
-- SEED, the row is the truth. kind_config is the SINGLE source: the claim reads
-- max_inflight from it directly (no projection step), so this is the only write
-- that establishes a cap. It also seeds orphan_grace_secs (the orphan-grace
-- window) and finalizer_name (so the in-DB orphan sweep can pick soft-vs-hard
-- delete without a Go registry).
--
-- ON CONFLICT is DO NOTHING for the operator-editable columns (an operator's
-- runtime edit is the truth — never clobbered by a re-seed), with ONE exception:
-- finalizer_name is BACKFILLED when the existing row has it NULL and this seed
-- carries a non-null value. finalizer_name comes from the manifest, not operator-
-- editable, and the orphan sweep relies on it to choose soft-vs-hard delete — a
-- row that was created (e.g. by an operator config edit) before the finalizer was
-- ever seeded would otherwise keep finalizer_name NULL forever, making the sweep
-- HARD-delete a finalizer kind's orphan (leaking its real infra). The guarded
-- backfill is a no-op once set and never overwrites a present value, so it can't
-- fight the manifest; it only heals a NULL.
INSERT INTO kind_config (kind, kind_version, max_inflight, task_deadline_secs, resync_interval_secs, resync_recomposes, orphan_grace_secs, finalizer_name, max_transient_attempts, retired)
VALUES ($1, sqlc.arg('kind_version')::int, $2, $3, $4, $5, $6, $7, $8, sqlc.arg('retired')::boolean)
ON CONFLICT (kind, kind_version) DO UPDATE
    SET finalizer_name = EXCLUDED.finalizer_name
    WHERE kind_config.finalizer_name IS NULL AND EXCLUDED.finalizer_name IS NOT NULL;

-- name: UpsertKindConfig :exec
-- AUTHORITATIVE upsert of a kind's operational settings — the OPERATOR edit path
-- (vs SeedKindConfig's insert-if-absent seed). Overwrites cap + resync +
-- orphan_grace_secs. The cap change is LIVE: the claim reads kind_config
-- directly, so the next claim sees the new ceiling with no restart and no
-- reconcile. The task_deadline / resync / orphan-grace changes are picked up by
-- the worker/control plane/composer on their next tick / kind_config NOTIFY.
-- finalizer_name comes from the manifest, not operator-editable: it is COALESCEd
-- so a NULL param preserves the seeded value rather than wiping it. `retired` IS
-- operator-editable (this is the runtime retire/un-retire path): a version is
-- sunset by setting it true here (freeze-new / drain-existing) and revived by
-- setting it false, both live with no re-publish.
INSERT INTO kind_config (kind, kind_version, max_inflight, task_deadline_secs, resync_interval_secs, resync_recomposes, orphan_grace_secs, finalizer_name, max_transient_attempts, retired)
VALUES ($1, sqlc.arg('kind_version')::int, $2, $3, $4, $5, $6, $7, $8, sqlc.arg('retired')::boolean)
ON CONFLICT (kind, kind_version) DO UPDATE
    SET max_inflight           = EXCLUDED.max_inflight,
        task_deadline_secs     = EXCLUDED.task_deadline_secs,
        resync_interval_secs   = EXCLUDED.resync_interval_secs,
        resync_recomposes      = EXCLUDED.resync_recomposes,
        orphan_grace_secs      = EXCLUDED.orphan_grace_secs,
        max_transient_attempts = EXCLUDED.max_transient_attempts,
        retired                = EXCLUDED.retired,
        finalizer_name         = COALESCE(EXCLUDED.finalizer_name, kind_config.finalizer_name);

-- name: ListKindConfig :many
-- Every (kind, kind_version)'s operational settings — the set the CONTROL plane reads to
-- build the Resyncer kind map (resync_interval_secs > 0), one entry per active
-- kind_version. One small indexed scan of a handful of rows; off every hot path, and it
-- dials no client. (The claim reads max_inflight from kind_config directly by
-- (kind, kind_version); nobody copies it anywhere.)
SELECT kind, kind_version, max_inflight, task_deadline_secs, resync_interval_secs, resync_recomposes, orphan_grace_secs, finalizer_name, max_transient_attempts, retired
FROM kind_config
ORDER BY kind, kind_version;

-- name: ListConfiguredKindVersions :many
-- The distinct (kind, kind_version) set the cluster has an applied manifest for — the
-- DYNAMIC ProviderConfigCache's discovery source (it serves each worker's DEFAULT
-- providerconfig PER (kind, kind_version), so it must track the live pair set, not just
-- the kind set, to hand a v2 worker vpc/v2's default and a v1 worker vpc/v1's).
-- kind_config carries exactly one row per (kind, kind_version) — the PK — so this is
-- already distinct; a lean two-column scan of a handful of rows, off every hot
-- path. Ordered for a stable snapshot.
SELECT kind, kind_version
FROM kind_config
ORDER BY kind, kind_version;

-- name: GetKindConfig :one
-- One (kind, kind_version)'s operational settings by PK — the API's per-kind config panel
-- reads this (alongside the kind's default providerconfig) so an operator sees cap
-- + resync in one place. Index probe; off the hot path. No row → the (kind, kind_version)
-- is uncapped with no resync (the API renders defaults).
SELECT kind, kind_version, max_inflight, task_deadline_secs, resync_interval_secs, resync_recomposes, orphan_grace_secs, finalizer_name, max_transient_attempts, retired
FROM kind_config
WHERE kind = $1 AND kind_version = sqlc.arg('kind_version')::int;

