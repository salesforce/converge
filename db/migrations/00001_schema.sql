-- +goose Up
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
-- pg_trgm backs the GIN trigram index on resource_meta.name so the UI's
-- substring `name ILIKE '%foo%'` filter is index-driven instead of a
-- full table scan.
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- task_type names what the dispatcher should run for a given
-- work_queue row. Three slots:
--   reconcile -- runs the kind's full pipeline (compose? + work? + rollup?)
--   delete    -- runs the kind's Deleter; on success removes the finalizer
--   operate   -- runs a registered subresource verb against a
--                resource_operations row
CREATE TYPE task_type AS ENUM ('reconcile', 'delete', 'operate');

-- shard_of maps a resource id to its shard (0..255), the single source
-- of truth for the shard hash. resources.shard_id uses the same
-- expression as a GENERATED column; work_queue/work_outbox are RANGE-
-- partitioned by shard_id (a partition key can't be generated) so they
-- call this at every INSERT site instead. IMMUTABLE so the planner can
-- fold it to a constant and prune partitions when the argument is known.
-- Keep the body identical to the resources.shard_id expression and to
-- shardutil.NumShards=256 — engine startup fail-fasts if they drift.
--
-- abs(mod(h,256)), NOT mod(abs(h),256): hashtext returns a signed int4
-- and can return INT_MIN (-2147483648), whose abs() OVERFLOWS int4 →
-- "integer out of range", failing the INSERT for that resource. Taking
-- mod first (never overflows; yields (-256,256)) then abs (→ [0,255])
-- is overflow-safe and value-identical for every other input, since
-- mod keeps the dividend's sign and abs strips it either way.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION shard_of(rid UUID) RETURNS SMALLINT AS $$
    SELECT abs(mod(hashtext(rid::text), 256))::smallint;
$$ LANGUAGE sql IMMUTABLE;
-- +goose StatementEnd

-- spec_versions: append-only history of ROOT spec revisions — the
-- user-facing rollback / roll-forward (checkout) log. This is a DENORMALIZED
-- SIDE-LOG, deliberately OFF the hot path:
--   * The live desired state stays INLINE on resources.spec (unchanged from
--     the pre-history design), so the reconcile/drain/enqueue/claim/compose
--     hot paths never deref a second table — byte-identical to baseline.
--   * On a ROOT Apply (and only roots; owner_id IS NULL) we ALSO append a
--     copy of the body here. Composed children — the ~1M bulk — never write
--     to this table, so it adds ZERO cost to the compose/value-flow path
--     (children have no meaningful history anyway; they're re-derived every
--     compose).
--
-- A version is identified by (resource_id, generation): the generation the
-- root's spec produced. Checkout (rollback_to_spec) copies a chosen version's
-- body back INTO resources.spec in place and bumps generation — the live
-- spec is always the inline copy; this table is purely the navigable log.
-- source labels the writer (apply / rollback) for the history UI.
--
-- PLAIN + LOGGED, no FK (no-FK-on-hot-path convention, see resource_deps);
-- hard-delete cleanup + the gc_spec_history sweeper keep it bounded.
CREATE TABLE spec_versions (
    resource_id   UUID NOT NULL,
    generation    BIGINT NOT NULL,
    spec          JSONB NOT NULL,
    source        TEXT NOT NULL DEFAULT 'apply',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (resource_id, generation)
);

-- (resource_id, generation DESC): root history listing (newest-first) and
-- the GC keep-last-N ranking. The PK already leads with resource_id, but a
-- DESC index serves the newest-first ORDER BY without a sort.
CREATE INDEX idx_spec_versions_history ON spec_versions (resource_id, generation DESC);

-- providerconfigs is the runtime-editable CONFIG store (Crossplane's
-- ProviderConfig, adapted): per-kind documents that parameterise provider
-- behaviour WITHOUT a redeploy. A tiny, rarely-written table (a handful of
-- rows), deliberately SEPARATE from resources so config edits never touch the
-- ~1M-row reconcile hot path and configs don't flow through the work pipeline
-- (they are inert data that providers READ).
--
-- Two roles, distinguished by is_default:
--   * is_default = TRUE  → the kind's DEFAULT/bootstrap config. Loaded at boot
--     and CAPTURED by the worker; editing it live-reconfigures every worker
--     (the is_default-gated providerconfigs_changed NOTIFY drives the
--     reconfigure listener) with NO restart. At most ONE per kind, enforced by
--     the partial unique index below.
--   * is_default = FALSE → a per-resource CUSTOM override. A resource points at
--     it via resources.provider_config_id; at schedule time its spec is CLONED
--     into work_queue.provider_config (exactly like spec) and the worker merges
--     it over the default per field. Edits apply on the resource's NEXT
--     schedule — NOT live-pushed, so editing one never storms the reconfigure
--     channel.
--
-- spec is the opaque config document; its shape is the CONSUMER kind's
-- Capabilities.ConfigType (validated server-side by the API on write).
--
-- owner_id (added by ALTER below, after resources exists — the two tables
-- reference each other) is the OPTIONAL composer ownership link. A config a
-- Composer emits is owned by the composing resource (the composer root):
--   * lets the composer diff "the configs I produced last time" against its
--     fresh output (ListProviderConfigsByOwner) and upsert only the delta, the
--     same model as composed children/edges.
--   * ON DELETE CASCADE garbage-collects the config when its owner (the root)
--     is deleted, so a composition's configs never outlive it.
-- User/API-created configs leave owner_id NULL (unowned, never auto-deleted).
CREATE TABLE providerconfigs (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL UNIQUE,            -- global handle; provider_config_ref resolves by it
    kind        TEXT NOT NULL,                   -- the consumer kind this config parameterises
    -- kind_version is the web-API version of the consumer kind this config is FOR. A
    -- config is per-(kind, kind_version): a vpc/v1 config carries v1's config_schema shape,
    -- a vpc/v2 config v2's. `name` stays GLOBALLY unique (resources attach by name
    -- via provider_config_ref), so one name is pinned to one kind_version — a v2 config
    -- takes a distinct name. The resource-apply guard rejects attaching a config
    -- whose (kind, kind_version) doesn't match the resource's, so a v1 resource can never
    -- run a v2 config. Default is per-(kind, kind_version) too (see the unique index below).
    -- REQUIRED and explicit (>= 1): NO column DEFAULT — every providerconfig write
    -- must name the (kind, kind_version) it configures, never silently v1.
    kind_version       SMALLINT NOT NULL CHECK (kind_version >= 1),
    is_default  BOOLEAN NOT NULL DEFAULT FALSE,  -- TRUE = this (kind, kind_version)'s one live-reconfigurable default
    -- TWO orthogonal payloads, split by what a change MEANS:
    --   spec = OPERATIONAL config (timeouts, roles, retention, selectors-as-data).
    --          Structured JSONB so the DB can query/index it; merged default-over-
    --          override per resource (MergeConfigDoc). An edit is operational
    --          (live-pushed, no recompose).
    --   data = an OPAQUE provider BUNDLE the worker materialises and interprets
    --          (e.g. a zip of Starlark .star files; could be a tarball/wasm). BYTEA,
    --          not JSONB, on purpose: the DB never looks inside, byte-exact round-
    --          trip matters (a signature/checksum over the bytes stays valid, and
    --          jsonb would re-order/strip), and validating it as JSON buys nothing.
    --          Carried on BOTH the kind DEFAULT and per-resource CUSTOM configs:
    --          a custom config's data is cloned into work_queue.provider_bundle at
    --          schedule (exactly like spec) and REPLACES the default wholesale at
    --          the worker (opaque bytes can't deep-merge like spec's JSON; empty
    --          override falls back to the default). Per-resource determinism holds:
    --          a resource always materialises the same bundle.
    spec        JSONB NOT NULL DEFAULT '{}'::jsonb,
    data        BYTEA NOT NULL DEFAULT ''::bytea,
    owner_id    UUID,                            -- composer owner (FK added after resources); NULL = user/API-created
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- At most ONE default per (kind, kind_version): the config plane loads "the default for
-- (K, N)" and must get a single answer, and each kind_version has its own default (a
-- vpc/v2 resource merges over vpc/v2's default, not v1's). Partial UNIQUE on
-- (kind, kind_version) WHERE is_default lets a (kind, kind_version) have many custom configs but
-- only one default. Also serves the plane's default lookup as an index probe.
CREATE UNIQUE INDEX uq_providerconfigs_default_per_kind
    ON providerconfigs (kind, kind_version) WHERE is_default;

-- Trigram index on name so the provider-configs UI page can ILIKE '%term%'
-- search by name fast (mirrors idx_resource_meta_name_trgm). The plain UNIQUE
-- btree above only accelerates prefix/equality, not substring.
CREATE INDEX idx_providerconfigs_name_trgm ON providerconfigs USING gin (name gin_trgm_ops);

-- resources is the polymorphic table. Roots and owned children both
-- live here. K8s / Crossplane vocabulary, with TWO orthogonal axes
-- (this is the load-bearing distinction):
--   spec        = desired state (INLINE JSONB; root revision history is
--                 logged separately to spec_versions, off the hot path)
--   status      = observed state
--   generation  = bumped on every spec change (BEFORE-UPDATE trigger)
--
--   synced_gen  = the SYNCED axis (Crossplane "Synced", K8s
--                 observedGeneration). The generation the provider's
--                 pipeline most recently reconciled to. Advances ONLY
--                 on a successful full-pipeline reconcile; reset
--                 implicitly when bump_generation makes
--                 synced_gen < generation. Drives ALL of the DAG
--                 scheduling/gating machinery.
--
--   health_ok   = the READY/HEALTH axis (Crossplane "Ready",
--                 Available/Unavailable). Whether the resource is
--                 observed HEALTHY right now. Can flip true→false with
--                 NO generation bump — e.g. a periodic resync probe
--                 discovers an out-of-band failure hours after the spec
--                 last changed. Defaults true; only an explicit
--                 provider Ready=False (or a composite whose children
--                 went unready) sets it false.
--
-- is_ready collapses both axes for the fast path / partial indexes:
-- ready = synced AND healthy AND not deleting. A resource reconciled to
-- its current spec but later found unhealthy is is_ready=false even
-- though synced_gen >= generation — this is exactly the case a single
-- "did we reconcile the spec" flag could not express.
--
-- Rich, multi-axis K8s-style conditions (Synced/Ready/custom, with
-- reason+message+lastTransitionTime) live in the narrow side table
-- resource_conditions, written ONLY on transition and off the coalesced
-- hot-path UPDATE. The happy-path Synced=True / Ready=True is synthesized
-- by the API from these two scalars and needs no condition row at all.
--
-- finalizers + deletion_requested_at implement the K8s soft-delete
-- protocol: DELETE sets deletion_requested_at and seeds finalizers
-- from the kind's registered FinalizerName (provided by Go on the
-- delete request); providers remove their own string when cleanup
-- is done; the row is hard-deleted only when finalizers is empty.
--
-- root_id is denormalized (NULL on roots, root's id on owned rows).
-- Composer fills it once per child at insert time so the partial
-- index `(root_id) WHERE synced_gen < generation` answers
-- "are any descendants of this root still lagging?" in one probe.
--
-- last_reconciled_at is written ONLY by the opt-in resync sweeper
-- (requeue_for_resync) when it re-pends a settled resource for a drift
-- re-check. The hot reconcile/drainer path never touches it, and its
-- index is partial WHERE last_reconciled_at IS NOT NULL — so a
-- deployment with no resync-enabled kind leaves the column NULL on
-- every row and the index empty (zero maintenance cost).
--
-- shard_id is hash(id) % 256 STORED, used by the reaper and drainer
-- to scope sweeps to per-pod shard sets.
CREATE TABLE resources (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kind                TEXT NOT NULL,
    -- kind_version PINS the resource to a web-API-style version (vpc/v1, vpc/v2). It
    -- NEVER auto-moves: publishing vpc/v3 touches no v2 resource. The kind_version
    -- changes ONLY by an explicit uuid-stable flip (a user re-apply or a
    -- composer re-emit) that rewrites the spec to the new kind_version's shape and
    -- bumps generation. Broker/claim routing keys on (kind, kind_version) exact
    -- equality; work_queue.kind_version is denormalized from here at schedule time so
    -- the claim reads a concrete kind_version with no hot-path resolution. Not
    -- indexed here (the shard-scoped scheduling gates already prune); it rides
    -- along on the row and on work_queue. SMALLINT (2 bytes, not INT/4): kind versions
    -- are tiny web-API versions (v1, v2, …) that never approach 32767, so the
    -- narrower column halves the per-tuple cost on this heavily-UPDATEd 1M-row
    -- table (measured: INT here widened the 1M retry-variance window). REQUIRED and
    -- explicit (>= 1): NO column DEFAULT — an apply must supply the version (the
    -- API/store reject a missing/0 kind_version before this row is ever written).
    kind_version               SMALLINT NOT NULL CHECK (kind_version >= 1),
    -- name + labels live in the 1:1 resource_meta side table, not here:
    -- the reconcile/drain/cascade/schedule path is UUID-keyed and never
    -- reads them, so keeping them off this frequently-UPDATEd row keeps
    -- its tuples small. Only create/compose writes and user-facing reads
    -- touch resource_meta.
    owner_id            UUID REFERENCES resources(id) ON DELETE CASCADE,
    root_id             UUID,

    spec                JSONB NOT NULL DEFAULT '{}',
    status              JSONB,

    -- provider_config_id: optional per-resource CUSTOM config override —
    -- points at a row in the providerconfigs table (a NON-default config; the
    -- kind default applies automatically). NULL (the overwhelming common case)
    -- means "no custom config — the provider uses its kind DEFAULT only".
    -- When set, that config's spec is CLONED into work_queue.provider_config
    -- the moment a task is scheduled (schedule_eligible / cascade / resync /
    -- delete / operate), and at run time it OVERRIDES the kind default for that
    -- one task. The resource's own `spec` is never touched and never overrides
    -- config — config and spec are orthogonal axes. ON DELETE SET NULL so
    -- dropping a config cleanly demotes its consumers back to the kind default
    -- instead of cascade-deleting them. Backed by a partial index
    -- (idx_resources_provider_config) so the SET NULL fan-out on a config
    -- delete is an index probe, not a 1M seq scan.
    provider_config_id  UUID REFERENCES providerconfigs(id) ON DELETE SET NULL,

    generation          BIGINT NOT NULL DEFAULT 1,
    synced_gen          BIGINT NOT NULL DEFAULT 0,

    -- health_ok is the Ready/health axis. Defaults true so a never-yet-
    -- probed resource is considered healthy once synced; an explicit
    -- Ready=False from a provider (or a composite rollup that found
    -- unready children) flips it false, demoting is_ready without any
    -- spec/generation change.
    health_ok           BOOLEAN NOT NULL DEFAULT true,

    -- composed_gen tracks the generation the composer last produced
    -- children + edges for. Lets the worker skip the composer stage on
    -- a re-pend caused by "still waiting on descendants" (rollup gate
    -- not satisfied). Without this, every reaper tick re-runs the full
    -- 1M-child compose just to discover nothing changed. Composer-less
    -- kinds never advance this column; it stays 0.
    composed_gen        BIGINT NOT NULL DEFAULT 0,

    -- manifest_version is the kind_manifest content hash this resource is pinned
    -- to, denormalized here (write-cold) so the schedule/cascade INSERT into
    -- work_queue reads it from the row it already scans — no per-row kind_config
    -- join on the hottest path. Stamped at create/compose from the kind's current
    -- kind_config.manifest_version, and re-stamped for every row of a kind when
    -- its CRD is applied (sync_kind_config_from_manifest UPDATEs resources). The
    -- enqueued work_queue row carries it forward to fence a commit against a
    -- mid-flight manifest edit. 0 = pinned before any manifest applied (inert).
    manifest_version    BIGINT NOT NULL DEFAULT 0,

    -- failure_gen is the FAILED axis as a scalar: the generation a
    -- reconcile last HARD-FAILED at (the Crossplane Synced=False axis,
    -- made directly queryable instead of inferred from a condition row).
    -- 0 = never failed. failure_gen = generation  ⟺  currently failed for
    -- the live spec. A spec change bumps generation, so a stale failure
    -- auto-invalidates with NO write and NO GC — the comparison is to the
    -- live generation, exactly like the synced_gen axis. The drainer
    -- stamps it on a failed reconcile and clears it (→0) on a success that
    -- advances synced_gen. This is the one bit the two existing axes can't
    -- express: "this not-ready row is broken, not merely in-flight."
    failure_gen         BIGINT NOT NULL DEFAULT 0,

    -- failure_terminal distinguishes a TERMINAL failure (won't succeed
    -- without a spec change — a malformed spec / CompositionFailed / a
    -- permanent provider rejection) from a transient one. When true AND
    -- failure_gen = generation, the scheduler/reaper STOP re-queuing the
    -- row: it stays phase=Failed until the user edits the spec
    -- (bump_generation makes failure_gen <> generation, so this is moot
    -- again). Set from the provider via framework.Terminal(); cleared
    -- with failure_gen on the next success. Default false → every failure
    -- is retryable unless the provider explicitly says otherwise.
    failure_terminal    BOOLEAN NOT NULL DEFAULT false,

    -- failure_attempts is the DURABLE transient-failure counter for the
    -- poison-pill: it counts consecutive TRANSIENT reconcile failures across
    -- retry cycles. It MUST live here on resources, not on work_queue, because a
    -- transient failure DELETEs the work_queue row (drain) and re-pends a FRESH
    -- one (requeue → schedule_eligible), which would reset any work_queue-local
    -- counter to its default every cycle — so the cap was never reached and a
    -- broken handler retried forever. The drain increments this on each transient
    -- failure and, when it reaches the kind's max_transient_attempts, escalates
    -- the failure to TERMINAL (dead-letter). Reset to 0 on any synced_gen-advancing
    -- success and by bump_generation on a spec change (a new generation is a fresh
    -- start). 0 = no consecutive transient failures for the live generation.
    failure_attempts    INT NOT NULL DEFAULT 0,

    -- Generated: ready iff the pipeline reconciled the current
    -- generation (synced) AND the resource is observed healthy AND no
    -- delete has been requested. STORED so the partial indexes below
    -- can use it.
    is_ready            BOOLEAN NOT NULL
        GENERATED ALWAYS AS (
            synced_gen >= generation AND health_ok AND deletion_requested_at IS NULL
        ) STORED,

    -- phase is the SINGLE source of truth for readiness state — the one
    -- value every list row, count, filter, and detail view keys off, so
    -- DB / API / UI are structurally incapable of disagreeing: no layer
    -- re-derives readiness independently. Seven states, fixed precedence,
    -- evaluated top-down over scalars ALREADY on the row — no join, no
    -- JSON, no subquery:
    --
    --   Deleting    — soft-delete in progress (highest precedence: once the
    --                 finalizer drain has started, deletion_requested_at wins)
    --   Quarantined — an operator set this FAILED resource aside (frozen_until = 'infinity').
    --                 Above Failed because quarantine is the explicit "I've taken
    --                 this off the retry loop" acknowledgement — the surfaced
    --                 state is "set aside", not "still failing". Frozen: no
    --                 retry, no resync, no resurrection by a dep's status flow;
    --                 excluded from its root's rollup. Cleared by un-quarantine
    --                 or a spec edit. NOT deleted by the reaper (still desired).
    --   Failed      — failure_gen = generation (a hard reconcile error
    --                 for the live spec; Crossplane Synced=False)
    --   Orphaned    — the composer no longer produces this child and it is in
    --                 the grace window (finite frozen_until, NOT yet draining).
    --                 Below Failed so a failed-AND-orphaned child shows the root
    --                 cause; above Degraded/Ready so a synced-healthy orphan
    --                 reads 'Orphaned' (it LEFT the composition) rather than
    --                 masquerading as Ready. A re-emit clears frozen_until → the
    --                 row returns to its live phase.
    --   Degraded    — synced to spec but observed unhealthy (Ready=False:
    --                 a health probe demoted it, or a composite's children
    --                 are unready → reason=ChildrenNotReady)
    --   Ready       — synced AND healthy (== is_ready when not deleting)
    --   Reconciling — otherwise (spec change in flight / never reconciled)
    --
    -- TEXT, not an enum: avoids ALTER TYPE churn during WIP; the precedence
    -- lives ONLY in this CASE. STORED so the failed partial index can use
    -- it. Recomputed only when the row is physically written — exactly the
    -- rows the coalesced drainer UPDATE already touches; the no-op gate
    -- that protects the 1M hot path protects phase too (a skipped row is
    -- never recomputed). is_ready is deliberately NOT changed for an orphan or
    -- a quarantined row: set-aside-ness is composition/operational metadata, not
    -- the resource's own readiness — its is_ready reflects its last real state.
    phase               TEXT NOT NULL
        GENERATED ALWAYS AS (
            CASE
                WHEN deletion_requested_at IS NOT NULL          THEN 'Deleting'
                -- frozen_until = 'infinity' ⇒ quarantined (operator set aside);
                -- a finite frozen_until ⇒ orphaned (grace, pending teardown).
                -- Quarantined ranks above Failed (the surfaced truth is "set
                -- aside", not "still failing"); Orphaned ranks below Failed so a
                -- failed-AND-orphaned child shows the root cause. The 'infinity'
                -- branch MUST precede the IS NOT NULL branch (infinity is non-NULL).
                WHEN frozen_until = 'infinity'                  THEN 'Quarantined'
                WHEN failure_gen = generation                   THEN 'Failed'
                WHEN frozen_until IS NOT NULL                   THEN 'Orphaned'
                WHEN synced_gen >= generation AND NOT health_ok THEN 'Degraded'
                WHEN synced_gen >= generation                   THEN 'Ready'
                ELSE 'Reconciling'
            END
        ) STORED,

    finalizers              TEXT[] NOT NULL DEFAULT '{}',
    deletion_requested_at   TIMESTAMPTZ,

    -- frozen_until: the ONE column encoding both FROZEN states — orphaned and
    -- quarantined. A frozen row is skipped by every scheduler and excluded from
    -- its root's rollup/descendant gate; the difference is only WHEN (and IF) the
    -- reaper tears it down, which the VALUE distinguishes:
    --
    --   * ORPHANED (a finite future instant = now() + orphan_grace_secs):
    --     the composer dropped this child but the kind has grace > 0, so instead
    --     of deleting it the prune stamps frozen_until = now() + grace, leaving
    --     the row in the DAG. A re-emit CLEARS it (re-adoption — no teardown).
    --     sweep_expired_orphans escalates it to a real delete once frozen_until
    --     has PASSED (frozen_until < now()). Drives phase='Orphaned'.
    --
    --   * QUARANTINED ('infinity'::timestamptz): an operator set a FAILED
    --     resource ASIDE. 'infinity' is NEVER < now(), so the reaper NEVER sweeps
    --     it — quarantine is a permanent freeze, not a pending teardown — with NO
    --     separate predicate needed (the sweep's `frozen_until < now()` excludes
    --     it for free). Cleared only by an explicit un-quarantine (a spec edit
    --     bumps generation but does NOT clear it). Drives phase='Quarantined'.
    --
    -- One column ⇒ ONE partial index (idx_resources_frozen_sweep) carries both
    -- states, vs. two indexes before — and a synced_gen-advancing UPDATE is
    -- NON-HOT, so it re-inserts into EVERY resources index; halving the
    -- freeze-state index count halves that per-write tax (measured ~5s/1M).
    -- NULL for a live row; off the is_ready/shard hot path. The freeze test
    -- everywhere is the single predicate `frozen_until IS NULL`.
    frozen_until            TIMESTAMPTZ,

    -- Written only by the resync sweeper; see table comment above.
    last_reconciled_at  TIMESTAMPTZ,

    -- labels live in resource_meta (see below).

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- abs(mod(...)) not mod(abs(...)): hashtext can return INT_MIN, whose
    -- abs() overflows int4. mod-first is overflow-safe and value-identical.
    -- Must stay in lockstep with shard_of() — verifyShardModulus checks both.
    shard_id            SMALLINT NOT NULL
        GENERATED ALWAYS AS (abs(mod(hashtext(id::text), 256))) STORED
)
WITH (
    -- resources is the one large, LONG-LIVED, heavily-UPDATEd table: every
    -- reconcile completion rewrites a row (status / synced_gen / health_ok),
    -- and because the lagging/failed partial indexes' predicates reference
    -- synced_gen / failure_gen / generation, a synced_gen-advancing update is
    -- NON-HOT — it creates a dead tuple AND re-inserts into EVERY resources
    -- index. On the cluster default (autovacuum at 20% dead) this table bloats
    -- fast during a fan-out burst and its planner stats go stale, which
    -- directly hurts the substitute pass + schedule_eligible plans (both are
    -- selectivity-sensitive). So tune it as aggressively as the UNLOGGED work
    -- tables: vacuum/analyze early so dead tuples are reclaimed and stats stay
    -- fresh. cost_limit/cost_delay give vacuum a high IO budget so it keeps up
    -- with the burst instead of falling behind.
    --
    -- fillfactor 90 leaves in-page headroom so the HOT-ELIGIBLE updates — the
    -- status-only intermediates (advance_synced_gen=false while waiting on
    -- descendants) and health_ok-only flips, neither column indexed — chain
    -- HOT and skip the all-index re-insert entirely. The synced_gen-advancing
    -- updates can't be HOT regardless (indexed predicate), and lose nothing
    -- from the 10% headroom.
    fillfactor = 90,
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 50,
    autovacuum_analyze_threshold = 50,
    autovacuum_vacuum_cost_delay = 10,
    autovacuum_vacuum_cost_limit = 1000
);

-- (owner_id): serves the child-count correlated subquery
-- (SELECT count(*) FROM resources c WHERE c.owner_id = r.id) on the root
-- list page, GetChildrenByOwner, and CountChildrenByReadiness. No query
-- carries an is_ready predicate/sort here, so the index is on owner_id
-- alone (was (owner_id, is_ready) — the trailing column was never used).
CREATE INDEX idx_resources_owner ON resources (owner_id);

-- Partial (shard_id) WHERE synced_gen < generation — drives the reaper's
-- requeue_failed_and_pending sweep.
--
-- NOTE: deliberately the BASELINE definition (no trailing id, no frozen
-- predicate). An attempt to bake `frozen_until IS NULL` into this WHERE (to make
-- the gate a pure index probe) instead CAUSED a hard 40P01 deadlock storm on the
-- 1M post-commit schedule — the narrower index shifted the planner's FK-lock
-- order. The freeze predicate lives in the query WHERE only (cheap heap filter);
-- this index stays wide. See the requeue_failed_and_pending / schedule_eligible
-- gates.
CREATE INDEX idx_resources_shard_lagging
    ON resources (shard_id) WHERE synced_gen < generation;

-- Partial (root_id) WHERE synced_gen < generation — used by the worker's
-- "are descendants ready?" check at rollup time, and by future
-- subgraph-eligibility queries.
--
-- NOTE: BASELINE definition (no trailing id, no frozen predicate) — same
-- reason as idx_resources_shard_lagging above: baking the freeze predicate into
-- this index's WHERE deadlocked the 1M schedule. The descendant gate keeps
-- `frozen_until IS NULL` as a query-side filter (a small per-row heap re-check)
-- rather than in the index — correctness preserved, no deadlock.
CREATE INDEX idx_resources_root_lagging
    ON resources (root_id) WHERE synced_gen < generation;

-- Full (root_id, created_at, id) — drives ListDescendants, which the
-- worker's status-rollup stage runs once a root's descendants are all
-- ready. The partial idx_resources_root_lagging above can't serve it
-- (it excludes the caught-up rows rollup actually needs), so without
-- this index ListDescendants falls back to a seq scan of resources.
-- The leading column is the equality predicate; created_at, id match
-- the query's ORDER BY so the sort is satisfied by the index. Measured:
-- no detectable bulk-insert overhead vs. the existing index set.
CREATE INDEX idx_resources_root_descendants
    ON resources (root_id, created_at, id);

-- Partial (kind, last_reconciled_at) WHERE last_reconciled_at IS NOT NULL
-- — drives requeue_for_resync's "which already-probed resources of this
-- kind are due for another drift re-check?" scan. PARTIAL on
-- last_reconciled_at IS NOT NULL so it is completely EMPTY in a
-- deployment where no kind opts into resync (every row's
-- last_reconciled_at stays NULL) — zero entries, zero maintenance on the
-- hot reconcile path. Never-yet-probed rows (NULL) are found via
-- idx_resources_kind_pagination — but only once a full ResyncInterval has
-- elapsed since they settled (the gate baselines NULL rows on updated_at),
-- so they aren't swept the instant they settle; they enter this index once
-- stamped.
CREATE INDEX idx_resources_resync
    ON resources (kind, last_reconciled_at) WHERE last_reconciled_at IS NOT NULL;

-- NB: there is intentionally NO plain (created_at, id) pagination index. All
-- list pages ORDER BY created_at DESC and are served by idx_resources_kind_pagination
-- / idx_resources_roots_pagination; ListDescendants scans by root_id via
-- idx_resources_root_descendants (its created_at sort is a tiny in-subtree sort).
-- A plain (created_at,id) index would only ADD a forced re-insert to every
-- non-HOT status write at 1M scale with no query consumer.
CREATE INDEX idx_resources_kind_pagination
    ON resources (kind, created_at DESC, id DESC);

-- Partial (created_at DESC, id DESC) WHERE failure_gen = generation —
-- drives the failed-only filter (phase='Failed') and the failed count
-- bucket. PARTIAL on the failure predicate so in a healthy 1M deployment
-- it holds ZERO entries: INSERT/UPDATE of a healthy row does no
-- maintenance on it (Postgres skips a partial index whose predicate the
-- new tuple fails), same discipline as idx_resources_resync. Replaces the
-- old correlated EXISTS(resource_conditions WHERE status='False') subplan
-- with an indexed scalar compare.
CREATE INDEX idx_resources_failed
    ON resources (created_at DESC, id DESC) WHERE failure_gen = generation;

-- Partial (frozen_until) WHERE frozen_until IS NOT NULL — the SINGLE index for
-- BOTH frozen states (orphaned = finite, quarantined = 'infinity'). Drives the
-- reaper's sweep_expired_orphans idle gate + scan (`frozen_until < now()`, which
-- ranges over orphans and never matches the 'infinity' quarantined rows). Same
-- empty-partial discipline as idx_resources_failed: in steady state nothing is
-- frozen, so it holds ZERO entries and costs nothing on the hot reconcile/compose
-- path; it gains entries only during an orphan-prune spike or an operator
-- quarantine. frozen_until leads so the "expired in my shard range" sweep is an
-- index range. (Collapsed from the two former idx_resources_orphan_sweep +
-- idx_resources_quarantined indexes — measured ~5s/1M cheaper per the non-HOT
-- write path re-inserting into every index; the dead quarantined index, which
-- had no query consumer, is gone outright.)
CREATE INDEX idx_resources_frozen_sweep
    ON resources (frozen_until) WHERE frozen_until IS NOT NULL;

-- Partial (shard_id) WHERE deletion_requested_at IS NOT NULL — drives sweep_deletable's
-- zero-cost idle gate + range scan (the reaper backstop that removes a marked teardown
-- tree bottom-up once each node's finalizers + children + dependents are gone). Same
-- empty-partial discipline as the frozen/failed indexes: in steady state nothing is
-- being deleted, so it holds ZERO entries and costs nothing on the hot reconcile/compose
-- path; a row enters only while it is actively tearing down and leaves when removed.
CREATE INDEX idx_resources_deleting
    ON resources (shard_id) WHERE deletion_requested_at IS NOT NULL;

CREATE INDEX idx_resources_roots_pagination
    ON resources (created_at DESC, id DESC) WHERE owner_id IS NULL;

-- Partial (provider_config_id) WHERE provider_config_id IS NOT NULL — backs
-- the ON DELETE SET NULL fan-out when a providerconfigs row is deleted
-- (find-and-null its consumers without a 1M seq scan) and the "which
-- resources reference config X" admin query. PARTIAL on the NOT NULL
-- predicate so the overwhelmingly-common no-custom-config rows hold ZERO
-- entries: applying/updating a normal resource does no maintenance on it.
CREATE INDEX idx_resources_provider_config
    ON resources (provider_config_id) WHERE provider_config_id IS NOT NULL;

-- The ONE deliberate ALTER in this schema (everything else is defined in its
-- final shape in place): providerconfigs.owner_id FK is added HERE, not inline in
-- the providerconfigs table above, because the two tables reference each other in
-- a CYCLE (resources → providerconfigs via provider_config_id, AND providerconfigs
-- → resources via owner_id) and resources doesn't exist yet when providerconfigs
-- is created. One side of a circular FK MUST be added after both tables exist —
-- it cannot be inlined. ON DELETE CASCADE: deleting a composer root removes the
-- configs it owns. The partial index backs both the CASCADE delete fan-out (find
-- an owner's configs without a seq scan) and the composer's by-owner diff
-- (ListProviderConfigsByOwner); PARTIAL on NOT NULL so unowned (user/API) configs
-- hold no entries.
ALTER TABLE providerconfigs
    ADD CONSTRAINT providerconfigs_owner_fk
    FOREIGN KEY (owner_id) REFERENCES resources(id) ON DELETE CASCADE;
CREATE INDEX idx_providerconfigs_owner
    ON providerconfigs (owner_id) WHERE owner_id IS NOT NULL;

-- resource_meta: 1:1 side table holding the identity/metadata columns
-- (name, labels) that the reconcile path doesn't read, kept out of the
-- frequently-UPDATEd resources row so its tuples stay small. id is the
-- shared PK (FK → resources, ON DELETE CASCADE keeps the two in lockstep
-- — deleting a resource or its parent removes the meta row).
--
-- kind is "duplicated" from resources.kind but load-bearing, not
-- redundant: the global UNIQUE(kind,name) must be a single-table index,
-- and name lives here; uniqueness on name alone would be wrong (different
-- kinds may share a name). Immutable, cheap (short TEXT).
-- No created_at/updated_at here: resources already owns the row's
-- timestamps (the API/queries read those), and nothing consumes a
-- meta-local timestamp — carrying them would be dead bytes + a pointless
-- write on every label edit.
CREATE TABLE resource_meta (
    id           UUID PRIMARY KEY REFERENCES resources(id) ON DELETE CASCADE,
    kind         TEXT NOT NULL,
    name         TEXT NOT NULL,
    labels       JSONB NOT NULL DEFAULT '{}'
);

-- Global name uniqueness: one (kind, name) per Converge instance,
-- root OR child alike (k8s/Crossplane model). Backs "Apply Spec" keyed by
-- (kind, name) and the composer's ON CONFLICT (kind, name) arbiter.
-- Composers emit globally-unique child names (a composer typically prefixes by its root's
-- deployment_instance name); a collision is a hard upsert error.
CREATE UNIQUE INDEX uniq_resource_meta ON resource_meta (kind, name);

-- Label filter (UI/page): GIN over labels.
CREATE INDEX idx_resource_meta_labels ON resource_meta USING gin (labels jsonb_path_ops);

-- Name substring search (UI): global trigram over name. The roots-only
-- name search filters resources.owner_id IS NULL (a different table) and
-- reuses this same global trigram index, so no roots-specific partial is
-- needed.
CREATE INDEX idx_resource_meta_name_trgm ON resource_meta USING gin (name gin_trgm_ops);

-- NO bump_generation trigger. Generation is advanced EXPLICITLY by each of
-- the (few) spec-writing UPDATE sites — UpsertResource's upd_res, the
-- composer's child update, the two value-flow substitute passes, and
-- rollback_to_spec — every one of which already knows the spec changed
-- (gated by IS DISTINCT FROM, or an intentional patch/checkout). A
-- BEFORE-UPDATE trigger would re-do OLD.spec IS DISTINCT FROM NEW.spec — a
-- deep compare of the full (TOASTed, possibly-MB) body — on EVERY resources
-- UPDATE, including the drainer's coalesced status write that never touches
-- spec. Bumping at the source removes that redundant compare from the hot
-- status path entirely and avoids a second deep compare on apply.
-- is_ready is GENERATED so it follows automatically once synced_gen falls
-- behind generation; no manual reset needed.
--
-- INVARIANT: generation bumps iff spec content changed. Every spec-writing
-- UPDATE that sets `spec` MUST also set `generation = generation + 1`
-- (under the same change-gate), and NO other UPDATE may. The drainer status
-- write deliberately does NOT bump (it writes status/synced_gen, not spec).

-- resource_deps: directed edges between resources, dependent → dependency.
-- value_flows is the optional metadata describing what spec values get
-- filled from the upstream's status. Each element is
-- {"dep_field":"/account_id","src_field":"/account_id"}.
--
-- No `substituted` flag — the dep-gate (`p.synced_gen < p.generation`)
-- closes the bootstrap window: a dependent can't be scheduled while
-- upstream lags, and a SELECT FOR SHARE during compose-time inline
-- substitution prevents the upstream from flipping mid-tx.
--
-- No FKs on hot path. owner_id ON DELETE CASCADE on resources still
-- tears down the subtree; the cleanup_orphaned_deps statement-level
-- trigger sweeps orphaned edges + work_queue rows in one shot.
CREATE TABLE resource_deps (
    dependent_id  UUID NOT NULL,
    dependency_id UUID NOT NULL,
    value_flows   JSONB NOT NULL DEFAULT '[]'::jsonb,
    -- src_kind_version records the upstream (dependency) kind_version each value_flow pointer
    -- was validated against. The edge is uuid-keyed and version-blind, so when
    -- an upstream flips kind_version and renames/relocates a status field, the
    -- dependent's src_field pointer silently resolves NULL and the flow is
    -- skipped with no error. FlipResourceKindVersion compares this to the new kind_version
    -- and raises a WARN on skew so the operator sees a stale value-flow instead
    -- of a silent stale value. 0 = never recorded; treated as
    -- "no known skew" so such edges keep working until first re-validated.
    src_kind_version     INT NOT NULL DEFAULT 0,
    PRIMARY KEY (dependent_id, dependency_id)
);

CREATE INDEX idx_resource_deps_dependency ON resource_deps(dependency_id);
CREATE INDEX idx_resource_deps_dependent ON resource_deps(dependent_id);

-- cleanup_orphaned_deps: AFTER DELETE STATEMENT trigger. One set-based
-- DELETE per dependent table on owner_id CASCADE.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION cleanup_orphaned_deps() RETURNS trigger AS $$
BEGIN
    DELETE FROM resource_deps d
     USING old_rows o
     WHERE d.dependent_id = o.id OR d.dependency_id = o.id;
    DELETE FROM work_queue q
     USING old_rows o
     WHERE q.resource_id = o.id AND q.shard_id = shard_of(o.id);
    -- Reclaim spec_versions history of deleted ROOTS (no FK — see the
    -- resource_deps no-FK-on-hot-path rationale). One set-based delete over
    -- the just-removed ids; children have no rows here so it's a no-op for
    -- the 1M subtree teardown.
    DELETE FROM spec_versions s
     USING old_rows o
     WHERE s.resource_id = o.id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER cleanup_orphaned_deps
    AFTER DELETE ON resources
    REFERENCING OLD TABLE AS old_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION cleanup_orphaned_deps();

-- work_queue: per-resource × task_type queue.
-- UNIQUE(resource_id, task_type) lets operate coexist with reconcile
-- on the same resource; reconcile/delete serialize through the unique
-- constraint because the dispatcher only schedules one of those at a
-- time per resource.
--
-- spec on this row is the snapshot at claim time so the provider
-- reads stable input even if the resource's spec is updated mid-task.
-- NULL for task types whose pipeline ignores spec.
--
-- shard_id is hash(resource_id) % 256, matching resources.shard_id and
-- work_outbox.shard_id.
--
-- UNLOGGED: source of truth is resources.synced_gen vs generation; on
-- crash, reaper re-pends rows whose work hasn't finished.
--
-- RANGE PARTITIONED BY shard_id into 16 partitions of 16 contiguous
-- shards each (0-15, 16-31, … 240-255). Under high writer counts
-- (200-500 workers) a single heap + single claim index funnels every
-- insert/claim/delete onto a small set of shared physical pages — the
-- LWLock/BufferContent + Lock/extend contention measured in the 1M
-- stress test (WorkQueueTakeBatch was the #1 sink at ~21ms/claim under
-- 200 workers).
--
-- WHY RANGE(shard_id) AND NOT HASH(resource_id): workers own *contiguous*
-- shard ranges (shardutil.ShardsForPod) and claim with
-- `shard_id BETWEEN lo AND hi` (the worker's range). A bound range
-- predicate lets the planner PRUNE each claim to the single partition
-- that holds those shards, so a claim locks exactly one heap + one index.
-- Two non-prunable alternatives were measured slower: HASH(resource_id)
-- can't prune at all (predicate is on shard_id, not resource_id), and
-- even RANGE(shard_id) with `shard_id = ANY($array)` doesn't get runtime
-- pruning — both fan every claim out to all 16 partitions, take 16× the
-- relation locks, and overflow the lock-manager fast-path →
-- LWLock/LockManager became the #1 wait and the run got *slower*. The
-- bound-range BETWEEN form is the one that wins. See db/queries/work_queue.sql.
--
-- shard_id is a PLAIN column (not GENERATED) because a partition key
-- cannot be a generated column. It's populated explicitly at every
-- INSERT site via shard_of(resource_id). The partition key must appear
-- in every unique/PK constraint, hence PK=(id, shard_id) and
-- UNIQUE=(resource_id, task_type, shard_id) — shard_id is functionally
-- determined by resource_id, so uniqueness semantics are unchanged.
CREATE UNLOGGED TABLE work_queue (
    id            UUID NOT NULL DEFAULT gen_random_uuid(),
    resource_id   UUID NOT NULL,
    task_type     task_type NOT NULL,
    op_id         UUID,
    kind          TEXT NOT NULL,
    generation    BIGINT NOT NULL,
    spec          JSONB,
    -- provider_config: the per-task CLONE of the resource's CUSTOM config
    -- (resources.provider_config_id → that providerconfigs row's spec),
    -- snapshotted at schedule time so the task carries a stable override even if
    -- the config is edited mid-flight. NULL (default) means "no custom override —
    -- the provider uses its kind default config only". At run time the worker
    -- merges kind-default ⊕ this column (this column wins per field). It is a
    -- pure config axis, fully independent of `spec`.
    provider_config JSONB,
    -- provider_bundle: the per-task CLONE of the resource's CUSTOM config BUNDLE
    -- (providerconfigs.data — the opaque artifact, e.g. a zip of .star files),
    -- snapshotted at schedule time exactly like provider_config. NULL (the common
    -- case) means "no bundle override — use the kind default bundle". Unlike
    -- provider_config it does NOT field-merge: at run time it REPLACES the default
    -- bundle wholesale (opaque bytes can't deep-merge), empty falling back to the
    -- default. bytea, not jsonb, for the same byte-exact reasons as
    -- providerconfigs.data.
    provider_bundle BYTEA,
    attempts      INT NOT NULL DEFAULT 1,
    -- broker_id is the CLAIM LEASE holder — the pod that took this row (the BROKER
    -- in a fanned-out deploy, or the in-process control/broker for a local run).
    -- It is the reaper's stale-reclaim key and the AppendOutbox fence identity; it
    -- is NOT the process that runs the provider stage (that is worker_id, below).
    broker_id     TEXT,
    -- worker_id is the DUMB WORKER that actually ran the stage (its friendly
    -- hostname/pod name), set by the broker at dispatch when it hands the task to a
    -- connected worker over the WorkStream. NULL means "not yet dispatched to a
    -- remote worker" (or an in-process run, where broker_id already IS the executor).
    -- Attribution only — surfaced in the UI "running on <worker>" card; never fenced
    -- on, never read by the reaper. Cleared to NULL on release/reclaim with broker_id.
    worker_id     TEXT,
    heartbeat_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- manifest_version PINS the kind_manifest content hash this task was enqueued
    -- under (stamped from kind_config.manifest_version by schedule_eligible /
    -- cascade / requeue). The fenced commit paths (StampComposedGen, AppendOutbox)
    -- check it so a task claimed under an old manifest cannot commit against a
    -- changed reaction set — the commit no-ops and the task is re-enqueued under
    -- the current manifest. 0 = enqueued before any manifest applied (inert: the
    -- fences treat 0 as "don't check").
    manifest_version BIGINT NOT NULL DEFAULT 0,
    -- kind_version RIDES ALONG from resources.kind_version (stamped by schedule_eligible /
    -- cascade / resync / sweep at enqueue). It is the web-API version the broker
    -- routes on: the broker's HasSubscriber(kind, kind_version) gate refuses to
    -- dispatch this task unless a connected worker serves that (kind, kind_version), so
    -- a v1 task can never land on a v2 worker. Deliberately NOT part of the
    -- pending-claim index key (idx_work_queue_pending stays (kind, task_type,
    -- shard_id)) — the broker's in-RAM kind_version gate runs BEFORE the claim and the
    -- claim itself stays exact-equality-sargable; kind_version is just carried on the
    -- claimed row to the worker in the StageTask. SMALLINT (2 bytes): this table
    -- churns hard (insert→claim→delete per task, ~1M rows/run), so the narrower
    -- column matters; kind versions never approach 32767. NO column DEFAULT — every
    -- enqueue path SELECTs kind_version from the (NOT NULL) resources row, so a task
    -- always carries the resource's explicit version, never a silent v1.
    kind_version         SMALLINT NOT NULL,
    -- claim_epoch is a strict monotonic per-row fencing token. It is the SOLE
    -- ownership fence on every result-write path (AppendOutbox, StampComposedGen)
    -- for ALL task types, replacing an identity check with an owner-independent
    -- one: a task carries the epoch it was claimed under in its StageTask, and the
    -- write lands only if the row still bears that exact epoch. Every path that
    -- makes the row (re)claimable — the claim itself, a schedule_eligible re-point,
    -- a reaper stale-reclaim, a scoped release — bumps it by one in the SAME
    -- statement that frees the claim, so a stale/zombie/misrouted result carries an
    -- OLDER epoch and matches nothing. Because the write need not be done by the
    -- claiming broker, any broker holding the current-epoch token writes the fenced
    -- outbox row directly (a worker that reconnects to a different broker, or a peer
    -- that executed a forwarded task). Starts at 1 (never 0) so a fresh, never-
    -- reclaimed row is already strictly fenced — critical for operate/delete, which
    -- have no generation gate to fall back on. Exclusivity is UNCHANGED (SKIP LOCKED
    -- + the broker_id IS NULL flag elects the claim winner before this is written);
    -- claim_epoch fences the WRITE, not the claim.
    claim_epoch          BIGINT NOT NULL DEFAULT 1,
    shard_id      SMALLINT NOT NULL,
    PRIMARY KEY (id, shard_id),
    UNIQUE (resource_id, task_type, shard_id)
) PARTITION BY RANGE (shard_id);

-- 16 range partitions × 16 shards. Per-partition storage params:
-- autovacuum knobs can't be set on a partitioned parent (no heap), so
-- each leaf carries them. Tuned aggressively because these UNLOGGED
-- tables churn hard (insert→claim→delete per task); a slow autovacuum
-- lets dead tuples bloat the claim index and re-introduce page contention.
-- +goose StatementBegin
DO $$
DECLARE i INT;
BEGIN
    FOR i IN 0..15 LOOP
        EXECUTE format(
            'CREATE UNLOGGED TABLE work_queue_p%s PARTITION OF work_queue '
            || 'FOR VALUES FROM (%s) TO (%s) '
            || 'WITH (autovacuum_vacuum_scale_factor = 0.05, '
            || 'autovacuum_analyze_scale_factor = 0.02, '
            || 'autovacuum_vacuum_threshold = 50, '
            || 'autovacuum_analyze_threshold = 50, '
            || 'autovacuum_vacuum_cost_delay = 10, '
            || 'autovacuum_vacuum_cost_limit = 1000)',
            i, i*16, (i+1)*16);
    END LOOP;
END $$;
-- +goose StatementEnd

-- Claim index: (kind, task_type, shard_id) WHERE broker_id IS NULL.
-- created_at is intentionally NOT in the key — WorkQueueTakeBatch does
-- not ORDER BY it (FIFO isn't a correctness requirement), so keeping
-- created_at out of the index lets a claim be a plain Append of per-
-- shard index scans that each stop at LIMIT, with no Sort/MergeAppend.
-- Also keeps the index narrower → less churn on this UNLOGGED, high-
-- insert/update/delete table.
CREATE INDEX idx_work_queue_pending ON work_queue (kind, task_type, shard_id)
    WHERE broker_id IS NULL;
CREATE INDEX idx_work_queue_heartbeat ON work_queue (heartbeat_at)
    WHERE broker_id IS NOT NULL;
-- Leased-rows-by-kind index for the per-range in-flight recount
-- (recount_inflight): (shard_id, kind) WHERE broker_id IS NOT NULL. The
-- recount is `count(*) ... WHERE broker_id IS NOT NULL AND shard_id
-- BETWEEN lo AND hi GROUP BY kind`; this index makes it an index scan of
-- just the reaper's partition(s), grouped by kind. shard_id leads so the
-- range predicate prunes the Append. Distinct from idx_work_queue_heartbeat
-- (keyed on heartbeat_at for the reaper's stale-row scan).
CREATE INDEX idx_work_queue_leased ON work_queue (shard_id, kind)
    WHERE broker_id IS NOT NULL;

-- ── Per-kind concurrency cap (global parallelism limit across ALL pods) ──
--
-- Caps how many tasks of a kind are in flight (claimed, broker_id NOT NULL)
-- at once, SUMMED across every worker pod — e.g. vpc ≤ 100 so no more than
-- 100 vpc jobs run in parallel cluster-wide (to respect a cloud API limit).
-- The cap is GLOBAL: a single pool of N tokens that ANY worker on ANY shard
-- draws from, so a shard with a burst of work can use the whole pool and no
-- shard is starved. (The in-app WORKER_MAX_PARALLEL is only per-pod and
-- can't bound the cross-pod sum.) Enforced in the work_queue claim — the one
-- place every pod's work is serialized.
--
-- kind_config: the SINGLE per-kind operational-settings table — the ConfigMap
-- of Converge. ONE row per kind carrying everything an operator tunes
-- per kind: the global concurrency cap (max_inflight), the per-task timeout
-- (task_deadline_secs), and the drift-resync policy (resync_interval_secs,
-- resync_recomposes). LOGGED — config/identity, must survive a crash (losing it
-- would silently uncap every kind on recovery).
--
-- It is the SOURCE OF TRUTH, read directly — there is NO derived/projected copy:
--   * WHO WRITES: the DISPATCH pod seeds it insert-if-absent at boot from each
--     kind's DECLARED Capabilities (max_inflight + task_deadline_secs + resync
--     policy, all pure literals from the provider). It NEVER overwrites, so an
--     operator UPDATE is the runtime-editable truth and survives a restart.
--   * WHO READS:
--       - the work_queue CLAIM reads max_inflight by PK, inline in its budget
--         CTE (one index probe, the hot path) — no Go-side copy step;
--       - the WORKER dispatcher reads task_deadline_secs to bound each task's
--         execution (re-read on its tick + on a kind_config NOTIFY, so an edit
--         takes effect with no restart);
--       - the CONTROL plane reads resync_interval_secs/_recomposes to build the
--         Resyncer kind map (re-read on its tick + on a kind_config NOTIFY, so an
--         edit takes effect with no restart — the K8s informer relist/resync
--         pattern). The control plane dials NO client; it just SELECTs this.
--
-- CAP SEMANTICS — the load-bearing gate: a row with max_inflight > 0 is CAPPED;
-- a row with max_inflight = 0 (or no row at all) is UNCAPPED and the claim skips
-- ALL cap logic for it (zero overhead). The claim's budget/bump CTEs therefore
-- match `kind = $1 AND max_inflight > 0`, so seeding an uncapped kind's row
-- (max_inflight=0, present only for its resync policy) changes nothing on the
-- cap path. resync_interval_secs = 0 → the kind opts out of drift detection.
-- task_deadline_secs = 0 → the kind runs tasks with no deadline.
--
-- ORPHAN-GRACE columns (orphan_grace_secs, finalizer_name):
--   * orphan_grace_secs: how long a composer-dropped child of this kind lingers
--     in the Orphaned grace window before the reaper tears it down. 0 (default)
--     = no grace, prune immediately (today's behavior; opt-in per kind). Seeded
--     from Capabilities.OrphanGracePeriod, operator-editable at runtime via the
--     kinds API. The composer reads it (via the Go registry, not this table) to
--     bake frozen_until = now() + grace; the sweep only compares it to now().
--   * finalizer_name: the kind's declared finalizer string, seeded from
--     Capabilities.FinalizerName. Exists here because the reaper's in-DB
--     sweep_expired_orphans has NO Go registry (a control pod dials nothing), so
--     it reads the finalizer from this table to decide soft-delete (finalizer
--     present) vs hard-delete (NULL/empty) on grace expiry. NULL = leaf kind.
CREATE TABLE kind_config (
    kind                 TEXT NOT NULL,
    -- kind_version is the WEB-API-style user-facing version (vpc/v1, vpc/v2 — no
    -- minor/micro). Config is per-kind_version: a new kind_version can carry new caps /
    -- deadlines / finalizer independently of any other kind_version's resources. The
    -- claim probes kind_config by the concrete (kind, kind_version) the work_queue row
    -- carries — one PK probe, O(1) hot-path cost. Kind stays bare `vpc`;
    -- the kind_version is this orthogonal column, never in the kind string. REQUIRED
    -- and explicit (>= 1): NO column DEFAULT — seeded from the manifest's explicit
    -- kind_version, so a missing value is a hard error, not a silent v1.
    kind_version                INT  NOT NULL CHECK (kind_version >= 1),
    max_inflight         INT  NOT NULL DEFAULT 0 CHECK (max_inflight >= 0),
    task_deadline_secs   INT  NOT NULL DEFAULT 0 CHECK (task_deadline_secs >= 0),
    resync_interval_secs INT  NOT NULL DEFAULT 0 CHECK (resync_interval_secs >= 0),
    resync_recomposes    BOOLEAN NOT NULL DEFAULT FALSE,
    orphan_grace_secs    INT  NOT NULL DEFAULT 0 CHECK (orphan_grace_secs >= 0),
    finalizer_name       TEXT,
    -- max_transient_attempts is the poison-pill cap: after this many CONSECUTIVE
    -- transient reconcile failures (resources.failure_attempts) the drain escalates
    -- the failure to TERMINAL (dead-letter) so a persistently-broken handler stops
    -- retrying every RetryAfter forever and burning cluster capacity. Read per-row by
    -- drain_outbox_batch when applying a transient failure. Declared on the kind's
    -- manifest and seeded here (like max_inflight/resync). 0 = unbounded (never
    -- dead-letter on attempt count) — the default, so a kind opts INTO the cap by
    -- setting it. Operator-tunable via a live kind_config edit.
    max_transient_attempts INT NOT NULL DEFAULT 0 CHECK (max_transient_attempts >= 0),
    -- retired marks a web-API version SUNSET: NEW creates/flips ONTO this (kind,
    -- kind_version) are rejected at the API edge (ErrVersionRetired → 422), but
    -- EXISTING resources on it keep reconciling normally so they can drain or be
    -- migrated off (freeze-new / drain-existing — never a forced teardown). It is
    -- OFF the claim hot path: the claim never reads it (a retired version's live
    -- resources still schedule); only the low-rate apply/flip gate + the kinds API
    -- (migration-progress view) read it. Seeded from the manifest (a publish can
    -- declare a version retired) and operator-editable at runtime via
    -- /api/kinds/{kind}/config so a version can be retired WITHOUT a re-publish.
    -- Once a retired version's population reaches zero, its kind_config +
    -- kind_manifest rows can be dropped by the operator (GC). Default false.
    retired              BOOLEAN NOT NULL DEFAULT FALSE,
    -- manifest_version FENCES in-flight work against a mid-flight kind_manifest
    -- edit. It is the manifest's content hash (set by sync_kind_config_from_manifest
    -- below); schedule_eligible / cascade / requeue stamp it onto each enqueued
    -- work_queue row, and StampComposedGen / AppendOutbox check it so a task that
    -- was claimed under an old manifest cannot commit a compose / status against a
    -- changed reaction set (the commit no-ops -> benign requeue under the new
    -- manifest). 0 = no manifest applied yet. Read on the claim hot path as a
    -- ride-along column, NOT a separate probe.
    manifest_version     BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (kind, kind_version)
);

-- ── kind_manifest: the CRD definition ──────────────────────────────────────
-- The OPERATOR-applied, runtime-editable definition of a kind: its JSON Schemas
-- (spec/status/config), its declared REACTIONS, and its lifecycle finalizer.
-- This is the Terraform-style "what this kind IS" record; the converge CORE holds
-- NO compile-time provider registry — a kind exists when its manifest row exists,
-- and a worker exists when it connects over Connect. LOGGED config/identity (like
-- kind_config / providerconfigs), read OFF the claim hot path (API/UI, composer
-- finalizer lookup, reaper, the once-per-kind reaction cache) — NEVER per task.
--
-- reactions is []ReactionDecl, each describing one reaction as pure data:
--   {name, trigger, emits, verb?, finalizer?}
--   trigger ∈ specChange | childrenSettled | deleteRequested | operation | reactor | resync
--   emits   = a bitset over {children,edges,configs,status,conditions,finalizer,operationOutput,sideEffect}
-- The core dispatches purely off (trigger, emits): it never reads a kind NAME,
-- only the declared masks (compose = specChange+children; work = specChange+status;
-- rollup = childrenSettled+status; teardown = deleteRequested+finalizer;
-- operate = operation+operationOutput; react = reactor+sideEffect). A `reactor`
-- reaction carries no transition/kind/label — a reactor_bindings SUBSCRIPTION
-- supplies those; the CRD only registers the reactor.
--
-- manifest_version is the CONTENT HASH of (reactions || schemas), so an
-- idempotent re-apply that changes nothing does NOT invalidate in-flight work;
-- only a real change bumps it (computed app-side, passed to upsert_kind_manifest).
CREATE TABLE kind_manifest (
    kind             TEXT NOT NULL,
    -- kind_version is the web-API-style user-facing version (vpc/v1, vpc/v2). One
    -- manifest row PER (kind, kind_version): publishing a new kind_version INSERTs a new row
    -- and touches no existing-kind_version resource. The kind string stays bare `vpc`;
    -- the kind_version is this orthogonal column, never in the kind string or the
    -- (kind,name) address. (kind, kind_version) is bound to ONE content hash at publish
    -- (handlers_kind_manifest), so a given kind_version can never silently mean two
    -- different manifests across a multi-binary fleet. REQUIRED and explicit (>= 1):
    -- NO column DEFAULT — a manifest publish must carry its kind_version, never a
    -- silent v1 (the API rejects a missing/0 kind_version before this row is written).
    kind_version            INT NOT NULL CHECK (kind_version >= 1),
    description      TEXT,
    spec_schema      JSONB,                       -- JSON Schema; NULL = untyped opt-out
    status_schema    JSONB,
    config_schema    JSONB,
    reactions        JSONB   NOT NULL DEFAULT '[]',
    finalizer_name   TEXT,
    -- Operational policy the manifest DECLARES (the seed for kind_config). The
    -- sync trigger seeds these insert-if-absent into kind_config (so an operator's
    -- live /api/kinds/{kind}/config edit is never clobbered by a re-apply), and the
    -- ManifestCache serves orphan_grace_secs to the composer's prune policy.
    max_inflight         INT     NOT NULL DEFAULT 0,
    task_deadline_secs   INT     NOT NULL DEFAULT 0,
    resync_interval_secs INT     NOT NULL DEFAULT 0,
    resync_recomposes    BOOLEAN NOT NULL DEFAULT FALSE,
    orphan_grace_secs    INT     NOT NULL DEFAULT 0,
    max_transient_attempts INT   NOT NULL DEFAULT 0,   -- transient-failure dead-letter cap; 0 = unbounded
    retired          BOOLEAN NOT NULL DEFAULT FALSE, -- version SUNSET: a publish may declare it; mirrored into kind_config (freeze-new/drain-existing)
    schema_hash      TEXT,                         -- sha256 of the three schemas; gates incompatible schema edits
    manifest_version BIGINT  NOT NULL DEFAULT 1,   -- content hash of reactions+schemas (set by app on apply)
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, kind_version)
);

-- validate_kind_manifest: encodes the reaction legality rules as a CLOSED
-- lattice on each reaction's (trigger, emits), so a manifest that mis-declares
-- a reaction is rejected at apply time (before any task runs against bad data).
-- This is the DB half of the SDK-side validator; both share the same lattice.
-- Rejections that matter for correctness:
--   * a specChange reaction that emits children MUST also emit status|conditions
--     (a composer reports composite status) — mirrors "StatusRollup requires Composer";
--   * AT MOST ONE reaction may be (trigger=childrenSettled AND emits&status) — the
--     rollup is singular; two would race the advance gate;
--   * AT MOST ONE specChange reaction may emit children — one composer per kind;
--   * a children-emitting reaction may NOT also emit sideEffect (graph mutation and
--     external side effects don't mix in one fenced compose tx);
--   * deleteRequested requires a finalizer; operation requires a verb.
-- A `reactor` reaction is a pure side-effect capability (emits sideEffect): it
-- carries NO transition/kind/label — those live on the reactor_bindings row that
-- SUBSCRIBES the reactor to a watched kind's transition. So the reactor CRD only
-- REGISTERS the kind; the binding is the dynamic subscription.
-- Raises EXCEPTION on violation (the apply tx rolls back).
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION validate_kind_manifest(p_reactions JSONB) RETURNS VOID AS $$
DECLARE
    rx            JSONB;
    trig          TEXT;
    emits         JSONB;
    has_children  BOOLEAN;
    has_status    BOOLEAN;
    has_side      BOOLEAN;
    n_children    INT := 0;
    n_rollup      INT := 0;
    n_reactor     INT := 0;
BEGIN
    IF jsonb_typeof(p_reactions) <> 'array' THEN
        RAISE EXCEPTION 'kind_manifest.reactions must be a JSON array, got %', jsonb_typeof(p_reactions);
    END IF;
    FOR rx IN SELECT * FROM jsonb_array_elements(p_reactions) LOOP
        trig  := rx->>'trigger';
        emits := COALESCE(rx->'emits', '[]'::jsonb);
        IF trig IS NULL THEN
            RAISE EXCEPTION 'reaction % missing trigger', rx->>'name';
        END IF;
        IF trig NOT IN ('specChange','childrenSettled','deleteRequested','operation','reactor','resync') THEN
            RAISE EXCEPTION 'reaction % has unknown trigger %', rx->>'name', trig;
        END IF;
        has_children := emits ? 'children';
        has_status   := emits ? 'status' OR emits ? 'conditions';
        has_side     := emits ? 'sideEffect';
        IF trig = 'specChange' AND has_children THEN
            n_children := n_children + 1;
            IF NOT has_status THEN
                RAISE EXCEPTION 'reaction %: a specChange reaction emitting children must also emit status/conditions', rx->>'name';
            END IF;
            IF has_side THEN
                RAISE EXCEPTION 'reaction %: a children-emitting reaction may not also emit sideEffect', rx->>'name';
            END IF;
        END IF;
        IF trig = 'childrenSettled' AND has_status THEN
            n_rollup := n_rollup + 1;
        END IF;
        IF trig = 'deleteRequested' AND COALESCE(rx->>'finalizer', '') = '' THEN
            RAISE EXCEPTION 'reaction %: deleteRequested requires a finalizer', rx->>'name';
        END IF;
        IF trig = 'operation' AND COALESCE(rx->>'verb', '') = '' THEN
            RAISE EXCEPTION 'reaction %: operation requires a verb', rx->>'name';
        END IF;
        -- A `reactor` reaction must emit sideEffect and NOTHING that mutates the
        -- graph/status of the watched resource (it acts on OTHER kinds via a
        -- binding, not on itself). It carries no transition — the binding does.
        IF trig = 'reactor' THEN
            n_reactor := n_reactor + 1;
            IF NOT has_side THEN
                RAISE EXCEPTION 'reaction %: a reactor reaction must emit sideEffect', rx->>'name';
            END IF;
            IF has_children OR has_status THEN
                RAISE EXCEPTION 'reaction %: a reactor reaction may only emit sideEffect', rx->>'name';
            END IF;
        END IF;
    END LOOP;
    -- Exactly one reactor reaction per kind: the claim resolves the reaction to
    -- run from the reactor's CRD, so two would be ambiguous. Several reactors on
    -- one event = several reactor KINDS + several bindings, not several reactions.
    IF n_reactor > 1 THEN
        RAISE EXCEPTION 'at most one reactor reaction per kind (got %)', n_reactor;
    END IF;
    IF n_children > 1 THEN
        RAISE EXCEPTION 'at most one specChange reaction may emit children (got %)', n_children;
    END IF;
    IF n_rollup > 1 THEN
        RAISE EXCEPTION 'at most one childrenSettled reaction may emit status (got %)', n_rollup;
    END IF;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- sync_kind_config_from_manifest: on a kind_manifest write, (1) validate the
-- reactions lattice, (2) SEED the policy row in kind_config insert-if-absent
-- (never overwrite an operator's live /api/kinds/{kind}/config edits), and (3)
-- ALWAYS mirror the manifest_version + finalizer_name into kind_config so the
-- claim hot path can stamp the current version with no extra read. finalizer_name is
-- authoritative from the manifest (the reaper reads it from kind_config).
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION sync_kind_config_from_manifest() RETURNS trigger AS $$
BEGIN
    PERFORM validate_kind_manifest(NEW.reactions);
    -- INSERT seeds the full policy from the manifest; ON CONFLICT only re-syncs
    -- the NON-operator-editable axes (manifest_version + finalizer_name, which are
    -- provider-declared) so an operator's live cap/deadline/resync/grace edit via
    -- /api/kinds/{kind}/config is preserved across a manifest re-apply.
    -- `retired` is seeded from the manifest here but treated as OPERATOR-editable
    -- (like the caps): a publish declaring retired=true sunsets the version on
    -- first apply, but a re-apply never un-retires a version an operator retired
    -- at runtime (DO UPDATE omits `retired` — it is preserved), the same
    -- seed-don't-overwrite rule the caps follow.
    INSERT INTO kind_config (kind, kind_version, finalizer_name, manifest_version,
                             max_inflight, task_deadline_secs, resync_interval_secs,
                             resync_recomposes, orphan_grace_secs, max_transient_attempts, retired)
    VALUES (NEW.kind, NEW.kind_version, NEW.finalizer_name, NEW.manifest_version,
            NEW.max_inflight, NEW.task_deadline_secs, NEW.resync_interval_secs,
            NEW.resync_recomposes, NEW.orphan_grace_secs, NEW.max_transient_attempts, NEW.retired)
    ON CONFLICT (kind, kind_version) DO UPDATE
       SET manifest_version = EXCLUDED.manifest_version,
           finalizer_name   = EXCLUDED.finalizer_name;
    -- Denormalize the version onto every resource of this (kind, kind_version) so
    -- schedule/cascade reads it off the row (no kind_config join on the hot path).
    -- WRITE-COLD: only when the version actually changes (a real CRD apply, not an
    -- idempotent re-seed — the content-hash version is stable), so an unchanged
    -- re-apply touches zero resource rows. SCOPED TO THE KIND VERSION: publishing vpc/v3
    -- must touch NO vpc/v2 resource, so the restamp filters kind_version = NEW.kind_version. The
    -- kind-leading index lets the fan-out scan by kind rather than a 1M seq scan,
    -- and it is skipped entirely on the first INSERT of a (kind, kind_version) (no
    -- resources of that kind_version exist yet).
    IF TG_OP = 'UPDATE' AND NEW.manifest_version IS DISTINCT FROM OLD.manifest_version THEN
        UPDATE resources SET manifest_version = NEW.manifest_version
         WHERE kind = NEW.kind AND kind_version = NEW.kind_version
           AND manifest_version IS DISTINCT FROM NEW.manifest_version;
    END IF;
    -- Wake the ManifestCache so an operator's apply takes effect promptly (the
    -- cache also has a 60s failsafe re-read, so a dropped NOTIFY still converges).
    -- Payload = "kind:kind_version", so a listener refreshes just that one (kind, kind_version).
    PERFORM pg_notify('kind_manifest_changed', NEW.kind || ':' || NEW.kind_version::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER trg_kind_manifest_sync_config
    AFTER INSERT OR UPDATE ON kind_manifest
    FOR EACH ROW EXECUTE FUNCTION sync_kind_config_from_manifest();

-- Deleting a (kind, kind_version) manifest (the operator retiring a version that
-- no resource/binding/config references) must wake the ManifestCache the same way
-- an apply does, so every pod drops the gone (kind, kind_version) from its live
-- routing/validation set promptly (the 60s failsafe re-read is the backstop).
-- Payload = "kind:kind_version" (same shape as the apply NOTIFY) so a listener
-- refreshes just that one pair. The blocking checks + the paired kind_config /
-- providerconfigs deletes are enforced app-side (store.DeleteKindManifest) in one
-- transaction; this trigger only broadcasts the change.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION notify_kind_manifest_deleted() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('kind_manifest_changed', OLD.kind || ':' || OLD.kind_version::text);
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER trg_kind_manifest_notify_delete
    AFTER DELETE ON kind_manifest
    FOR EACH ROW EXECUTE FUNCTION notify_kind_manifest_deleted();

-- kind_inflight: the live in-flight tally, stored as PARTIALS so the
-- recount stays cheap and range-local, but READ as ONE GLOBAL POOL.
--
--   * GLOBAL POOL (no starvation): the claim's budget is
--     `max_inflight - SUM(in_flight over ALL of a kind's rows)`. Every
--     worker, whatever shard it owns, subtracts the SAME global sum, so the
--     N tokens are one shared pool — not sliced per shard. A single shard
--     can take all N.
--   * PARTIAL ROWS (cheap recount): one row per (kind, range_lo), where
--     range_lo is the low shard of whoever wrote it. The claim bumps its
--     own pod's partial as an optimistic hint; recount_inflight (on the
--     reaper tick) OWNS its shard range — it deletes every partial whose
--     range_lo falls in the reaper's [lo,hi] and writes ONE authoritative
--     row with the true count for that range. Because the 3 reaper ranges
--     tile [0,255] disjointly, every partial is owned by exactly one
--     reaper, and SUM over the (few) surviving rows is the exact global
--     count. This is robust even though workers (≈50 ranges) and reapers
--     (≈3 ranges) slice the shards differently — the recount reclaims the
--     finer worker hints within its range.
--   * UNLOGGED, like work_queue: a crash TRUNCATEs work_queue (every lease
--     vanishes), so the tally MUST reset to 0 with it. A LOGGED tally would
--     survive a crash inflated against an empty queue and wedge the cap.
CREATE UNLOGGED TABLE kind_inflight (
    kind        TEXT     NOT NULL,
    -- kind_version: the concurrency cap is per-(kind, kind_version) — vpc/v1 and vpc/v2 carry
    -- independent inflight budgets (max_inflight lives on the per-kind_version
    -- kind_config row). The tally is partitioned by (kind, kind_version, range_lo) so
    -- the claim's budget CTE probes the concrete (kind, kind_version) it is claiming.
    -- NO column DEFAULT — every kind_inflight write names the explicit (kind,
    -- kind_version) it tallies (sourced from the work_queue row's version).
    kind_version       INT      NOT NULL,
    range_lo    SMALLINT NOT NULL,
    in_flight   INT      NOT NULL DEFAULT 0,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, kind_version, range_lo)
);

-- rollup_recheck: the deferred-recheck queue that closes the rollup STRADDLE
-- race. When a composite root's final descendants settle across two
-- concurrently-running drain batches, NEITHER batch's MVCC snapshot sees "0
-- descendants lagging" (each is blind to the other's not-yet-visible commit),
-- so cascade_on_ready_change's schedule_eligible call enqueues the root in
-- NEITHER batch — the root strands until the reaper's slow poll (the 3.5s↔12s
-- variance). Instead of waiting for the reaper, the cascade ENQUEUES the
-- straddled root here (committed atomically with the batch), and the drainer
-- drains it via drain_rollup_rechecks() in a FRESH transaction whose snapshot —
-- started after the straddling batches committed — DOES see 0 lagging, so
-- schedule_eligible's descendant gate clears and the root pends within one
-- drainer tick (ms), not the reaper's 5–10s + RetryAfter age gate.
--
-- shard_id keys the row to a drainer band's contiguous range (BETWEEN pruning,
-- like work_queue). UNLOGGED: a crash loses pending rechecks, but the reaper
-- remains the durable crash-recovery backstop — this table only accelerates the
-- common in-flight straddle, it is never the sole guarantee. On the 1M healthy
-- run nothing strands → this table stays empty → zero cost.
CREATE UNLOGGED TABLE rollup_recheck (
    resource_id UUID     NOT NULL PRIMARY KEY,
    shard_id    SMALLINT NOT NULL
);
CREATE INDEX idx_rollup_recheck_shard ON rollup_recheck (shard_id);

-- schedule_recheck: the deferred-schedule queue for a wide compose's children.
-- A wide compose commits ~10^5–10^6 children that each need their first
-- work_queue row. Scheduling them all in one post-commit statement would be a
-- multi-SECOND writer overlapping the whole drain fleet's window, colliding with
-- DrainOutboxBatch on the same resources rows (via the cascade → apply_value_flows
-- path) into a 40P01 deadlock whose dropped wake would strand the children on the
-- reaper's 500/tick backstop. Instead the compose ARMS its candidates here,
-- committed ATOMICALLY with the children (so the wake can never be lost), and the
-- drainer drains it via drain_schedule_rechecks() in SHARD BANDS, paced with its
-- own drains — small schedule_eligible batches that don't form the deadlock
-- convoy. Mirrors rollup_recheck exactly. UNLOGGED: a crash loses pending rows but
-- the reaper (requeue_failed_and_pending) is the durable backstop — this only
-- accelerates the common case. On the healthy run it drains within a few ticks
-- then stays empty → zero steady-state cost.
CREATE UNLOGGED TABLE schedule_recheck (
    resource_id UUID     NOT NULL PRIMARY KEY,
    shard_id    SMALLINT NOT NULL
);
CREATE INDEX idx_schedule_recheck_shard ON schedule_recheck (shard_id);

-- wake_state: GLOBAL rate-limiter for the PER-ROW latency-wake NOTIFY
-- ('outbox_ready', fired from AppendOutbox on every one of ~1M appends). One
-- row per channel, holding the last time a NOTIFY fired on it.
--
-- ONLY 'outbox_ready' is gated. 'work_ready' is fired DIRECTLY (un-gated
-- pg_notify) from schedule_eligible / cascade_on_ready_change: it is a
-- per-STATEMENT/per-batch wake (≤ low-hundreds/sec fleet-wide), nowhere near
-- the async-queue-lock storm threshold, and gating it LOST wakes — notify_gated
-- holds a pg_try_advisory_XACT_lock until COMMIT, and work_ready fires inside
-- the drainer's seconds-long drain tx, so a concurrent enqueuer lost the
-- advisory race and stranded a freshly-pended root. See schedule_eligible.
--
-- WHY a DB-side gate (and how this one avoids the traps that bit two earlier
-- designs):
--   * pg_notify is NOT lock-free — it takes an AccessExclusiveLock on the
--     single shared async-notification queue. So firing it from many backends
--     concurrently (e.g. 1M job completions each calling pg_notify) serializes
--     ~all of them on that one lock — a measured collapse (1000+ backends
--     stuck in pg_notify). The fix MUST cap how many backends per second
--     actually reach pg_notify, GLOBALLY — an in-process per-pod gate doesn't,
--     because 50 pods × in-flight notifies still storm the queue lock.
--   * The earlier wake_state design made EVERY caller do `UPDATE … WHERE
--     last_notify_at < now()-window` — which takes the row lock to evaluate,
--     so concurrent callers QUEUED on it (852k ms of contention). This design
--     is READ-MOSTLY: the gate does a cheap unlocked SELECT first and 99.99%
--     of callers return without touching any lock; only a caller near a window
--     boundary attempts a NON-BLOCKING advisory try-lock, and at most one
--     winner per window does the UPDATE + pg_notify. No caller ever WAITS.
--
-- UNLOGGED: a crash TRUNCATEs it; notify_gated re-seeds (ON CONFLICT DO
-- NOTHING) so the first post-crash call re-creates the row at '-infinity' and
-- fires immediately.
CREATE UNLOGGED TABLE wake_state (
    channel        TEXT        NOT NULL PRIMARY KEY,
    last_notify_at TIMESTAMPTZ NOT NULL DEFAULT '-infinity'
);
-- 'cluster_changed' is the third channel: fired (gated) by the cluster_members
-- INSERT/DELETE trigger when a member JOINS or LEAVES, so every node's
-- Resharder wakes and recomputes its dynamic shard assignment from the new live
-- set. Membership churn is rare vs the work/outbox hot paths, but gating it the
-- same way keeps the async-queue lock safe during a mass rollout (50 pods all
-- joining at once → ~1 NOTIFY per 50ms, not 50).
-- 'providerconfig_changed' is fired (gated) whenever a resource of
-- kind='providerconfig' is inserted/updated/deleted, so every worker node's
-- reconfigure listener re-reads the affected default/bootstrap config and
-- live-pushes it to the providers that captured it at Setup — no restart.
-- Config edits are rare vs the work/outbox hot paths; gating keeps a bulk
-- config rollout from storming the async-notify queue lock.
INSERT INTO wake_state (channel) VALUES ('work_ready'), ('outbox_ready'), ('cluster_changed'), ('providerconfig_changed');

-- notify_gated(p_channel): fire a coalesced NOTIFY on p_channel, GLOBALLY
-- rate-limited to ~1 per 50ms window across ALL backends, without any caller
-- ever blocking on a lock. Three layers, cheapest first:
--   1. Unlocked SELECT of last_notify_at — an MVCC read, no row lock. If the
--      window hasn't elapsed (the overwhelming common case under a flood),
--      RETURN immediately. This is all 999,999-of-1M callers ever do.
--   2. pg_try_advisory_xact_lock — NON-BLOCKING. Near a window boundary
--      several callers pass step 1; only ONE acquires the advisory lock, the
--      rest get false and RETURN at once (no queue, unlike a row lock). The
--      lock auto-releases at txn end.
--   3. The winner re-checks the window under the lock, stamps last_notify_at,
--      and emits the single pg_notify. So at most ~1 backend per window per
--      channel ever reaches pg_notify → the async-queue lock is uncontended.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION notify_gated(p_channel TEXT) RETURNS void AS $$
DECLARE
    last_at TIMESTAMPTZ;
    cutoff  TIMESTAMPTZ := clock_timestamp() - interval '50 milliseconds';
BEGIN
    -- Window check is `last_at > cutoff` (a direct timestamp compare), NOT
    -- `now() - last_at < interval` — the latter subtracts the seed '-infinity'
    -- and errors (22008 cannot subtract infinite timestamps). '-infinity' is
    -- simply < any finite cutoff, so the first call always passes. clock_
    -- timestamp() (wall clock at statement time, not txn start) so a long
    -- cascade tx still gates correctly.

    -- Layer 1: cheap unlocked read. Re-seed if the UNLOGGED row vanished
    -- (post-crash); ON CONFLICT DO NOTHING is a no-op PK probe in steady state.
    SELECT last_notify_at INTO last_at FROM wake_state WHERE channel = p_channel;
    IF NOT FOUND THEN
        INSERT INTO wake_state (channel) VALUES (p_channel) ON CONFLICT (channel) DO NOTHING;
        last_at := '-infinity';
    END IF;
    IF last_at > cutoff THEN
        RETURN;  -- within window: 99.99% of callers stop here, lock-free
    END IF;

    -- Layer 2: non-blocking — only one winner per window proceeds.
    IF NOT pg_try_advisory_xact_lock(hashtext('wake:' || p_channel)) THEN
        RETURN;
    END IF;

    -- Layer 3: winner re-checks under the advisory lock, stamps, fires once.
    UPDATE wake_state
       SET last_notify_at = clock_timestamp()
     WHERE channel = p_channel
       AND last_notify_at <= cutoff;
    IF FOUND THEN
        PERFORM pg_notify(p_channel, '');
    END IF;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- work_outbox: append-only buffer for task results. Drainer pops up
-- to N rows per tick and applies the deltas in one transaction.
--
-- For reconcile rows, advance_synced_gen tells the drainer whether
-- the worker considers the kind's pipeline fully reconciled to the
-- observed_generation. False when (e.g.) a composer-having kind
-- emitted children but rollup couldn't run because descendants
-- aren't ready yet.
--
-- health_ok / conditions carry the Ready/health axis the worker
-- observed this run:
--   * health_ok   — NULL means "the worker said nothing about health"
--                   (the common case: a plain reconcile that only
--                   advances synced_gen). The drainer leaves
--                   resources.health_ok untouched when this is NULL, so
--                   a kind that never reports health stays healthy=true
--                   for free. Non-NULL true/false sets the scalar.
--   * conditions  — NULL or '[]' means "no condition rows to write"
--                   (the 1M-stress case → the drainer's conditions pass
--                   short-circuits). Otherwise a JSONB array of
--                   {type,status,reason,message} the drainer upserts
--                   into resource_conditions on transition.
-- PK is work_id (a random UUID = work_queue.id), NOT a monotonic
-- BIGSERIAL. A serial PK funnels every insert from all workers onto the
-- single right-most b-tree leaf page of the PK index — the dominant
-- LWLock/BufferContent + Lock/extend hotspot under high writer counts
-- (measured: identical AppendOutbox/DrainOutboxBatch ran ~2× slower at
-- 200 writers vs 20). work_id is random, so inserts scatter uniformly
-- across leaf pages: no shared tail page, no serialization on insert.
-- One outbox row is emitted per claimed work item, so work_id is unique.
-- The drainer pops/deletes by (shard_id, work_id) via idx_work_outbox_drain.
--
-- RANGE PARTITIONED BY shard_id into 16 partitions of 16 contiguous
-- shards, same design as work_queue: the drainer pops with
-- `shard_id BETWEEN shard_lo AND shard_hi`, so RANGE-on-shard_id prunes
-- each drain to the single partition holding those shards — one heap +
-- one index, no lock-manager fast-path spill. shard_id is a PLAIN column
-- (partition keys can't be generated), set via shard_of(resource_id) at the one
-- INSERT site (AppendOutbox). PK=(work_id, shard_id) to include the
-- partition key; work_id alone is still effectively unique.
CREATE UNLOGGED TABLE work_outbox (
    work_id         UUID NOT NULL,
    resource_id     UUID NOT NULL,
    op_id           UUID,
    task_type       task_type NOT NULL,
    succeeded       BOOLEAN NOT NULL,
    -- New status (compose/work/rollup) or verb output (operate).
    payload         JSONB,
    error_message   TEXT,
    observed_generation BIGINT NOT NULL DEFAULT 0,
    advance_synced_gen   BOOLEAN NOT NULL DEFAULT true,
    health_ok       BOOLEAN,
    conditions      JSONB,
    -- failed: the worker reports this reconcile HARD-FAILED. The drainer
    -- stamps resources.failure_gen = observed_generation for it. Distinct
    -- from `succeeded` because a failed reconcile must still drive a write
    -- (to record the failure) where before only successes did. Defaults
    -- false; the 1M-stress workers never set it, so the drainer's new
    -- failure branch is constant-false on the hot path.
    failed          BOOLEAN NOT NULL DEFAULT false,
    -- terminal: the failure is non-retryable (framework.Terminal). The
    -- drainer sets resources.failure_terminal from it. Only meaningful
    -- when failed=true.
    terminal        BOOLEAN NOT NULL DEFAULT false,
    finalizer_name  TEXT,
    -- manifest_version carried from the claimed work_queue row, so a drainer /
    -- audit can see which manifest the result was produced under. Inert on the
    -- hot path (default 0). The AppendOutbox fence checks the work_queue row's
    -- version directly; this column is the audit copy on the outbox side.
    manifest_version BIGINT NOT NULL DEFAULT 0,
    shard_id        SMALLINT NOT NULL,
    PRIMARY KEY (work_id, shard_id)
) PARTITION BY RANGE (shard_id);

-- Per-partition storage params (see work_queue rationale above).
-- +goose StatementBegin
DO $$
DECLARE i INT;
BEGIN
    FOR i IN 0..15 LOOP
        EXECUTE format(
            'CREATE UNLOGGED TABLE work_outbox_p%s PARTITION OF work_outbox '
            || 'FOR VALUES FROM (%s) TO (%s) '
            || 'WITH (autovacuum_vacuum_scale_factor = 0.05, '
            || 'autovacuum_analyze_scale_factor = 0.02, '
            || 'autovacuum_vacuum_threshold = 50, '
            || 'autovacuum_analyze_threshold = 50, '
            || 'autovacuum_vacuum_cost_delay = 10, '
            || 'autovacuum_vacuum_cost_limit = 1000)',
            i, i*16, (i+1)*16);
    END LOOP;
END $$;
-- +goose StatementEnd

-- Drain access path: pop rows by shard. work_id (random UUID) as the
-- second column keeps the index inserts scattered too — no monotonic
-- tail. FIFO within a shard isn't a correctness requirement (same as
-- work_queue), so we don't need a time-ordered key here. Cascades to
-- every partition automatically.
CREATE INDEX idx_work_outbox_drain ON work_outbox (shard_id, work_id);

-- resource_operations: durable record of subresource verb invocations.
CREATE TABLE resource_operations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    resource_id     UUID NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
    verb            TEXT NOT NULL,
    input           JSONB NOT NULL DEFAULT '{}',
    output          JSONB,
    state           TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'running', 'succeeded', 'failed')),
    error_message   TEXT,
    attempts        INT NOT NULL DEFAULT 0,
    requested_by    TEXT,
    requested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ
);

CREATE INDEX idx_resource_operations_by_resource
    ON resource_operations (resource_id, requested_at DESC);

-- ─────────────────────────────────────────────────────────────────────────
-- LIFECYCLE REACTOR SPINE
--
-- A generic reactive event spine over resource lifecycle TRANSITIONS. A
-- "saga" (e.g. submit a composer root → wait until rolled up → run a reactor)
-- is NOT a first-class type: it is the emergent chain of bindings, where each
-- reactor's action (an external call, or a store.ApplySpec that produces the
-- next resource) fires the next transition. The single load-bearing wait
-- primitive is "a resource reached synced_gen >= generation", surfaced as the
-- 'synced' transition — exactly the edge cascade_on_ready_change already
-- computes for reactive rollup, reused one level up: a worker slot is NEVER
-- held during a wait, and there is no poll — the wait is the ABSENCE of a
-- lifecycle_outbox{synced} row until the synced_gen edge fires.
-- ─────────────────────────────────────────────────────────────────────────

-- reactor_bindings: the runtime-editable SUBSCRIPTION surface — the SOLE place
-- the "when X transitions, run reactor R" wiring lives. One row subscribes a
-- reactor kind to a watched kind's lifecycle transition (optionally
-- label-scoped): (watch_kind, transition[, label predicate]) → reactor. Adding a
-- subscription is an INSERT; there is no manifest projection and no "derived vs
-- manual" split — every binding is an explicit, editable subscription an
-- operator applies. LOGGED config/identity (like kind_config / providerconfigs).
-- The dispatcher reads it FRESH on every claim (no Go-side cache), so
-- enable/disable/retarget is live.
--
-- The reactor CRD (kind_manifest with a `reactor`-trigger reaction) only
-- REGISTERS the reactor kind + its config schema — it declares NO transition,
-- kind, or label. Those are supplied HERE, by the binding.
--
--   watch_kind  the resource kind whose transition fires the subscription.
--   reactor     the reactor KIND that handles it. An ordinary worker kind: the
--               broker drains lifecycle_outbox and ships the reaction
--               (STAGE_REACT) to a connected worker advertising this kind,
--               exactly like every other stage. It differs from watch_kind (a
--               reactor owns no resource). Its WHERE-to-deliver config (bucket,
--               endpoint, prefix) comes from the reactor kind's DEFAULT
--               providerconfig, pulled by the worker — not from the binding.
CREATE TABLE reactor_bindings (
    name            TEXT PRIMARY KEY,
    watch_kind      TEXT NOT NULL,                       -- resource kind to watch
    -- watch_kind_version optionally scopes the subscription to ONE web-API version
    -- of the watched kind. NULL (the default) = ALL versions — the binding fires on
    -- the transition of any vpc resource regardless of its pinned kind_version. A
    -- concrete value fires ONLY for resources on that version (e.g. watch vpc/v2
    -- 'synced' but not vpc/v1). Applied at EMIT (the created-emit query + the
    -- cascade fan-out both AND this against the transitioning resource's
    -- kind_version), so a row is written only for a matching binding — no
    -- claim-then-drop. Distinct from reactor_version (which versions the REACTOR
    -- being run); this versions the WATCHED kind.
    watch_kind_version SMALLINT CHECK (watch_kind_version IS NULL OR watch_kind_version >= 1),
    transition      TEXT NOT NULL                        -- which lifecycle edge
        CHECK (transition IN ('created', 'synced', 'degraded', 'failed', 'deleted')),
    label_match     JSONB NOT NULL DEFAULT '{}',         -- {} = match all; else resource_meta.labels @> this
    reactor         TEXT NOT NULL,                       -- the reactor kind handle the worker runs
    -- reactor_version optionally PINS which published kind_version of the reactor
    -- this binding invokes. NULL (the default) = resolve the reactor's HIGHEST
    -- published kind_version at claim time, so a reactor upgrade takes effect for every
    -- unpinned binding automatically. A concrete value pins the binding to that
    -- exact reactor kind_version, so a new reactor publish does NOT silently change what
    -- an existing subscription runs — the predictability knob for a versioned
    -- reactor. claim_reactor_deliveries resolves the reaction name against
    -- (reactor, COALESCE(reactor_version, highest)); a pin to a version with no
    -- published manifest yields NULL reaction (left for the reaper, like an
    -- unapplied reactor CRD).
    reactor_version SMALLINT CHECK (reactor_version IS NULL OR reactor_version >= 1),
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Match probe used by the cascade trigger's EXISTS short-circuit AND by the
-- claim's binding JOIN: (watch_kind, transition) of enabled bindings.
CREATE INDEX idx_reactor_bindings_match
    ON reactor_bindings (watch_kind, transition) WHERE enabled;

-- lifecycle_outbox: one row per real resource lifecycle transition, written
-- transactionally in the SAME statement that flips the resource state (for
-- 'synced' that is cascade_on_ready_change's already-computed `upgraded`
-- array). Drained by the ReactorDispatcher duty (a structural clone of the
-- Drainer): claim FOR UPDATE SKIP LOCKED in the pod's shard range → deliver
-- → ack (delete).
--
-- A delivery's durable unit is PER-BINDING, not per-transition: the FAN-OUT
-- happens at EMIT (the cascade INSERT joins reactor_bindings and writes one row
-- per matching enabled binding, stamping binding_name), NOT at claim. This is
-- what makes fan-out durable+independent: two bindings on the same
-- (kind,transition) get two rows, so one's success/ack/retry never touches the
-- other's. label_match is applied at emit too, so a row exists ONLY for a
-- binding the resource actually matches (no claim-then-drop orphan).
--
-- LOGGED (NOT UNLOGGED like work_outbox): the at-least-once delivery contract
-- and the un-re-derivable 'deleted' transition require a committed row to
-- survive a crash. Cheap here — rows are written ONLY on real transitions WITH
-- a matching binding (a 1M-healthy steady state writes none), nowhere near
-- work_outbox's per-row 1M hot path. RANGE-partitioned by shard_id exactly like
-- work_outbox so a resource's transition is claimed by the same pod-range that
-- drains it (locality, no cross-pod handoff) with shard_id BETWEEN lo AND hi.
--
-- PK (resource_id, transition, generation, binding_name, shard_id) is the DEDUP
-- key: the straddle-re-pend / demote→resync re-cross / multi-gen duplicate-fire
-- all produce the same tuple PER binding, so a re-emit is a no-op (ON CONFLICT
-- DO NOTHING) — duplicate-fire eliminated at the source, independently per
-- binding. broker_id/heartbeat_at are the claim lease (NULL = claimable);
-- reap_stale_lifecycle frees a dead claim, mirroring reap_stale_work.
CREATE TABLE lifecycle_outbox (
    resource_id   UUID NOT NULL,
    kind          TEXT NOT NULL,
    transition    TEXT NOT NULL
        CHECK (transition IN ('created', 'synced', 'degraded', 'failed', 'deleted')),
    generation    BIGINT NOT NULL,    -- the generation that crossed (idempotency keying)
    binding_name  TEXT NOT NULL,      -- the reactor_bindings row this delivery is FOR (per-binding identity)
    broker_id     TEXT,               -- claim lease holder; NULL = claimable
    heartbeat_at  TIMESTAMPTZ,        -- claim freshness; stale → reap_stale_lifecycle frees it
    -- claim_epoch mirrors work_queue.claim_epoch for the reactor spine: the strict
    -- monotonic fencing token ack_reactor_delivery checks so only the current
    -- epoch's holder may ack a delivery. A reconnected zombie carrying an older
    -- epoch matches nothing, so its stale ack can never delete a live re-delivery.
    -- Bumped by one on every reclaimable transition (claim, reap). Starts at 1.
    claim_epoch   BIGINT NOT NULL DEFAULT 1,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    shard_id      SMALLINT NOT NULL,  -- shard_of(resource_id); partition key (plain col)
    PRIMARY KEY (resource_id, transition, generation, binding_name, shard_id)
) PARTITION BY RANGE (shard_id);

-- 16 partitions of 16 contiguous shards each — same loop as work_outbox, but
-- LOGGED. Per-partition autovacuum tuned like the other hot partitioned tables.
-- +goose StatementBegin
DO $$
DECLARE i INT;
BEGIN
    FOR i IN 0..15 LOOP
        EXECUTE format(
            'CREATE TABLE lifecycle_outbox_p%s PARTITION OF lifecycle_outbox '
            || 'FOR VALUES FROM (%s) TO (%s) '
            || 'WITH (autovacuum_vacuum_scale_factor = 0.05, '
            || 'autovacuum_analyze_scale_factor = 0.02, '
            || 'autovacuum_vacuum_threshold = 50, '
            || 'autovacuum_analyze_threshold = 50, '
            || 'autovacuum_vacuum_cost_delay = 10, '
            || 'autovacuum_vacuum_cost_limit = 1000)',
            i, i*16, (i+1)*16);
    END LOOP;
END $$;
-- +goose StatementEnd

-- Claim access path: unclaimed rows by shard. Partial WHERE broker_id IS NULL
-- mirrors idx_work_queue_pending — the claim scans only the claimable tail.
CREATE INDEX idx_lifecycle_outbox_claim
    ON lifecycle_outbox (shard_id) WHERE broker_id IS NULL;
-- Stale-claim reaper path: claimed rows by heartbeat, mirrors idx_work_queue_heartbeat.
CREATE INDEX idx_lifecycle_outbox_heartbeat
    ON lifecycle_outbox (heartbeat_at) WHERE broker_id IS NOT NULL;

-- claim_reactor_deliveries: claim up to max_rows due PER-BINDING deliveries in
-- the pod's CONTIGUOUS shard range, stamp broker_id (so other pods skip them),
-- and RETURN each with the resource's live name/status + the resolved reactor
-- kind — everything the dispatcher needs to ship STAGE_REACT, in ONE round-trip.
-- The reactor's WHERE-to-deliver config is its kind's DEFAULT providerconfig,
-- pulled by the worker (GetProviderConfig) like any kind — not carried here.
--
-- Each lifecycle_outbox row IS already one delivery (the fan-out + label-match
-- happened at EMIT, so the row carries binding_name). So there is NO fan-out
-- JOIN here and NO label re-probe — we join reactor_bindings by name to resolve
-- the reactor kind (gated on `enabled` so a binding disabled between emit and
-- claim is skipped; its row ages out via reap → GC), then join kind_manifest to
-- resolve that reactor kind's single `reactor`-trigger reaction NAME. The
-- binding name is operator-chosen and opaque — it does NOT encode the reaction
-- name (that lives in the reactor CRD), so the DB resolves the reaction here and
-- the dispatcher ships it verbatim, never parsing the binding name.
--
-- Modeled on WorkQueueTakeBatch: FOR UPDATE SKIP LOCKED, shard_id BETWEEN
-- lo AND hi (range → partition pruning; NEVER = ANY(array)). status (large
-- JSONB) is read ONCE per resource: the resources join is collapsed in a
-- per-resource CTE so N bindings on one resource don't copy its status N times.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION claim_reactor_deliveries(
    p_broker_id TEXT, max_rows INTEGER, shard_lo SMALLINT, shard_hi SMALLINT)
RETURNS TABLE(
    resource_id UUID, kind TEXT, name TEXT, transition TEXT,
    generation BIGINT, status JSONB, binding_name TEXT, reactor TEXT,
    reaction TEXT, reactor_kind_version INT, claim_epoch BIGINT)
AS $$
    WITH picked AS (
        SELECT lo.resource_id, lo.transition, lo.generation, lo.binding_name, lo.shard_id
          FROM lifecycle_outbox lo
         WHERE lo.broker_id IS NULL
           AND lo.shard_id BETWEEN shard_lo AND shard_hi  -- range → partition pruning
         LIMIT max_rows
         FOR UPDATE SKIP LOCKED
    ), claimed AS (
        UPDATE lifecycle_outbox lo
           -- Bump the fencing token on claim and RETURN it so the dispatcher ships
           -- it in the STAGE_REACT task; only that exact epoch may later ack.
           SET broker_id = p_broker_id, heartbeat_at = now(),
               claim_epoch = lo.claim_epoch + 1
          FROM picked p
         WHERE lo.resource_id = p.resource_id AND lo.transition = p.transition
           AND lo.generation = p.generation AND lo.binding_name = p.binding_name
           AND lo.shard_id = p.shard_id
        RETURNING lo.resource_id, lo.kind, lo.transition, lo.generation, lo.binding_name, lo.claim_epoch
    ), gone AS (  -- ORPHAN GC: a delivery whose resource was HARD-DELETED between emit
        -- and claim has nothing to react about. The final SELECT INNER-JOINs resources,
        -- so such a row would be claimed (epoch bumped, broker_id set) yet never RETURNED
        -- → never acked → reap_stale_lifecycle re-arms it → the next claim re-bumps and
        -- re-drops it: a claim→reap→claim ping-pong forever (no FK, no other GC). DELETE
        -- the orphan here in the same statement so it leaves the table instead. Keyed on
        -- the full per-binding tuple, like ack, so a sibling binding's live row is untouched.
        DELETE FROM lifecycle_outbox lo
          USING claimed c
         WHERE lo.resource_id = c.resource_id AND lo.transition = c.transition
           AND lo.generation = c.generation AND lo.binding_name = c.binding_name
           AND NOT EXISTS (SELECT 1 FROM resources r WHERE r.id = c.resource_id)
        RETURNING 1
    ), res AS (  -- read each resource's heavy status + name ONCE, not per binding
        SELECT c.resource_id AS rid, r.status, m.name
          FROM (SELECT DISTINCT resource_id FROM claimed) c
          JOIN resources r ON r.id = c.resource_id
          LEFT JOIN resource_meta m ON m.id = c.resource_id
    )
    SELECT c.resource_id, c.kind, res.name, c.transition, c.generation,
           res.status, c.binding_name, b.reactor,
           -- Resolve the reactor kind's single `reactor`-trigger reaction NAME and
           -- the kind_version it came from TOGETHER, so the version returned is exactly
           -- the one the reaction was resolved against. The worker keys its
           -- ReactionHandler by (reactor, reactor_kind_version, reaction), and pulls that
           -- version's DEFAULT providerconfig — so a v2 reactor runs its v2 handler
           -- with its v2 config, never silently v1. NULL reaction = the reactor CRD
           -- isn't applied at the resolved version → the dispatcher leaves the row for
           -- the reaper (at-least-once). kind_manifest is per-(kind, kind_version); a
           -- binding may PIN a reactor_version: the WHERE filters to the pinned version
           -- when b.reactor_version is set, else to ALL published versions, and
           -- ORDER BY km.kind_version DESC + LIMIT 1 takes the pinned one (single match)
           -- or the reactor's HIGHEST published version (unpinned) — so an unpinned
           -- binding follows a reactor upgrade automatically, a pinned one is immune.
           rv.reaction, rv.reactor_kind_version, c.claim_epoch
      FROM claimed c
      JOIN res ON res.rid = c.resource_id
      JOIN reactor_bindings b ON b.name = c.binding_name AND b.enabled
      LEFT JOIN LATERAL (
          SELECT rx->>'name' AS reaction, km.kind_version AS reactor_kind_version
            FROM kind_manifest km,
                 LATERAL jsonb_array_elements(km.reactions) AS rx
           WHERE km.kind = b.reactor AND rx->>'trigger' = 'reactor'
             AND (b.reactor_version IS NULL OR km.kind_version = b.reactor_version)
           ORDER BY km.kind_version DESC
           LIMIT 1
      ) rv ON TRUE;
$$ LANGUAGE sql;
-- +goose StatementEnd

-- ack_reactor_delivery: a delivery succeeded → DELETE its outbox row so it is
-- never re-delivered. Keyed by the full PER-BINDING dedup tuple (binding_name
-- included) so acking binding A's delivery NEVER touches binding B's row for the
-- same transition. A crash BEFORE ack leaves the row claimed with a stale
-- heartbeat; reap_stale_lifecycle frees it (bumping claim_epoch) for one more
-- at-least-once attempt. (DELETE-on-success keeps the table to just the in-flight
-- set — same "outbox is drained, not retained" shape as work_outbox.)
--
-- FENCED on claim_epoch: only the holder of the epoch the delivery was claimed
-- under may ack it. A dispatcher whose claim was reaped and re-issued to another
-- pod (its heartbeat lapsed, or it reconnected late) carries a stale epoch, so its
-- ack matches 0 rows and CANNOT delete the live re-delivery — the reaction still
-- fires (at-least-once preserved).
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION ack_reactor_delivery(
    p_resource_id UUID, p_transition TEXT, p_generation BIGINT, p_binding_name TEXT,
    p_claim_epoch BIGINT)
RETURNS void AS $$
    DELETE FROM lifecycle_outbox
     WHERE resource_id = p_resource_id
       AND transition = p_transition
       AND generation = p_generation
       AND binding_name = p_binding_name
       AND claim_epoch = p_claim_epoch;
$$ LANGUAGE sql;
-- +goose StatementEnd

-- reap_stale_lifecycle: dead dispatchers whose heartbeat lapsed lose their
-- claim; the row is freed (broker_id NULLed) so the next claim re-delivers it.
-- Exact clone of reap_stale_work over lifecycle_outbox — the at-least-once +
-- crash-recovery backstop (NOT the primary delivery path; the dispatcher's
-- own poll loop is). Also GCs rows whose binding vanished: a NULLed row with
-- no matching enabled binding is never re-claimed and ages out via vacuum.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION reap_stale_lifecycle(stale_seconds INTEGER, max_rows INTEGER, shard_lo SMALLINT, shard_hi SMALLINT)
RETURNS INTEGER AS $$
DECLARE
    n INTEGER;
BEGIN
    -- ZERO-COST IDLE GATE: this runs on EVERY reaper tick across all control
    -- pods (5s), but a stale claim can only exist if SOME row is currently
    -- claimed at all. Probe the partial heartbeat index (WHERE broker_id IS NOT
    -- NULL) with LIMIT 1 first — empty on the overwhelming common path (no
    -- in-flight reactor deliveries), so we skip the full 16-partition
    -- FOR UPDATE SKIP LOCKED sweep (~10ms) that otherwise burned CPU every tick
    -- even with zero reactor activity. Gate on "any claimed row" (NOT "any
    -- enabled binding"): a row whose binding was later disabled still needs
    -- reaping, so the row's own existence is the correct trigger.
    IF NOT EXISTS (
        SELECT 1 FROM lifecycle_outbox
         WHERE broker_id IS NOT NULL AND shard_id BETWEEN shard_lo AND shard_hi
         LIMIT 1
    ) THEN
        RETURN 0;
    END IF;

    WITH picked AS (
        SELECT resource_id, transition, generation, binding_name, shard_id FROM lifecycle_outbox
         WHERE broker_id IS NOT NULL
           AND heartbeat_at < now() - (stale_seconds || ' seconds')::interval
           AND shard_id BETWEEN shard_lo AND shard_hi  -- range → partition pruning
         ORDER BY heartbeat_at
         LIMIT max_rows
         FOR UPDATE SKIP LOCKED
    ), freed AS (
        UPDATE lifecycle_outbox lo
           -- Bump the fencing token as the claim is freed, so the prior holder's
           -- late ack (an older epoch) can never delete the re-issued delivery.
           SET broker_id = NULL, heartbeat_at = now(),
               claim_epoch = lo.claim_epoch + 1
          FROM picked p
         WHERE lo.resource_id = p.resource_id AND lo.transition = p.transition
           AND lo.generation = p.generation AND lo.binding_name = p.binding_name
           AND lo.shard_id = p.shard_id
         RETURNING lo.resource_id
    )
    SELECT count(*) INTO n FROM freed;
    RETURN n;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- resource_events: append-only audit log per resource.
CREATE TABLE resource_events (
    id            BIGSERIAL PRIMARY KEY,
    resource_id   UUID NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
    type          TEXT NOT NULL,
    actor         TEXT,
    reason        TEXT,
    message       TEXT,
    detail        JSONB NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_resource_events_by_resource
    ON resource_events (resource_id, created_at DESC, id DESC);

-- resource_conditions: K8s/Crossplane-style multi-axis status conditions.
-- ONE row per (resource_id, type); type is 'Ready' or a provider's own
-- custom axis (e.g. 'Healthy', 'Bound'). This is a NARROW side table on
-- purpose — conditions are NOT stored back on the wide resources row, so
-- writing one never re-TOASTs a million-row record, and it is written
-- ONLY on a transition (the drainer's upsert is a no-op when
-- status/reason/message are unchanged), so steady state costs nothing.
--
-- What is NOT stored here:
--   * Synced axis — fully derived from synced_gen vs generation; the API
--     synthesizes the Synced condition (+ latest WorkFailed event as its
--     reason) without a stored row.
--   * happy-path Ready=True for kinds that never emit conditions — the
--     health_ok scalar defaults true and the API synthesizes Ready=True
--     (Available). A row appears only once a provider actually reports
--     a Ready (or custom) condition, which only resync-probing / health-
--     aware kinds do. The 1M stress kinds emit none → zero rows here.
--
-- status is 'True' | 'False' | 'Unknown'; reason is a CamelCase machine
-- code; last_transition_at is stamped only when status changes (K8s
-- lastTransitionTime parity); observed_generation records which
-- generation the condition was computed against.
CREATE TABLE resource_conditions (
    resource_id          UUID NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
    type                 TEXT NOT NULL,
    status               TEXT NOT NULL,
    reason               TEXT NOT NULL DEFAULT '',
    message              TEXT NOT NULL DEFAULT '',
    observed_generation  BIGINT NOT NULL DEFAULT 0,
    last_transition_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (resource_id, type)
);

-- cluster_members: the running-fleet registry — one row per live PROCESS
-- (pod): a CLUSTER MEMBER, i.e. one instance of the Converge binary
-- identified by its ROLE. Every process (control, worker, worker-<kind>, or
-- role=all) UPSERTs ONE row here every ~30s (the ClusterMemberReporter driver,
-- maintained in cmd/converge/main.go), so the cluster view can list who is
-- up, their ROLE, the kinds they serve, the shard span they own, their booted
-- runtime config, and a live in-flight task count.
--
-- Liveness is K8s-style SOFT status with delayed GC, computed OFF this table:
--   * The API derives Ready vs NotReady at read time from last_heartbeat age
--     (> ~90s = 3 missed beats → NotReady) — there is no stored status column,
--     and the row is KEPT while NotReady so a just-crashed pod is still
--     visible (greyed) in the UI.
--   * On a CLEAN shutdown a member deletes its OWN row (deregisters) as the
--     last step, so it disappears from the cluster view immediately.
--   * gc_stale_cluster_members() (a Go ClusterMemberGC sweeper on the
--     ControlPlane, NOT pg_cron — that extension isn't enabled here) is the
--     FALLBACK: it hard-deletes rows past a LONGER TTL (~5m), reclaiming members
--     that crashed (no deregister ran) or whose deregister timed out. Cadence
--     invariant: beat 30s ≤ NotReady/3 ≤ TTL 5m.
--
-- last_heartbeat is stamped server-side with now() by the UPSERT (DB clock),
-- never an app timestamp, so cross-pod clock skew can't distort liveness.
-- started_at is set once on INSERT (the ON CONFLICT update preserves it) so
-- uptime is stable across beats.
--
-- LOGGED (unlike work_queue/kind_inflight/wake_state): this is config/identity
-- state, not a derived lease tally — it should survive a planned Postgres
-- restart so the cluster view doesn't blank out, and a crashed member's ghost
-- row is reclaimed by the GC TTL rather than relying on a crash TRUNCATE. The
-- table is tiny (one row per member, dozens of rows) and entirely off the 1M
-- hot path — the heartbeat is a single PK UPSERT, never on any per-task path.
--
-- shards is an int4range holding the member's owned contiguous shard span as
-- [lo, hi+1) (half-open, Postgres-canonical); NULL means it owns no shards
-- (e.g. a control-only member with no reaper slice). config is a small JSONB
-- blob of the booted runtime knobs (STATIC — written once, e.g. connect_addr + the
-- resolved sweeper/poll/pool defaults). workers is the LIVE per-beat snapshot of
-- the dumb workers connected to a broker (a JSON array; empty on
-- control/react and on a broker with none) — its OWN column, sibling to the live
-- in_flight tally, so live worker state never pollutes the static config blob.
-- No index beyond the PK — every query is a full scan of a dozens-of-rows table.
CREATE TABLE cluster_members (
    member_id      TEXT        PRIMARY KEY,            -- process id "host-pid-uuid8"
    role           TEXT        NOT NULL,               -- "all"|"control"|"broker"
    shards         int4range,                          -- owned contiguous shard span [lo,hi+1); NULL = none
    config         JSONB       NOT NULL DEFAULT '{}',  -- small STATIC booted runtime-config blob
    in_flight      INT         NOT NULL DEFAULT 0,     -- live claimed-task count (broker tier; 0 elsewhere)
    workers        JSONB       NOT NULL DEFAULT '[]',  -- live connected-worker snapshot (broker tier; [] elsewhere)
    version        TEXT        NOT NULL DEFAULT '',    -- BuildVersion (ldflags git-describe)
    hostname       TEXT        NOT NULL DEFAULT '',
    pid            INT         NOT NULL DEFAULT 0,
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(), -- process start (set once on INSERT)
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now()  -- stamped now() every beat (DB clock)
)
-- AUTOVACUUM TUNING for a tiny-but-write-hot table. cluster_members holds one
-- row per process (dozens), but EVERY heartbeat is an UPDATE — at a 53-process
-- fleet beating every 30s that's ~2 updates/sec, which under the DEFAULTS
-- (vacuum_threshold 50 + 0.2×rows ≈ 60 dead tuples) crosses the autovacuum
-- trigger within roughly ONE autovacuum_naptime (60s). The result is an
-- autovacuum+autoanalyze pass on this table on nearly every naptime wake — a
-- recurring CPU/WAL burst on the primary for zero benefit (the planner needs no
-- fresh stats on a dozens-of-rows table, and bloat is bounded by HOT pruning).
-- So: a generous fillfactor leaves in-page room so heartbeat UPDATEs stay HOT
-- (dead tuples are reclaimed by the cheap on-access prune, not a vacuum), and
-- the raised thresholds let dead tuples accrue to a few thousand before a vacuum
-- fires — turning a per-minute burst into a rare one. Off the hot path; the
-- table stays small and fully cached regardless.
WITH (
    fillfactor = 70,
    autovacuum_vacuum_threshold = 2000,
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_analyze_threshold = 2000,
    autovacuum_analyze_scale_factor = 0
);

-- notify_cluster_changed: fire a coalesced 'cluster_changed' NOTIFY whenever a
-- member JOINS (INSERT) or LEAVES (DELETE) the registry. This is the reactive half
-- of the cluster-topology reaction: every node's TopologyWatcher LISTENs on
-- 'cluster_changed' and, on a wake, runs its reactors — the Resharder (recompute this
-- node's contiguous shard range via assign_member_shards) and, on a broker, the mesh
-- (dial/drop peer routes). Both read the LIVE (liveness-filtered) membership, so a
-- JOIN or a graceful LEAVE re-tiles + re-connects within ~one NOTIFY window.
--
-- Fires ONLY on INSERT/DELETE, never UPDATE — so the heartbeat UPSERT (an ON CONFLICT
-- DO UPDATE, which fires the UPDATE trigger path, not INSERT) does NOT spam it, and a
-- member writing back its freshly-assigned `shards` span (also an UPDATE) can't feed
-- back into a reshard loop. A genuine new member is a true INSERT (fires); a
-- deregister / GC reclaim is a DELETE (fires). A HARD CRASH fires NO event (the row
-- lingers, only its heartbeat lapses) — that case is caught by the watcher's failsafe
-- poll re-reading the liveness-filtered membership, NOT by this trigger.
-- Gated via notify_gated so a mass join/leave can't storm the async-queue lock.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION notify_cluster_changed() RETURNS trigger AS $$
BEGIN
    PERFORM notify_gated('cluster_changed');
    RETURN NULL; -- AFTER trigger: return value ignored
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER cluster_members_changed
    AFTER INSERT OR DELETE ON cluster_members
    FOR EACH ROW EXECUTE FUNCTION notify_cluster_changed();

-- notify_providerconfig_changed: fire a coalesced 'providerconfig_changed'
-- NOTIFY when a DEFAULT config changes — but NOT when a custom (non-default)
-- override changes. The two roles in providerconfigs have opposite propagation:
--
--   * DEFAULT (is_default = TRUE): loaded at boot and CAPTURED by the worker; an
--     edit must be LIVE-PUSHED so providers re-dial / re-parameterise with no
--     restart. That is what this NOTIFY drives — every node's reconfigure
--     listener wakes, re-reads the per-kind defaults, and pushes the changed one.
--
--   * CUSTOM (is_default = FALSE): cloned into work_queue.provider_config at
--     schedule time, exactly like spec. Its edits take effect on the resource's
--     NEXT schedule — firing the reconfigure wake for them would be pure noise.
--
-- The discriminator is the explicit is_default flag, so the gate is exact (no
-- attachment heuristics). An UPDATE that flips is_default either way also
-- notifies — a kind's default set is changing. providerconfigs is tiny, so a
-- per-row trigger here never touches the ~1M reconcile hot path.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION notify_providerconfig_changed() RETURNS trigger AS $$
BEGIN
    IF (TG_OP = 'INSERT' AND NEW.is_default)
       OR (TG_OP = 'UPDATE' AND (NEW.is_default OR OLD.is_default))
       OR (TG_OP = 'DELETE' AND OLD.is_default) THEN
        PERFORM notify_gated('providerconfig_changed');
    END IF;
    RETURN NULL; -- AFTER trigger: return value ignored
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- One trigger for all ops: the is_default gate lives in the function body (which
-- can read NEW or OLD per TG_OP), so no NEW/OLD WHEN-clause split is needed.
CREATE TRIGGER providerconfigs_changed
    AFTER INSERT OR UPDATE OR DELETE ON providerconfigs
    FOR EACH ROW EXECUTE FUNCTION notify_providerconfig_changed();

-- pointer_to_path: convert an RFC 6901 JSON Pointer ("/foo/bar") into
-- a TEXT[] suitable for jsonb_set / `#>`.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION pointer_to_path(p TEXT) RETURNS TEXT[] AS $$
BEGIN
    IF p IS NULL OR p = '' OR p = '/' THEN
        RETURN ARRAY[]::TEXT[];
    END IF;
    IF left(p, 1) = '/' THEN
        RETURN string_to_array(substring(p from 2), '/');
    END IF;
    RETURN ARRAY[p];
END;
$$ LANGUAGE plpgsql IMMUTABLE;
-- +goose StatementEnd

-- jsonb_set_many: apply a list of {p:path, v:value} patches.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION jsonb_set_many(doc JSONB, patches JSONB) RETURNS JSONB AS $$
DECLARE
    out JSONB := doc;
    p   JSONB;
BEGIN
    FOR p IN SELECT jsonb_array_elements(patches) LOOP
        out := jsonb_set(out, ARRAY(SELECT jsonb_array_elements_text(p->'p')), p->'v', true);
    END LOOP;
    RETURN out;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
-- +goose StatementEnd

-- apply_value_flows_for_dependents: re-substitute every flow on the
-- given dependents using the current status of each flow's upstream.
-- Called by the composer post-commit (before schedule_eligible) so a
-- newly-composed child whose upstream was already ready gets its
-- flowed spec fields populated before the dep-gate releases it.
--
-- The drainer's substitute pass handles the steady-state re-application
-- on every upstream reconcile; this function is the bootstrap.
--
-- KIND-VERSION-SKEW: a NULL/absent src pointer here is SILENTLY skipped ON PURPOSE
-- — it is the legitimate "upstream hasn't produced that status field YET"
-- bootstrap case, indistinguishable at runtime from a genuine skew without the
-- per-edge src_kind_version baseline. Distinguishing them on this hot path (a condition
-- write per skipped flow) would tax every 1M reconcile AND false-alarm on every
-- normal bootstrap. Instead the skew is surfaced fail-loud at the ONE moment it
-- can be introduced: FlipResourceKindVersion WARNs on every resource_deps edge whose
-- src_kind_version no longer matches the flipped upstream's new kind_version (see flip_manual.go
-- + FlipSkewEdgesSQL) and re-baselines src_kind_version. So a renamed-away upstream field
-- is operator-visible at flip time, while the runtime path stays silent + fast.
--
-- Patches resources.spec INLINE (jsonb_set_many): children carry their spec
-- inline and are never versioned, so substitution is an in-place UPDATE
-- exactly as before spec history existed — no side-table write on this path.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION apply_value_flows_for_dependents(dep_ids UUID[]) RETURNS VOID AS $$
BEGIN
    IF dep_ids IS NULL OR array_length(dep_ids, 1) = 0 THEN
        RETURN;
    END IF;
    WITH dep_flows AS (
        SELECT
            d.dependent_id,
            d.dependency_id,
            elem
        FROM resource_deps d, jsonb_array_elements(d.value_flows) AS elem
        WHERE d.dependent_id = ANY(dep_ids)
    ), candidate AS (
        SELECT
            df.dependent_id,
            pointer_to_path(elem->>'dep_field') AS dep_path,
            u.status #> pointer_to_path(elem->>'src_field') AS src_value
        FROM dep_flows df
        JOIN resources u ON u.id = df.dependency_id
        WHERE u.status IS NOT NULL
          AND u.synced_gen >= u.generation
          AND (u.status #> pointer_to_path(elem->>'src_field')) IS NOT NULL
          AND (u.status #> pointer_to_path(elem->>'src_field')) != 'null'::jsonb
    ), applicable AS (
        SELECT c.dependent_id, c.dep_path, c.src_value
          FROM candidate c
          JOIN resources r ON r.id = c.dependent_id
         WHERE (r.spec #> c.dep_path) IS DISTINCT FROM c.src_value
           -- FROZEN dependents are NOT patched/bumped: an orphaned (pending
           -- teardown) or quarantined (set-aside) dependent must not have its
           -- generation bumped by an upstream's status change — that would make
           -- it generation > synced_gen and resurrect it on the next schedule.
           -- It re-captures the flowed value when it un-freezes and reconciles.
           AND r.frozen_until IS NULL          -- orphaned OR quarantined = frozen
    ), per_dep AS (
        SELECT dependent_id,
               jsonb_agg(jsonb_build_object('p', dep_path, 'v', src_value)) AS patches
        FROM applicable
        GROUP BY dependent_id
    ), locked AS (
        -- DETERMINISTIC LOCK ORDERING (mirrors drain_outbox_batch). This is
        -- the COMPOSE-path twin of the drainer's substitute pass: it patches
        -- the SAME cross-shard dependent rows, and runs concurrently with
        -- drainers (a worker composing one root while drainers apply other
        -- roots' completions). If this UPDATE ... FROM locked its rows in
        -- join order while the drainer locks id-ascending, two transactions
        -- touching the same two dependents in opposite orders deadlock. Lock
        -- here in the SAME ascending-id order every resources writer uses, so
        -- the whole system has one global lock order and no cycle can form.
        SELECT r.id, pd.patches
        FROM per_dep pd
        JOIN resources r ON r.id = pd.dependent_id
        ORDER BY r.id
        -- FOR NO KEY UPDATE: writes only non-key columns, so this is the lock
        -- the UPDATE takes anyway; unlike FOR UPDATE it doesn't block the FOR
        -- KEY SHARE concurrent child/meta INSERTs take on these rows.
        FOR NO KEY UPDATE OF r
    )
    -- per_dep is already only dependents whose flowed value genuinely
    -- differs (applicable gates IS DISTINCT FROM), so the bump is
    -- unconditional here — bumping at the source replaces the dropped
    -- bump_generation trigger.
    UPDATE resources r
       SET spec = jsonb_set_many(r.spec, lk.patches),
           generation = r.generation + 1,
           updated_at = now()
      FROM locked lk
     WHERE r.id = lk.id;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- schedule_eligible: scheduler. Given candidate resource ids, find the
-- ones whose reconcile task can run now and insert a work_queue row.
-- Eligibility: needs reconcile (generation > synced_gen), no pending
-- deletion, every direct upstream caught up (p.synced_gen >= p.generation).
--
-- Single 'reconcile' task type — the kind's pipeline (compose? + work?
-- + rollup?) runs as one worker invocation. Replaces the old
-- compose/work/rollup task-type triage.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION schedule_eligible(candidate_ids UUID[]) RETURNS INTEGER AS $$
DECLARE
    n INTEGER;
BEGIN
    WITH eligible AS (
        SELECT r.id, r.kind, r.kind_version, r.generation, r.spec, r.provider_config_id, r.manifest_version
        FROM resources r
        WHERE r.id = ANY(candidate_ids)
          AND r.generation > r.synced_gen
          AND r.deletion_requested_at IS NULL
          -- FROZEN (frozen_until IS NOT NULL) — a resource set aside is NEVER
          -- (re)scheduled, even if a dependency's status change flowed into it
          -- and bumped its generation (the cascade feeds it as a candidate
          -- regardless). Covers BOTH:
          --   * ORPHANED (finite frozen_until), pending teardown. Re-activating a
          --     row on its way out would resurrect work for a resource the reaper
          --     is about to delete. (A re-emit clears frozen_until — the ONLY way
          --     it becomes schedulable again.)
          --   * QUARANTINED ('infinity'), a failed resource an operator set aside
          --     to stop it hammering + to unblock its root's rollup. Un-quarantine
          --     (clear the mark) or a spec edit re-arms it.
          AND r.frozen_until IS NULL
          -- Terminal-failure stop-gate: a row marked terminal for the live
          -- spec (won't succeed without an edit) is NOT re-queued. A spec
          -- edit bumps generation → failure_gen <> generation → eligible
          -- again. Transient failures (failure_terminal=false) still retry.
          AND NOT (r.failure_terminal AND r.failure_gen = r.generation)
          -- Upstream gate: every direct dependency must be caught up.
          AND NOT EXISTS (
              SELECT 1 FROM resource_deps d
              JOIN resources p ON p.id = d.dependency_id
              WHERE d.dependent_id = r.id
                AND p.synced_gen < p.generation
          )
          -- Descendant gate: a rollup-having root waits only while a
          -- descendant is still PROGRESSING (lagging AND not failed). A
          -- descendant that has FAILED (failure_gen = generation) no longer
          -- blocks the root — otherwise a permanently-doomed child pins the
          -- root in invisible limbo forever (is_ready=false, zero
          -- conditions, never scheduled). Releasing the root lets its
          -- rollup run, observe the failed child, and emit
          -- Ready=False/ChildrenNotReady → the root surfaces as Degraded
          -- instead of vanishing. Scoped to roots (owner_id IS NULL): a
          -- non-root never owns a subtree keyed by its own id, so the probe
          -- is provably empty for every leaf; the owner_id IS NOT NULL
          -- short-circuit (an in-row NOT NULL test) skips it for the ~1M
          -- leaves while keeping the real composer-root suppression intact.
          AND (
              r.owner_id IS NOT NULL
              OR NOT EXISTS (
                  SELECT 1 FROM resources d
                  WHERE d.root_id = r.id
                    AND d.synced_gen < d.generation
                    AND d.failure_gen <> d.generation   -- failed children don't block
                    AND d.deletion_requested_at IS NULL
                    AND d.frozen_until IS NULL           -- frozen (orphaned/quarantined) children don't block the rollup
              )
          )
        ORDER BY r.id
    ), ins AS (
        -- provider_config / provider_bundle: clone the resource's CUSTOM config
        -- (if any) — its spec AND its opaque bundle — from the referenced
        -- providerconfigs row. LEFT JOIN so the no-custom common case
        -- (provider_config_id NULL) yields NULL for both with no extra cost.
        INSERT INTO work_queue (resource_id, task_type, kind, kind_version, generation, spec, provider_config, provider_bundle, manifest_version, shard_id)
        SELECT e.id, 'reconcile'::task_type, e.kind, e.kind_version, e.generation, e.spec, pc.spec, pc.data,
               e.manifest_version, shard_of(e.id)
        FROM eligible e
        LEFT JOIN providerconfigs pc ON pc.id = e.provider_config_id
        ORDER BY e.id
        ON CONFLICT (resource_id, task_type, shard_id) DO UPDATE
            SET kind = EXCLUDED.kind, kind_version = EXCLUDED.kind_version,
                generation = EXCLUDED.generation, spec = EXCLUDED.spec,
                provider_config = EXCLUDED.provider_config,
                provider_bundle = EXCLUDED.provider_bundle,
                manifest_version = EXCLUDED.manifest_version,
                -- Re-pointing an in-flight row to a newer generation frees the claim
                -- (broker_id NULL) and bumps the fencing token in the same statement,
                -- so a result from the prior claim carries a stale epoch and no-ops.
                claim_epoch = work_queue.claim_epoch + 1,
                broker_id = NULL, worker_id = NULL, heartbeat_at = now(), created_at = now()
            WHERE work_queue.generation < EXCLUDED.generation
        RETURNING resource_id
    )
    SELECT count(*) INTO n FROM ins;
    -- Wake idle worker dispatchers iff we enqueued something. Every enqueue
    -- path (apply, cascade trigger, reaper/resyncer requeue, rollback) funnels
    -- through here, so this one call covers them all.
    --
    -- DIRECT pg_notify, NOT the notify_gated() coalescer: work_ready is a
    -- per-STATEMENT / per-BATCH wake (schedule_eligible runs once per
    -- drain/cascade/requeue batch — the cascade trigger is FOR EACH STATEMENT,
    -- not per row), so its fleet-wide rate is at most low-hundreds/sec — far
    -- below the async-notification-queue-lock storm threshold the gate exists
    -- to prevent (that storm was the PER-ROW outbox_ready path, which keeps the
    -- gate). Gating work_ready was actively HARMFUL: notify_gated takes
    -- pg_try_advisory_XACT_lock, held until COMMIT, and schedule_eligible fires
    -- inside the drainer's seconds-long drain tx — so a concurrent drainer
    -- enqueuing the LAST child's rollup re-pend lost the advisory race, returned
    -- without notifying, and stranded that root until a failsafe poll (the
    -- "rollup pending for ~5s, sometimes instant" symptom). A direct pg_notify
    -- is committed atomically with the work_queue insert (same tx) and ALWAYS
    -- delivered on commit, so a freshly-enqueued row can never lose its wake.
    -- The dispatcher's buffered-1 wakeup chan coalesces the resulting bursts.
    IF n > 0 THEN
        PERFORM pg_notify('work_ready', '');
    END IF;
    RETURN n;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- reap_stale_work: dead workers whose heartbeat lapsed lose their
-- claim; the work_queue row is freed (broker_id NULLed) so the next
-- scheduler tick can re-issue it. No condition writes — failures
-- surface via resource_events + work_outbox.error_message; the next
-- reconcile attempt either succeeds or the row stays lagging.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION reap_stale_work(stale_seconds INTEGER, max_rows INTEGER, shard_lo SMALLINT, shard_hi SMALLINT)
RETURNS INTEGER AS $$
DECLARE
    n INTEGER;
BEGIN
    -- ZERO-COST IDLE GATE: this runs on EVERY reaper tick across all control
    -- pods (5s/10s idle), but a stale claim can only exist if SOME row is
    -- currently leased. Probe the partial leased index (idx_work_queue_leased,
    -- WHERE broker_id IS NOT NULL) with LIMIT 1 first — on an idle/quiescent
    -- fleet nothing is claimed, so this index-only probe returns instantly and
    -- we skip the all-16-partition FOR UPDATE SKIP LOCKED sweep that otherwise
    -- fans across every partition's index every tick (measured the single most
    -- expensive idle reaper sub-op). Mirrors the gate on reap_stale_lifecycle.
    IF NOT EXISTS (
        SELECT 1 FROM work_queue
         WHERE broker_id IS NOT NULL AND shard_id BETWEEN shard_lo AND shard_hi
         LIMIT 1
    ) THEN
        RETURN 0;
    END IF;

    WITH picked AS (
        SELECT id, shard_id FROM work_queue
         WHERE broker_id IS NOT NULL
           AND heartbeat_at < now() - (stale_seconds || ' seconds')::interval
           AND shard_id BETWEEN shard_lo AND shard_hi  -- range → partition pruning
           -- A finished task whose result is ALREADY in work_outbox (awaiting the
           -- drainer) is NOT abandoned — it just looks stale because the worker
           -- stopped heartbeating it on completion and the drain is backlogged.
           -- Re-pending it would let a second worker re-run the SAME work_id and
           -- collide on the undrained work_outbox PK (23505). Skip those: the
           -- drainer will delete this row. Only genuinely-orphaned claims (no
           -- outbox row) are reaped.
           AND NOT EXISTS (
               SELECT 1 FROM work_outbox o
                WHERE o.work_id = work_queue.id AND o.shard_id = work_queue.shard_id
           )
         ORDER BY heartbeat_at
         LIMIT max_rows
         FOR UPDATE SKIP LOCKED
    ), freed AS (
        UPDATE work_queue q
           -- Bump the fencing token in the same statement that frees the claim, so
           -- a result from the reaped claim (a worker that comes back late) carries
           -- a stale epoch and no-ops — including the SAME-pod case, where broker_id
           -- alone would have matched.
           SET broker_id = NULL, worker_id = NULL, heartbeat_at = now(),
               claim_epoch = q.claim_epoch + 1
          FROM picked
         -- shard_id in the join prunes the UPDATE to the same partition(s)
         -- the inner scan pruned to, instead of re-scanning all 16.
         WHERE q.id = picked.id AND q.shard_id = picked.shard_id
         RETURNING q.id
    )
    SELECT count(*) INTO n FROM freed;
    RETURN n;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- delete_unclaimed_work: GARBAGE-COLLECT abandoned tasks. Hard-deletes
-- work_queue rows that have sat UNCLAIMED (broker_id IS NULL) for longer than
-- delete_seconds. This is NOT recovery — reap_stale_work already re-pends a
-- stale CLAIMED row (a worker died mid-task). This deletes the opposite case:
-- a row that was scheduled and then NOTHING ever claimed it for a full day,
-- because no worker exists for its kind (provider removed / kind decommissioned
-- / a permanently-empty shard band). Such a row would otherwise sit in the
-- queue forever, indexed in idx_work_queue_pending and re-scanned by every
-- claim for that kind. delete_seconds is intentionally long (default 24h) so a
-- transiently-down worker fleet has ample time to come back and drain its
-- backlog before anything is reclaimed.
--
-- created_at is the age signal: schedule_eligible's ON CONFLICT re-stamps
-- created_at = now() on every (re)schedule, and reap_stale_work frees a claim
-- WITHOUT touching created_at — so created_at is "last (re)scheduled", and an
-- old created_at on an unclaimed row means it was offered to the fleet long ago
-- and never taken up. (heartbeat_at would be wrong here: the reaper bumps it on
-- free, so a row that flapped claimed→reaped→unclaimed would look fresh.)
--
-- A deleted row is genuinely dropped, not retried: the resource stays lagging
-- (generation > synced_gen), so if a worker for that kind ever returns,
-- requeue_failed_and_pending re-enqueues a fresh row on its next tick — the
-- delete reclaims the dead queue entry, it does not abandon the resource.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION delete_unclaimed_work(delete_seconds INTEGER, max_rows INTEGER, shard_lo SMALLINT, shard_hi SMALLINT)
RETURNS INTEGER AS $$
DECLARE
    n INTEGER;
BEGIN
    -- ZERO-COST IDLE GATE: this runs on every reaper tick, but only matters
    -- when some unclaimed row is OLD. Probe the partial pending index
    -- (idx_work_queue_pending, WHERE broker_id IS NULL) for a single row past
    -- the threshold first — on a healthy fleet every pending row is claimed
    -- within seconds, so this index probe returns nothing and we skip the
    -- all-partition FOR UPDATE SKIP LOCKED sweep entirely. Mirrors the gate on
    -- reap_stale_work / reap_stale_lifecycle.
    IF NOT EXISTS (
        SELECT 1 FROM work_queue
         WHERE broker_id IS NULL
           AND created_at < now() - (delete_seconds || ' seconds')::interval
           AND shard_id BETWEEN shard_lo AND shard_hi
         LIMIT 1
    ) THEN
        RETURN 0;
    END IF;

    WITH picked AS (
        SELECT id, shard_id FROM work_queue
         WHERE broker_id IS NULL
           AND created_at < now() - (delete_seconds || ' seconds')::interval
           AND shard_id BETWEEN shard_lo AND shard_hi  -- range → partition pruning
         ORDER BY created_at
         LIMIT max_rows
         -- SKIP LOCKED so a concurrent claim grabbing this exact row (the rare
         -- race where a worker returns at the threshold edge) wins — we skip it
         -- and the now-claimed row is no longer our concern.
         FOR UPDATE SKIP LOCKED
    ), gone AS (
        DELETE FROM work_queue q
         USING picked
         -- shard_id in the join prunes the DELETE to the same partition(s) the
         -- inner scan pruned to, instead of re-scanning all 16.
         WHERE q.id = picked.id AND q.shard_id = picked.shard_id
         RETURNING q.id
    )
    SELECT count(*) INTO n FROM gone;
    RETURN n;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- sweep_expired_orphans: ORPHAN-GRACE expiry. When the composer drops a child
-- it no longer produces, it does NOT delete the row — it stamps frozen_until =
-- now() + the kind's orphan_grace_secs and leaves the row fully in the DAG (so
-- a re-emit next compose cycle re-adopts it untouched; that clears frozen_until
-- in the composer tx). This function is the OTHER end: once
-- the grace window has actually elapsed (frozen_until < now()) and the child
-- was NOT re-emitted, it escalates the row into the real delete machinery —
-- the deletion the composer deferred finally happens.
--
-- A QUARANTINED row (frozen_until = 'infinity') is NEVER swept, with NO special
-- predicate: 'infinity' is never < now(), so the `frozen_until < now()` gate
-- excludes it for free. The reaper must never delete a quarantined row (the
-- operator set it aside; it is not teardown), and the sentinel guarantees that.
--
-- Two escalation paths, decided per row by the kind's finalizer (read from
-- kind_config — the reaper is a control pod with NO Go registry, so the
-- finalizer name must be available in-DB):
--   * finalizer present  → SOFT delete: replicate request_resource_deletion's
--     body set-based (stamp deletion_requested_at, seed the finalizer, enqueue
--     the 'delete' task with the cloned custom config, emit a DeleteRequested
--     event). The kind's Deleter then runs and the drainer hard-deletes the row
--     once the finalizer clears. We clear frozen_until here because the row is
--     now 'Deleting', not 'Orphaned' (phase precedence).
--   * finalizer NULL/''  → HARD delete: the row is just data, drop it. The
--     cleanup_orphaned_deps AFTER-DELETE trigger sweeps its edges/work_queue/
--     spec_versions; ON DELETE CASCADE removes any grandchildren.
--
-- Grace is NOT a function arg — it is baked into frozen_until by the composer
-- at stamp time (per-kind from kind_config.orphan_grace_secs), so the sweep
-- only compares frozen_until to now(). Same janitor discipline as
-- delete_unclaimed_work: zero-cost idle gate, FOR UPDATE SKIP LOCKED, shard
-- BETWEEN range pruning. Idempotent — a row already draining
-- (deletion_requested_at set) is excluded, so a re-run is a no-op.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION sweep_expired_orphans(max_rows INTEGER, shard_lo SMALLINT, shard_hi SMALLINT)
RETURNS INTEGER AS $$
DECLARE
    n INTEGER;
BEGIN
    -- ZERO-COST IDLE GATE: probe idx_resources_frozen_sweep (partial WHERE
    -- frozen_until IS NOT NULL) for one grace-expired orphan in range. Steady
    -- state has nothing frozen → empty partial index → instant exit, skipping
    -- the FOR UPDATE SKIP LOCKED sweep. `frozen_until < now()` excludes the
    -- 'infinity' quarantined rows for free. Mirrors delete_unclaimed_work's gate.
    IF NOT EXISTS (
        SELECT 1 FROM resources
         WHERE frozen_until < now()        -- orphans whose grace elapsed; never 'infinity'
           AND deletion_requested_at IS NULL
           AND shard_id BETWEEN shard_lo AND shard_hi
         LIMIT 1
    ) THEN
        RETURN 0;
    END IF;

    WITH picked AS (
        SELECT r.id, r.shard_id, r.kind, r.kind_version, r.generation, r.manifest_version, r.spec,
               kc.finalizer_name AS finalizer,
               (SELECT pc.spec FROM providerconfigs pc WHERE pc.id = r.provider_config_id) AS pcfg,
               (SELECT pc.data FROM providerconfigs pc WHERE pc.id = r.provider_config_id) AS pbundle
          FROM resources r
          LEFT JOIN kind_config kc ON kc.kind = r.kind AND kc.kind_version = r.kind_version
         WHERE r.frozen_until < now()       -- orphan grace elapsed; 'infinity' (quarantined) never matches
           AND r.deletion_requested_at IS NULL          -- not already draining
           AND r.shard_id BETWEEN shard_lo AND shard_hi  -- range → partition pruning
         ORDER BY r.frozen_until
         LIMIT max_rows
         FOR UPDATE OF r SKIP LOCKED
    ),
    -- Finalizer kinds → soft-delete (request_resource_deletion, set-based).
    -- Clearing frozen_until flips the row from 'Orphaned' to 'Deleting'
    -- (phase precedence: Deleting wins over Orphaned).
    soft AS (
        UPDATE resources r
           SET deletion_requested_at = now(),
               finalizers            = ARRAY[p.finalizer],
               frozen_until          = NULL,
               updated_at            = now()
          FROM picked p
         WHERE r.id = p.id AND p.finalizer IS NOT NULL AND p.finalizer <> ''
        RETURNING r.id, p.kind AS kind, p.kind_version AS kind_version, p.generation AS generation, p.spec AS spec, p.pcfg AS pcfg, p.pbundle AS pbundle, p.manifest_version AS manifest_version, p.shard_id AS shard_id
    ),
    soft_q AS (
        -- Carry the resource's OWN spec on the teardown task (see sweep_deletable) —
        -- an IaC destroy needs the module source + vars; a NULL spec hot-loops.
        INSERT INTO work_queue (resource_id, task_type, kind, kind_version, generation, spec, provider_config, provider_bundle, manifest_version, shard_id)
        SELECT s.id, 'delete'::task_type, s.kind, s.kind_version, s.generation, s.spec, s.pcfg, s.pbundle,
               s.manifest_version, s.shard_id
        FROM soft s
        ON CONFLICT (resource_id, task_type, shard_id) DO NOTHING
        RETURNING resource_id
    ),
    soft_ev AS (
        INSERT INTO resource_events (resource_id, type, actor, reason, message)
        SELECT s.id, 'DeleteRequested', 'reaper-orphan-sweep', 'GraceExpired',
               'orphan grace expired; delete requested by reaper-orphan-sweep'
        FROM soft s
        RETURNING resource_id
    ),
    -- Leaf kinds (no finalizer) → hard delete. cleanup_orphaned_deps fires
    -- AFTER DELETE and sweeps edges/work_queue/spec_versions; ON DELETE CASCADE
    -- on owner_id removes any grandchildren.
    hard AS (
        DELETE FROM resources r
         USING picked p
         WHERE r.id = p.id AND (p.finalizer IS NULL OR p.finalizer = '')
        RETURNING r.id
    )
    -- Force soft_q/soft_ev to materialize even though data-modifying CTEs
    -- always run in Postgres; counting them keeps the optimizer honest and
    -- makes the row total = soft-deleted + hard-deleted.
    SELECT (SELECT count(*) FROM soft)
         + (SELECT count(*) FROM hard)
         + 0 * ((SELECT count(*) FROM soft_q) + (SELECT count(*) FROM soft_ev))
      INTO n;
    RETURN n;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- recount_inflight: SELF-HEALING ground-truth recount of per-kind in-flight
-- for ONE reaper's contiguous shard range [p_shard_lo, p_shard_hi]. Called
-- on the reaper tick. This is the ONLY thing that keeps the cap honest: the
-- claim's per-pod +N is just an optimistic hint, and drain/reap/re-point do
-- NO counter bookkeeping at all — the recount absorbs every completion,
-- reap, and orphaned re-point by recomputing the truth.
--
-- RANGE OWNERSHIP (the key to correctness across mismatched slicings):
-- workers slice the 256 shards finely (≈50 ranges) and write hint partials
-- keyed by their own range_lo; reapers slice coarsely (≈3 ranges). This
-- function takes total ownership of its range: it DELETES every partial
-- whose range_lo lies in [p_shard_lo, p_shard_hi] (reclaiming all the finer
-- worker hints inside it) and writes ONE authoritative partial per capped
-- kind, keyed range_lo = p_shard_lo, holding the true leased count for the
-- whole range. Because reaper ranges tile [0,255] disjointly, every partial
-- is owned by exactly one reaper, so after a full sweep SUM(in_flight) over
-- a kind's rows is the exact global count.
--
-- PERF: the count is range-bounded (`shard_id BETWEEN lo AND hi`) so it
-- prunes to the reaper's partition(s) and rides idx_work_queue_leased; it is
-- NOT the forbidden all-partition aggregate (that one has no shard bound).
-- Off the hot claim path; only for kinds that actually have a cap.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION recount_inflight(p_shard_lo SMALLINT, p_shard_hi SMALLINT)
RETURNS VOID AS $$
BEGIN
    -- Reclaim every partial (worker hints + our own prior row) inside this
    -- reaper's range, so the authoritative INSERT below is the only survivor.
    DELETE FROM kind_inflight
    WHERE range_lo BETWEEN p_shard_lo AND p_shard_hi;

    -- One authoritative partial per CAPPED (kind, kind_version) (max_inflight > 0;
    -- uncapped kinds have no tally): the true leased count in [lo,hi] (0 when
    -- none), keyed at range_lo = p_shard_lo. The cap is per-kind_version, so the live
    -- count groups by (kind, kind_version) and the join keys on both — a v1 lease never
    -- counts against the v2 budget.
    INSERT INTO kind_inflight (kind, kind_version, range_lo, in_flight, refreshed_at)
    SELECT c.kind, c.kind_version, p_shard_lo, COALESCE(live.n, 0), now()
    FROM kind_config c
    LEFT JOIN (
        SELECT kind, kind_version, count(*)::int AS n
        FROM work_queue
        WHERE broker_id IS NOT NULL
          AND shard_id BETWEEN p_shard_lo AND p_shard_hi   -- range → partition pruning
        GROUP BY kind, kind_version
    ) live ON live.kind = c.kind AND live.kind_version = c.kind_version
    WHERE c.max_inflight > 0;   -- only CAPPED (kind, kind_version) get a tally; uncapped skip it

    -- Stale-partial sweep: drop authoritative rows from a reaper range that
    -- stopped recounting (pod died / range reassigned), so a dead range
    -- can't inflate the global SUM forever. 5 min is many reaper intervals;
    -- a live reaper re-creates its row each tick, so this only removes
    -- orphans. (Worker hint rows are reclaimed by the DELETE above within
    -- one tick of the owning reaper, so they never reach this window.)
    DELETE FROM kind_inflight
    WHERE refreshed_at < now() - interval '5 minutes';
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- cascade_on_ready_change: AFTER STATEMENT trigger. When a row catches
-- up on the SYNCED axis (synced_gen >= generation, was lagging),
-- schedule (a) every direct dependent and (b) the rollup-having root
-- each upgraded descendant belongs to. Keyed on synced_gen, NOT
-- is_ready, on purpose: value flows substitute from the upstream's
-- status, which is durable once synced — a later health (health_ok)
-- flip doesn't invalidate the flowed value, so dependents need
-- re-scheduling only on the synced transition. Health demotion is
-- handled out-of-band, level-triggered: the resync sweeper re-pends a
-- composite to re-run its rollup, which aggregates descendant is_ready
-- into the parent's health_ok. There is deliberately no edge-triggered
-- demotion cascade (it would put health flips on the hot path).
--
-- REACTIVE ROLLUP: scheduling the root here is what makes a composite's
-- rollup fire immediately once its LAST descendant syncs, instead of
-- waiting for the reaper's poll + RetryAfter age gate (a multi-second
-- lag). The root goes through the SAME schedule_eligible call as the
-- edge-dependents, so it inherits every gate.
--
-- This does NOT reintroduce the pre-v2 owner-cascade convoy. That
-- failure mode wrote the single shared root work_queue row on EVERY
-- drainer batch (~1M descendants → 8 goroutines serialized on one
-- lock). Here, schedule_eligible's descendant gate (a single
-- idx_resources_root_lagging probe) filters the root out on every batch
-- where ANY descendant still lags — those batches do ZERO work_queue
-- writes for the root. Only the converging batch (0 descendants lagging)
-- performs the one INSERT, and the ON CONFLICT gen-gate makes even
-- concurrent attempts idempotent. The per-batch cost added for a root is
-- just that one indexed EXISTS probe, not a lock-contending write.
--
-- Rare fallback: if a root's final descendants commit across two
-- concurrently-running drain batches, neither batch's MVCC snapshot sees
-- 0 descendants lagging, so neither enqueues the root. The reaper's
-- requeue_failed_and_pending then picks it up on its next tick — only on
-- the genuine straddle. The reaper is the backstop (and covers crash
-- recovery); the cascade is the primary rollup-scheduling path.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION cascade_on_ready_change() RETURNS trigger AS $$
DECLARE
    upgraded UUID[];
    candidates UUID[];
    demoted_roots UUID[];
    upgraded_roots UUID[]; -- rollup-having roots of this batch's upgraded descendants (straddle candidates)
    n_armed INTEGER;       -- how many straddled roots we queued for deferred recheck
    n_repended INTEGER;    -- how many already-settled roots we re-pended for a re-promotion rollup
BEGIN
    SELECT array_agg(n.id ORDER BY n.id) INTO upgraded
      FROM new_rows n JOIN old_rows o ON o.id = n.id
     WHERE n.synced_gen >= n.generation
       AND o.synced_gen <  o.generation;

    -- REACTIVE DEMOTION: descendants that just went unhealthy or just
    -- FAILED this batch. Their composite root must re-run its rollup NOW
    -- (fold the demotion into its own health_ok / phase) instead of
    -- waiting for the next resync tick — the level-triggered lag the
    -- status-model audit flagged. Scoped to descendants (root_id NOT NULL)
    -- so a root's own flip doesn't self-schedule, and on the 1M healthy
    -- run nothing flips → this set is empty → no extra work.
    SELECT array_agg(DISTINCT n.root_id) INTO demoted_roots
      FROM new_rows n JOIN old_rows o ON o.id = n.id
     WHERE n.root_id IS NOT NULL
       AND ( (NOT n.health_ok AND o.health_ok)                       -- health flip true→false
          OR (n.failure_gen = n.generation
              AND o.failure_gen IS DISTINCT FROM o.generation) );    -- newly failed

    IF (upgraded IS NULL OR array_length(upgraded, 1) = 0)
       AND (demoted_roots IS NULL OR array_length(demoted_roots, 1) = 0) THEN
        RETURN NULL;
    END IF;

    -- One candidate set, one schedule_eligible call: the direct
    -- edge-dependents of the upgraded rows UNION the rollup-having root
    -- of each upgraded descendant (root_id IS NOT NULL skips the roots'
    -- own rows). schedule_eligible's gates decide what actually pends:
    -- dependents whose upstreams are all caught up, and the root only
    -- once its descendant gate clears (see the block comment above).
    IF upgraded IS NOT NULL AND array_length(upgraded, 1) > 0 THEN
        SELECT array_agg(DISTINCT c.id)
          INTO candidates
          FROM (
              SELECT d.dependent_id AS id
                FROM resource_deps d
               WHERE d.dependency_id = ANY(upgraded)
              UNION
              SELECT n.root_id
                FROM new_rows n
               WHERE n.id = ANY(upgraded)
                 AND n.root_id IS NOT NULL
          ) c;

        -- The rollup-having roots of THIS batch's upgraded descendants — the
        -- only rows that can suffer the straddle (a root waiting on its subtree).
        SELECT array_agg(DISTINCT n.root_id)
          INTO upgraded_roots
          FROM new_rows n
         WHERE n.id = ANY(upgraded)
           AND n.root_id IS NOT NULL;

        IF candidates IS NOT NULL AND array_length(candidates, 1) > 0 THEN
            PERFORM schedule_eligible(candidates);
        END IF;

        -- DEFERRED RECHECK (straddle close-out): schedule_eligible above may have
        -- SKIPPED a root because THIS batch's snapshot still saw a sibling
        -- descendant lagging (the other converging batch's settle isn't visible
        -- here yet). Such a root now has generation > synced_gen and NO work_queue
        -- reconcile row — enqueue it for a fresh-snapshot recheck by the drainer
        -- (drain_rollup_rechecks), committed atomically with this batch so it only
        -- arms if we commit. A root that schedule_eligible DID pend (has a
        -- work_queue row) is filtered out → no needless recheck. Cost is one
        -- indexed probe over the tiny upgraded_roots set; empty on the 1M healthy
        -- run (no roots upgrade). The NOT EXISTS against work_queue uses the
        -- pending claim index. ON CONFLICT keeps a re-arm idempotent. A direct
        -- pg_notify('rollup_recheck') wakes the drainer (per-statement, rare —
        -- ungated like work_ready, for the same lost-wake reason).
        IF upgraded_roots IS NOT NULL AND array_length(upgraded_roots, 1) > 0 THEN
            WITH armed AS (
                INSERT INTO rollup_recheck (resource_id, shard_id)
                SELECT r.id, r.shard_id
                  FROM resources r
                 WHERE r.id = ANY(upgraded_roots)
                   AND r.owner_id IS NULL                  -- roots only
                   AND r.generation > r.synced_gen         -- still needs its rollup
                   AND r.deletion_requested_at IS NULL
                   AND r.frozen_until IS NULL
                   AND NOT EXISTS (                        -- schedule_eligible did NOT pend it
                       SELECT 1 FROM work_queue q
                        WHERE q.resource_id = r.id
                          AND q.task_type = 'reconcile'::task_type
                          AND q.shard_id = r.shard_id
                   )
                ORDER BY r.id
                ON CONFLICT (resource_id) DO NOTHING
                RETURNING 1
            )
            SELECT count(*) INTO n_armed FROM armed;
            IF n_armed > 0 THEN
                PERFORM pg_notify('rollup_recheck', '');
            END IF;

            -- REACTIVE RE-PROMOTION (the symmetric twin of REACTIVE DEMOTION
            -- below): a descendant that just HEALED must let its composite root
            -- re-fold the recovered subtree health back into its own health_ok
            -- NOW — not on the hourly drift resync. The upgrade cascade sends the
            -- root through schedule_eligible (in `candidates`), but that gate
            -- requires generation > synced_gen; a root that already SETTLED
            -- Degraded (generation = synced_gen, e.g. it rolled up while this
            -- child was still transiently failed) fails that gate and is skipped,
            -- so its rollup would otherwise latch a stale Degraded snapshot until
            -- requeue_for_resync. The rollup_recheck arm just above covers only
            -- the still-LAGGING roots (generation > synced_gen); this covers the
            -- disjoint SETTLED case. Same direct enqueue-at-current-generation +
            -- work_ready NOTIFY as the demotion block: no bump, no compose
            -- (composed_gen >= generation), just a rollup re-run. ORDER BY id +
            -- ON CONFLICT gen-gate keep concurrent attempts idempotent and on the
            -- one global work_queue lock order. Empty on the 1M-healthy run (no
            -- root ever settles Degraded then heals), so zero added hot-path cost:
            -- one indexed probe over the tiny upgraded_roots set. The NOTIFY is
            -- gated on an ACTUAL insert (n_repended) — a converging root whose
            -- child just synced also lands in upgraded_roots but is NOT settled
            -- yet (generation > synced_gen), so it inserts nothing here and must
            -- not fire a spurious wake on every 1M converging batch.
            WITH repended AS (
                INSERT INTO work_queue (resource_id, task_type, kind, kind_version, generation, spec, provider_config, provider_bundle, manifest_version, shard_id)
                SELECT r.id, 'reconcile'::task_type, r.kind, r.kind_version, r.generation, r.spec, pc.spec, pc.data,
                       r.manifest_version, r.shard_id
                  FROM resources r
                  LEFT JOIN providerconfigs pc ON pc.id = r.provider_config_id  -- clone custom config (spec + bundle)
                 WHERE r.id = ANY(upgraded_roots)
                   AND r.owner_id IS NULL                  -- roots only
                   AND r.generation <= r.synced_gen        -- already SETTLED (schedule_eligible skipped it)
                   AND r.deletion_requested_at IS NULL
                   AND r.frozen_until IS NULL               -- orphaned OR quarantined = frozen
                 ORDER BY r.id
                ON CONFLICT (resource_id, task_type, shard_id) DO NOTHING
                RETURNING 1
            )
            SELECT count(*) INTO n_repended FROM repended;
            IF n_repended > 0 THEN
                PERFORM pg_notify('work_ready', '');
            END IF;
        END IF;

        -- LIFECYCLE REACTOR: emit a 'synced' delivery PER MATCHING BINDING for
        -- every just-upgraded resource. This is the durable, slot-free "wait
        -- until rolled up" edge — captured at the ONE line that already computes
        -- "newly-synced as a set" (`upgraded`), in the SAME tx that advanced
        -- synced_gen, so it commits atomically with the state flip.
        --
        -- The FAN-OUT is here, at emit: one row per (upgraded resource × matching
        -- enabled binding), so each binding's delivery is independently durable
        -- (ack/retry of one never touches another). label_match is applied here
        -- too (join resource_meta), so a row exists ONLY for a binding the
        -- resource matches — no claim-then-drop orphan downstream.
        --
        -- EXISTS short-circuit FIRST: when no 'synced' binding is registered this
        -- whole block is one indexed LIMIT-1 probe returning false → ZERO cost at
        -- 1M (the 1M-healthy steady state has no bindings → no join, no INSERT, no
        -- NOTIFY) — identical to HEAD on the hottest trigger. The DIRECT pg_notify
        -- (NEVER notify_gated): lifecycle_ready is per-STATEMENT (fires once per
        -- converging batch), and the gate's xact advisory lock held to commit
        -- would lose wakes inside the seconds-long drain tx (the documented
        -- work_ready scar — see schedule_eligible). ON CONFLICT DO NOTHING makes a
        -- re-cross idempotent per binding.
        IF EXISTS (SELECT 1 FROM reactor_bindings WHERE enabled AND transition = 'synced') THEN
            INSERT INTO lifecycle_outbox (resource_id, kind, transition, generation, binding_name, shard_id)
            SELECT n.id, n.kind, 'synced', n.generation, b.name, shard_of(n.id)
              FROM new_rows n
              JOIN reactor_bindings b
                ON b.enabled AND b.transition = 'synced' AND b.watch_kind = n.kind
                -- watch_kind_version scopes the binding to ONE version of the watched
                -- kind (NULL = all versions); AND it against the transitioning row's
                -- kind_version at emit so a row exists only for a matching binding.
               AND (b.watch_kind_version IS NULL OR b.watch_kind_version = n.kind_version)
              LEFT JOIN resource_meta m ON m.id = n.id
             WHERE n.id = ANY(upgraded)
               AND (b.label_match = '{}'::jsonb OR m.labels @> b.label_match)
            ON CONFLICT DO NOTHING;
            PERFORM pg_notify('lifecycle_ready', '');
        END IF;
    END IF;

    -- Re-pend demoted roots at their CURRENT generation (no bump, no
    -- compose — composed_gen >= generation skips it): a synced composite
    -- has generation = synced_gen so schedule_eligible would correctly
    -- skip it, but we WANT it to re-run its rollup over the now-unhealthy
    -- subtree. Same enqueue shape as requeue_for_resync; the ON CONFLICT
    -- gen-gate makes concurrent attempts idempotent. The upgraded path above
    -- already wakes workers via schedule_eligible's pg_notify; a PURE-demotion
    -- batch (empty `upgraded`) inserts directly here, so wake workers with the
    -- same direct, commit-atomic work_ready NOTIFY (per-statement, storm-safe —
    -- see schedule_eligible for why work_ready is ungated).
    IF demoted_roots IS NOT NULL AND array_length(demoted_roots, 1) > 0 THEN
        INSERT INTO work_queue (resource_id, task_type, kind, kind_version, generation, spec, provider_config, provider_bundle, manifest_version, shard_id)
        SELECT r.id, 'reconcile'::task_type, r.kind, r.kind_version, r.generation, r.spec, pc.spec, pc.data,
               r.manifest_version, r.shard_id
          FROM resources r
          LEFT JOIN providerconfigs pc ON pc.id = r.provider_config_id  -- clone custom config (spec + bundle)
         WHERE r.id = ANY(demoted_roots)
           AND r.deletion_requested_at IS NULL
           -- A frozen root (orphaned / quarantined) is not re-pended by composite
           -- demotion either — same freeze as every other scheduler.
           AND r.frozen_until IS NULL          -- orphaned OR quarantined = frozen
         -- ORDER BY id so concurrent demotion batches acquire the work_queue
         -- conflict locks in the same order schedule_eligible uses — keeps the
         -- enqueue paths on one global lock order (no ON CONFLICT deadlock).
         ORDER BY r.id
        ON CONFLICT (resource_id, task_type, shard_id) DO NOTHING;
        PERFORM pg_notify('work_ready', '');

        -- LIFECYCLE REACTOR: emit 'degraded' (health flipped true→false) and
        -- 'failed' (newly hard-failed this batch) for the descendants that
        -- triggered the demotion, fanned out PER MATCHING BINDING (label_match
        -- applied here too). Same EXISTS short-circuit + DIRECT
        -- pg_notify('lifecycle_ready') + ON CONFLICT idempotency as the 'synced'
        -- emit. Once-per-failure-generation per binding: a resync-re-pend of an
        -- already-failed row produces the same (resource,'failed',gen,binding)
        -- tuple → no re-fire of a compensator. The binding's transition must
        -- equal the row's COMPUTED edge (failed vs degraded) — so the join keys
        -- on the same CASE. Scanned from new_rows (the actual transitioning
        -- descendants), not demoted_roots — the binding watches the descendant kind.
        IF EXISTS (SELECT 1 FROM reactor_bindings WHERE enabled AND transition IN ('degraded', 'failed')) THEN
            INSERT INTO lifecycle_outbox (resource_id, kind, transition, generation, binding_name, shard_id)
            SELECT n.id, n.kind,
                   CASE WHEN n.failure_gen = n.generation THEN 'failed' ELSE 'degraded' END,
                   n.generation, b.name, shard_of(n.id)
              FROM new_rows n JOIN old_rows o ON o.id = n.id
              JOIN reactor_bindings b
                ON b.enabled AND b.watch_kind = n.kind
               AND b.transition = CASE WHEN n.failure_gen = n.generation THEN 'failed' ELSE 'degraded' END
                -- watch_kind_version scopes to one version of the watched kind (NULL = all).
               AND (b.watch_kind_version IS NULL OR b.watch_kind_version = n.kind_version)
              LEFT JOIN resource_meta m ON m.id = n.id
             WHERE ( (NOT n.health_ok AND o.health_ok)
                  OR (n.failure_gen = n.generation AND o.failure_gen IS DISTINCT FROM o.generation) )
               AND (b.label_match = '{}'::jsonb OR m.labels @> b.label_match)
            ON CONFLICT DO NOTHING;
            PERFORM pg_notify('lifecycle_ready', '');
        END IF;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER cascade_ready_change
    AFTER UPDATE ON resources
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION cascade_on_ready_change();

-- requeue_failed_and_pending: time-driven catch-up. Picks up rows
-- whose reconcile failed and the retry window has elapsed, plus
-- bootstrap rows that haven't been scheduled yet.
--
-- Lock-ordering note: deliberately NO FOR UPDATE on the resources
-- scan. Holding resources locks here while schedule_eligible writes
-- work_queue would deadlock with the drainer's (work_queue, resources)
-- lock order. Concurrent reapers picking the same candidate are
-- harmless: schedule_eligible's ON CONFLICT gen-gate makes the second
-- INSERT a no-op when the first already wrote at the current generation.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION requeue_failed_and_pending(retry_seconds INTEGER, max_rows INTEGER, shards SMALLINT[])
RETURNS INTEGER AS $$
DECLARE
    candidate_ids UUID[];
    n INTEGER := 0;
BEGIN
    SELECT array_agg(id ORDER BY id) INTO candidate_ids
    FROM (
        SELECT r.id
        FROM resources r
        WHERE r.shard_id = ANY(shards)
          AND r.synced_gen < r.generation
          AND r.deletion_requested_at IS NULL
          -- FROZEN states never re-pend (see schedule_eligible): an orphaned
          -- (pending-teardown) or quarantined (operator-set-aside) row stays put.
          AND r.frozen_until IS NULL          -- orphaned OR quarantined = frozen
          -- Terminal-failure stop-gate (see schedule_eligible): don't
          -- requeue a row whose failure is terminal for the live spec.
          AND NOT (r.failure_terminal AND r.failure_gen = r.generation)
          AND r.updated_at < now() - (retry_seconds || ' seconds')::interval
          AND NOT EXISTS (
              SELECT 1 FROM work_queue q
              WHERE q.resource_id = r.id
                AND q.shard_id = r.shard_id  -- prune to r's partition
                AND q.task_type = 'reconcile'::task_type
          )
          AND NOT EXISTS (
              SELECT 1 FROM resource_deps d
              JOIN resources p ON p.id = d.dependency_id
              WHERE d.dependent_id = r.id
                AND p.synced_gen < p.generation
          )
          -- Descendant gate (same as schedule_eligible): wait only while a
          -- descendant is still PROGRESSING; a failed descendant
          -- (failure_gen = generation) doesn't block the root. Scoped to
          -- roots — see schedule_eligible for the rationale.
          AND (
              r.owner_id IS NOT NULL
              OR NOT EXISTS (
                  SELECT 1 FROM resources d
                  WHERE d.root_id = r.id
                    AND d.synced_gen < d.generation
                    AND d.failure_gen <> d.generation   -- failed children don't block
                    AND d.deletion_requested_at IS NULL
                    AND d.frozen_until IS NULL           -- frozen (orphaned/quarantined) children don't block the rollup
              )
          )
        ORDER BY r.id
        LIMIT max_rows
    ) sub;

    IF candidate_ids IS NOT NULL AND array_length(candidate_ids, 1) > 0 THEN
        n := schedule_eligible(candidate_ids);
    END IF;
    RETURN n;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- requeue_for_resync: the drift-detection engine (Crossplane poll-loop
-- parity). Re-pends SETTLED resources of one kind so their provider
-- re-observes live health, WITHOUT bumping generation. This is what lets
-- a Ready=True resource flip to Ready=False hours after its spec last
-- changed: a leaf worker re-probes and reports health; a composite re-
-- runs its rollup and aggregates descendant is_ready into its own
-- health_ok.
--
-- Opt-in per kind: the Go control plane calls this once per tick only
-- for kinds whose Capabilities.ResyncInterval > 0. A deployment with no
-- resync-enabled kind never calls it, every row's last_reconciled_at
-- stays NULL, idx_resources_resync stays empty — zero cost. The 1M
-- stress kinds set ResyncInterval=0 → this function is never invoked.
--
-- Why not schedule_eligible: that gate requires generation > synced_gen
-- (a pending spec change). A settled resource has generation = synced_gen
-- so schedule_eligible would correctly skip it. Resync deliberately
-- enqueues at the CURRENT generation (no bump). We stamp
-- last_reconciled_at = now() at enqueue so the row isn't re-picked until a
-- full interval elapses, and so the partial index orders the due rows.
--
-- p_recompose (per-kind, from Capabilities.ResyncRecomposes): when FALSE
-- (the default for leaf/worker kinds and large composites) the composer is
-- skipped on the re-pend (composed_gen >= generation still holds) — only a
-- cheap re-observe + rollup. When TRUE we also knock composed_gen below
-- generation so the reconcile RE-RUNS the composer, re-asserting every
-- composed child's desired spec (drift-correcting children hand-edited away
-- from the desired set). A successful compose re-stamps
-- composed_gen = generation, so it self-settles after one pass. Opt-in
-- because a full re-diff of a 1M-child composite every tick is expensive;
-- only kinds with small fan-out should enable it.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION requeue_for_resync(
    p_kind TEXT, resync_seconds INTEGER, max_rows INTEGER, shards SMALLINT[],
    p_recompose BOOLEAN
) RETURNS INTEGER AS $$
DECLARE
    n INTEGER := 0;
BEGIN
    WITH due AS (
        SELECT r.id
        FROM resources r
        WHERE r.kind = p_kind
          AND r.shard_id = ANY(shards)
          AND r.deletion_requested_at IS NULL
          -- FROZEN states are NOT drift-resynced on the interval either: an
          -- orphaned (pending-teardown) row that is still synced/healthy, or a
          -- quarantined (set-aside) row, must stay put — the resyncer must never
          -- re-pend a resource an operator or the prune deliberately froze.
          AND r.frozen_until IS NULL          -- orphaned OR quarantined = frozen
          -- Settled on the synced axis: the regular reconcile/reaper
          -- path owns un-synced rows; resync only re-probes done work.
          AND r.synced_gen >= r.generation
          -- Due only after a FULL interval has elapsed since the row was
          -- last reconciled. A never-yet-resynced row (last_reconciled_at
          -- IS NULL) uses updated_at as the baseline — i.e. the interval
          -- clock starts when the row last settled, NOT "due immediately".
          -- Without this, a freshly composed root (last_reconciled_at NULL)
          -- matched on the very first sweep and — with p_recompose — forced
          -- a full recompose ~one sweep after settling (the redundant 2nd
          -- compose), instead of waiting the configured interval.
          AND COALESCE(r.last_reconciled_at, r.updated_at)
              < now() - (resync_seconds || ' seconds')::interval
          AND NOT EXISTS (
              SELECT 1 FROM work_queue q
              WHERE q.resource_id = r.id
                AND q.shard_id = r.shard_id  -- prune to r's partition
                AND q.task_type = 'reconcile'::task_type
          )
        ORDER BY r.last_reconciled_at NULLS FIRST, r.id
        LIMIT max_rows
    ), locked AS (
        -- DETERMINISTIC LOCK ORDERING. `due` picks the rows to re-probe in
        -- resync order (oldest-reconciled first) for the LIMIT, but the
        -- stamped UPDATE below writes resources rows that drainers/composers
        -- also write. Re-lock the chosen ids in ascending-id order — the same
        -- global order every other resources writer uses — so a resync sweep
        -- overlapping a drainer on a settled row degrades to a wait, never a
        -- deadlock. Tiny set (LIMIT-bounded), so the extra lock pass is cheap.
        SELECT r.id, r.kind, r.kind_version, r.generation, r.spec, r.provider_config_id, r.manifest_version
        FROM resources r
        WHERE r.id IN (SELECT id FROM due)
        ORDER BY r.id
        -- FOR NO KEY UPDATE: stamps only last_reconciled_at/composed_gen (non-
        -- key), so it matches the UPDATE's own lock and won't block the FOR KEY
        -- SHARE of concurrent INSERTs referencing these resources rows.
        FOR NO KEY UPDATE
    ), stamped AS (
        UPDATE resources r
           -- When p_recompose, knock composed_gen below generation so the
           -- resync-enqueued reconcile RE-RUNS the composer (the worker's
           -- skip-gate is composed_gen >= generation): re-asserts every
           -- composed child's desired spec — periodic drift-correction of
           -- children hand-edited away from the desired set — WITHOUT a
           -- generation bump. A successful compose re-stamps
           -- composed_gen = generation, so it self-settles after one pass.
           -- When FALSE, composed_gen is left untouched: cheap re-observe
           -- only (the original resync behavior).
           SET last_reconciled_at = now(),
               composed_gen = CASE WHEN p_recompose THEN 0 ELSE r.composed_gen END
          FROM locked
         WHERE r.id = locked.id
        RETURNING locked.id, locked.kind, locked.kind_version, locked.generation, locked.spec, locked.provider_config_id, locked.manifest_version
    ), ins AS (
        INSERT INTO work_queue (resource_id, task_type, kind, kind_version, generation, spec, provider_config, provider_bundle, manifest_version, shard_id)
        SELECT s.id, 'reconcile'::task_type, s.kind, s.kind_version, s.generation, s.spec, pc.spec, pc.data,
               s.manifest_version, shard_of(s.id)
        FROM stamped s
        LEFT JOIN providerconfigs pc ON pc.id = s.provider_config_id  -- clone custom config (spec + bundle)
        ORDER BY s.id
        ON CONFLICT (resource_id, task_type, shard_id) DO NOTHING
        RETURNING resource_id
    )
    SELECT count(*) INTO n FROM ins;
    -- Wake workers for the resync re-pends via the direct work_ready NOTIFY
    -- (per-batch, storm-safe, commit-atomic — see schedule_eligible for why
    -- work_ready is ungated); fleet-wide so it reaches worker pods cross-process.
    IF n > 0 THEN
        PERFORM pg_notify('work_ready', '');
    END IF;
    RETURN n;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- gc_spec_history: bounds the spec_versions root-history log to the keep_n
-- newest versions per root. Modeled on requeue_for_resync (shard-scoped,
-- LIMIT-bounded, off the hot path), driven by the SpecGC sweeper.
--
-- spec_versions holds ONLY root revisions (children never write there), and
-- the LIVE spec lives separately inline on resources.spec — so this is a
-- plain "keep the newest keep_n per resource_id, drop older" trim. There's
-- no live-body guard needed (the live spec isn't in this table) and no child
-- branch. The version a checkout currently points the live spec at may be an
-- older generation, but it still exists in the log as long as it's within
-- keep_n of the newest authored; if a very old version was checked out and
-- then keep_n newer ones were authored, the old one ages out of the log
-- (the live resources.spec still has the bytes — checkout already copied
-- them — so the resource is unaffected; only the log entry is trimmed).
--
-- shards passed as = ANY(shards): spec_versions is unpartitioned, and we
-- scope via a join to resources by PK. FOR UPDATE SKIP LOCKED so two pods
-- don't fight; the DELETE is idempotent.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION gc_spec_history(keep_n INTEGER, max_rows INTEGER, shards SMALLINT[])
RETURNS INTEGER AS $$
DECLARE
    n INTEGER := 0;
BEGIN
    WITH candidate AS (
        SELECT s.resource_id, s.generation
        FROM spec_versions s
        JOIN resources r ON r.id = s.resource_id
        WHERE r.shard_id = ANY(shards)
          AND (s.resource_id, s.generation) IN (
              SELECT resource_id, generation FROM (
                  SELECT s2.resource_id, s2.generation,
                         row_number() OVER (PARTITION BY s2.resource_id
                                            ORDER BY s2.generation DESC) AS rn
                  FROM spec_versions s2
                  WHERE s2.resource_id = s.resource_id
              ) ranked
              WHERE rn > keep_n
          )
        ORDER BY s.resource_id, s.generation
        LIMIT max_rows
        FOR UPDATE SKIP LOCKED
    ), del AS (
        DELETE FROM spec_versions s
         USING candidate c
         WHERE s.resource_id = c.resource_id AND s.generation = c.generation
        RETURNING s.resource_id
    )
    SELECT count(*) INTO n FROM del;
    RETURN n;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- gc_stale_cluster_members: hard-delete cluster_members rows whose last
-- heartbeat is older than ttl_seconds — the delayed garbage collector for the
-- running-fleet registry, and the FALLBACK to a clean shutdown's own
-- deregister-DELETE. Driven by the ControlPlane's ClusterMemberGC sweeper on a
-- coarse (~60s) cadence with a ~5m TTL, so a member that stopped beating
-- (crashed, or whose deregister timed out) is shown NotReady in the UI for a
-- while (the API derives that from heartbeat age) and only then reclaimed here.
-- RETURNS the number of rows deleted, mirroring gc_spec_history's shape so
-- the Go sweeper can log a count. Uses now() (DB clock), consistent with the
-- now() the UPSERT stamps last_heartbeat with. Single-statement DELETE over a
-- dozens-of-rows table — no shard scoping, no lock-ordering concern (it
-- shares no table with the hot paths).
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION gc_stale_cluster_members(ttl_seconds INTEGER)
RETURNS INTEGER AS $$
DECLARE
    n INTEGER := 0;
BEGIN
    WITH del AS (
        DELETE FROM cluster_members
         WHERE last_heartbeat < now() - make_interval(secs => ttl_seconds)
        RETURNING member_id
    )
    SELECT count(*) INTO n FROM del;
    RETURN n;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- assign_member_shards: the COMPUTE half of DYNAMIC SHARDING — given a member
-- id, return the contiguous shard span [lo, hi+1) it owns RIGHT NOW, derived
-- purely from the live cluster_members view (no leader, no advisory lock). Every
-- node calls this for ITSELF (the Resharder tick), so the assignment is computed
-- independently and identically on each node from the shared registry.
--
-- HOW: rank the calling member among the LIVE members sharing its exact `role`
-- (live = beat within p_liveness_secs), ordered deterministically by member_id,
-- then range-partition [0, p_total) by (rank, count) with the SAME integer math
-- as shardutil.ShardsForPod (start = rank*total/count, end = (rank+1)*total/count).
-- Grouping by role means each role tiles [0,total) INDEPENDENTLY — `control`
-- pods tile for the drainer/reaper, `worker` (or `worker-<kind>`) pods tile for
-- claims, `all` pods tile for both. The divisor is the live member COUNT, so the
-- tiling reshapes itself on every join/leave with no hand-set pod count.
--
-- Returns a half-open int4range [lo, hi+1) (Postgres-canonical, same encoding as
-- cluster_members.shards), or NULL when the member owns nothing: not registered
-- yet, its own beat aged out (excluded from the live set → yields to peers), or
-- more live members than shards (extra members idle, like ShardsForPod). The
-- ranges are CONTIGUOUS per member, so store.shardBounds' BETWEEN pruning holds;
-- transient cross-node disagreement during a join/leave just overlaps ranges,
-- which is safe (work_queue claims are FOR UPDATE SKIP LOCKED) and reconciles on
-- the next tick. STABLE: a read-only snapshot of the registry within the call.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION assign_member_shards(
    p_member_id     TEXT,
    p_total         INT,
    p_liveness_secs INT
) RETURNS int4range AS $$
DECLARE
    v_role  TEXT;
    v_rank  INT;
    v_count INT;
    v_start INT;
    v_end   INT;
BEGIN
    SELECT role INTO v_role FROM cluster_members WHERE member_id = p_member_id;
    IF v_role IS NULL THEN
        RETURN NULL; -- not registered yet (race with the first heartbeat)
    END IF;

    SELECT rnk, cnt INTO v_rank, v_count
    FROM (
        SELECT member_id,
               (row_number() OVER (ORDER BY member_id))::int - 1 AS rnk,
               (count(*)     OVER ())::int                       AS cnt
        FROM cluster_members
        WHERE role = v_role
          AND last_heartbeat >= now() - make_interval(secs => p_liveness_secs)
    ) ranked
    WHERE member_id = p_member_id;

    IF v_rank IS NULL THEN
        RETURN NULL; -- caller's own heartbeat aged out → owns nothing this tick
    END IF;

    -- Identical to shardutil.ShardsForPod's integer range-partition.
    v_start := v_rank * p_total / v_count;
    v_end   := (v_rank + 1) * p_total / v_count;
    IF v_end > p_total THEN
        v_end := p_total;
    END IF;
    IF v_start >= v_end THEN
        RETURN NULL; -- more live members than shards → this member idles
    END IF;

    RETURN int4range(v_start, v_end); -- half-open [lo, hi+1)
END;
$$ LANGUAGE plpgsql STABLE;
-- +goose StatementEnd

-- rollback_to_spec: CHECKOUT a historic root revision as the live spec.
-- Roll-back AND roll-forward — "make revision N live" for any authored
-- generation N in the root's spec_versions log, navigable in any order
-- (3 → 5 → 1 → 5 …) any number of times.
--
-- CHECKOUT, not revert: it copies the chosen version's body INTO
-- resources.spec in place and bumps generation (explicitly — there is no
-- bump_generation trigger), so the pipeline re-reconciles to that body. We
-- do NOT append a new spec_versions row for a checkout — navigation never
-- grows the set; only a genuine Apply (UpsertResource) authors a version.
-- The fixed set of versions is a stable thing you move a cursor over.
--
-- p_target_gen identifies the version by its authored generation (the PK
-- with resource_id). A no-op checkout to the already-live body is gated out
-- (IS DISTINCT FROM) so it neither bumps nor reschedules. Returns the
-- resource's generation after checkout, or -1 if (resource_id, generation)
-- doesn't exist.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION rollback_to_spec(p_id UUID, p_target_gen BIGINT, p_actor TEXT)
RETURNS BIGINT AS $$
DECLARE
    target_body JSONB;
    new_gen     BIGINT;
BEGIN
    SELECT spec INTO target_body
      FROM spec_versions
     WHERE resource_id = p_id AND generation = p_target_gen;
    IF NOT FOUND THEN
        RETURN -1;
    END IF;

    -- Copy the historic body into the live inline spec and bump generation
    -- explicitly (no bump_generation trigger anymore). The IS DISTINCT FROM
    -- gate makes a checkout to the already-live body a true no-op (no bump,
    -- no row write) — matching the prior trigger behaviour.
    UPDATE resources
       SET spec = target_body,
           generation = generation + 1,
           updated_at = now()
     WHERE id = p_id AND spec IS DISTINCT FROM target_body
    RETURNING generation INTO new_gen;

    -- If the gate skipped (already live), new_gen is unset by RETURNING; read
    -- the current generation so the caller gets a real value (not 0 → 404).
    IF new_gen IS NULL THEN
        SELECT generation INTO new_gen FROM resources WHERE id = p_id;
        RETURN new_gen;  -- no-op checkout: nothing to schedule
    END IF;

    INSERT INTO resource_events (resource_id, type, actor, reason, message)
    VALUES (p_id, 'SpecRolledBack', p_actor, 'Rollback',
            'checked out spec revision (authored gen ' || p_target_gen || ')');

    PERFORM schedule_eligible(ARRAY[p_id]);
    RETURN new_gen;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- request_resource_deletion: API DELETE handler invokes this. Sets
-- deletion_requested_at, seeds finalizers from p_finalizer (caller-
-- provided; the Go side knows the registered FinalizerName for the
-- kind). If empty, hard-deletes the row immediately. Returns true if
-- the row existed.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION request_resource_deletion(p_id UUID, p_finalizer TEXT, p_actor TEXT) RETURNS BOOLEAN AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM resources WHERE id = p_id) THEN
        RETURN false;
    END IF;
    -- Delete the WHOLE tree under/downstream of p_id, in dependency order (kro-style).
    -- request_resource_deletion no longer removes the row itself; it MARKS p_id and
    -- every transitively OWNED descendant AND every transitive DEPENDENT
    -- (resource_deps) for deletion + seeds each node's finalizer (from kind_config),
    -- then kicks sweep_deletable ONCE — a SEPARATE statement, so the marks are visible
    -- — to dispatch the currently-unblocked leaves' teardown tasks immediately (no
    -- wait for the first reaper tick). sweep_deletable then drives the rest: it
    -- dispatches each node's teardown only once it has no owned child and no dependent
    -- still being deleted (ordered finalizer execution — a dependency's teardown never
    -- runs while a dependent exists), and hard-deletes a node once its finalizers +
    -- children + dependents are all gone. So a parent always waits for its
    -- children/dependents, and owner_id ON DELETE CASCADE only ever fires on an
    -- already-childless leaf (never skipping a finalizer). p_finalizer is ignored
    -- (each node's finalizer comes from its kind_config); kept for the caller's ABI.
    PERFORM cascade_mark_for_deletion(p_id, p_actor);
    PERFORM sweep_deletable(100000, 0::smallint, 255::smallint);
    RETURN true;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- cascade_mark_for_deletion MARKS a resource and its entire teardown tree for
-- deletion: the seed p_id, every transitively OWNED descendant (owner_id chain),
-- and every transitive DEPENDENT (resource_deps.dependency_id → dependent_id). Each
-- marked node gets deletion_requested_at + its kind's finalizer (from kind_config).
-- It does NOT enqueue any teardown task and does NOT remove any row — that is
-- sweep_deletable's job, which runs AFTER these marks are committed so its
-- dependency/child checks see the real state: it dispatches a node's delete task
-- only once the node is UNBLOCKED (no live dependent, no owned child), enforcing
-- ordered finalizer execution (a dependency's teardown never runs while a dependent
-- still exists), and hard-deletes a node once its finalizers + children + dependents
-- are all gone. Keeping mark and dispatch in separate statements is load-bearing:
-- inside ONE data-modifying CTE the enqueue arm would see the PRE-mark snapshot and
-- treat every node as an unblocked leaf, dispatching the whole tree at once.
--
-- Delete OVERRIDES a set-aside: a frozen node (quarantined 'infinity' or orphaned
-- finite frozen_until) in the tree IS marked and has its frozen_until cleared, so it
-- tears down with the rest instead of being left behind (a leak) or letting a
-- dependency be removed out from under it (a dangling reference).
--
-- Cycle-safe (UNION dedups; a revisited node adds no row → recursion terminates)
-- and bounded. Idempotent: the mark UPDATE skips rows already draining
-- (deletion_requested_at set). Idle cost is ~0: it only runs on a delete request,
-- and the recursive walk uses idx_resources_owner + idx_resource_deps_dependency.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION cascade_mark_for_deletion(p_id UUID, p_actor TEXT) RETURNS VOID AS $$
DECLARE
    _n BIGINT;  -- throwaway sink so the data-modifying CTE is a TOP-LEVEL statement
BEGIN
    WITH RECURSIVE tree AS (
        -- seed (non-recursive term)
        SELECT p_id AS id
        UNION
        -- recursive term (ONE SELECT — Postgres allows a single self-referencing
        -- term): from each frontier node reach BOTH its owned children (composition,
        -- owner_id) AND its dependents (resource_deps.dependency_id → dependent_id),
        -- LATERAL-unioned. UNION dedups, so cycles terminate.
        SELECT e.id
          FROM tree t
          JOIN LATERAL (
              SELECT c.id FROM resources c WHERE c.owner_id = t.id
              UNION ALL
              SELECT d.dependent_id AS id FROM resource_deps d WHERE d.dependency_id = t.id
          ) e ON true
    ),
    -- Resolve each tree node's finalizer. Mark every node not already draining —
    -- INCLUDING frozen (quarantined / orphaned) ones: an explicit delete is the
    -- strongest operator intent and OVERRIDES the set-aside (see the marked CTE,
    -- which also clears frozen_until). If we skipped frozen nodes here, a quarantined
    -- node in the tree would be left un-marked → leaked, AND a dependency it points
    -- at could be hard-deleted while the quarantined node still references it (the
    -- sweep's "dependent still deletion_requested_at" gate would read false for an
    -- unmarked frozen dependent) — a dangling-reference bug. So delete wins over freeze.
    picked AS (
        SELECT r.id, kc.finalizer_name AS finalizer
          FROM tree t
          JOIN resources r ON r.id = t.id
          LEFT JOIN kind_config kc ON kc.kind = r.kind AND kc.kind_version = r.kind_version
         WHERE r.deletion_requested_at IS NULL
    ),
    marked AS (
        UPDATE resources r
           SET deletion_requested_at = now(),
               -- Delete OVERRIDES quarantine/orphan: clear frozen_until so the node
               -- isn't simultaneously frozen and Deleting, and so every gate keyed on
               -- `frozen_until IS NULL` (schedulers, the reaper's orphan sweep) treats
               -- it as a normal teardown. phase='Deleting' already outranks frozen, and
               -- the sweep now correctly sees it as a live (deletion_requested_at) node.
               frozen_until = NULL,
               -- finalizer-bearing kinds get their finalizer seeded; 0-finalizer
               -- kinds stay finalizers='{}' and are hard-deleted by the gate.
               finalizers = CASE WHEN p.finalizer IS NOT NULL AND p.finalizer <> ''
                                 THEN ARRAY[p.finalizer] ELSE r.finalizers END,
               updated_at = now()
          FROM picked p
         WHERE r.id = p.id
        RETURNING r.id
    ),
    ev AS (
        INSERT INTO resource_events (resource_id, type, actor, reason, message)
        SELECT m.id, 'DeleteRequested', p_actor, 'Cascade',
               'delete requested by ' || COALESCE(p_actor, 'unknown')
        FROM marked m
        RETURNING resource_id
    )
    SELECT count(*) INTO _n FROM (
        SELECT id FROM marked
        UNION ALL SELECT resource_id FROM ev
    ) forced;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- sweep_deletable is the LEVEL-TRIGGERED engine that drives a marked teardown tree
-- bottom-up, in dependency order. A node is UNBLOCKED once it has no remaining OWNED
-- child and no remaining DEPENDENT still being deleted. For each unblocked marked
-- node this sweep does one of two things:
--   * finalizers empty (teardown done, or the kind has none) → HARD-DELETE the row;
--   * finalizer still present and NO delete task queued yet → ENQUEUE its delete task
--     so its teardown runs NOW — and only now, in order (a dependency's finalizer,
--     e.g. the cloud VPC delete, never runs while a dependent subnet still exists).
-- As each node's teardown completes (drain strips its finalizer) and its row is
-- removed, the next layer up becomes unblocked and this sweep advances it. So the
-- tree tears down strictly dependents-before-dependencies.
--
-- STRICT: a node whose own teardown never finishes keeps its finalizer, stays
-- blocked, and blocks everything above it — no dead-letter unblock. Zero-cost idle
-- gate (idx_resources_deleting); shard-range pruning; each action LIMIT-bounded;
-- loops WITHIN a tick until stable so a ready subtree collapses at once. owner_id ON
-- DELETE CASCADE only ever fires on an already-childless leaf (never skips a finalizer).
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION sweep_deletable(max_rows INTEGER, shard_lo SMALLINT, shard_hi SMALLINT) RETURNS INTEGER AS $$
DECLARE
    total INTEGER := 0;
    n     INTEGER;
    queued INTEGER;
BEGIN
    -- Idle gate: any deletion-requested row in range at all? (idx_resources_deleting).
    -- Nothing tearing down → skip the whole sweep.
    IF NOT EXISTS (
        SELECT 1 FROM resources
         WHERE deletion_requested_at IS NOT NULL
           AND shard_id BETWEEN shard_lo AND shard_hi
         LIMIT 1
    ) THEN
        RETURN 0;
    END IF;

    LOOP
        -- (1) ENQUEUE teardown for unblocked finalizer nodes that have no delete task
        -- yet — the "dispatch the next layer once it's unblocked" step that enforces
        -- ordered finalizer execution.
        WITH ready AS (
            SELECT r.id, r.kind, r.kind_version, r.generation, r.spec,
                   (SELECT pc.spec FROM providerconfigs pc WHERE pc.id = r.provider_config_id) AS pcfg,
                   (SELECT pc.data FROM providerconfigs pc WHERE pc.id = r.provider_config_id) AS pbundle,
                   r.manifest_version, r.shard_id
              FROM resources r
             WHERE r.deletion_requested_at IS NOT NULL
               AND array_length(r.finalizers, 1) IS NOT NULL          -- has a finalizer
               AND r.shard_id BETWEEN shard_lo AND shard_hi
               AND NOT EXISTS (SELECT 1 FROM resources c WHERE c.owner_id = r.id)
               AND NOT EXISTS (
                   SELECT 1 FROM resource_deps d
                   JOIN resources dep ON dep.id = d.dependent_id
                   WHERE d.dependency_id = r.id AND dep.deletion_requested_at IS NOT NULL
               )
               AND NOT EXISTS (
                   SELECT 1 FROM work_queue w
                    WHERE w.resource_id = r.id AND w.task_type = 'delete'
                      AND w.shard_id = r.shard_id
               )
             LIMIT max_rows
        ), enq AS (
            -- Carry the resource's OWN spec on the teardown task: a real IaC provider
            -- (stdterraform) needs the module source + vars to `terraform destroy` — a
            -- NULL spec makes it fail "source required" and hot-loop forever, never
            -- stripping the finalizer. (The providerconfig/bundle ride along too, since
            -- the destroy also needs the state backend.)
            INSERT INTO work_queue (resource_id, task_type, kind, kind_version, generation, spec, provider_config, provider_bundle, manifest_version, shard_id)
            SELECT rd.id, 'delete'::task_type, rd.kind, rd.kind_version, rd.generation, rd.spec, rd.pcfg, rd.pbundle,
                   rd.manifest_version, rd.shard_id
            FROM ready rd
            ON CONFLICT (resource_id, task_type, shard_id) DO NOTHING
            RETURNING resource_id
        )
        SELECT count(*) INTO queued FROM enq;

        -- (2) HARD-DELETE unblocked, finalizer-empty nodes (teardown done, or none).
        WITH picked AS (
            SELECT r.id, r.shard_id FROM resources r
             WHERE r.deletion_requested_at IS NOT NULL
               AND (r.finalizers IS NULL OR array_length(r.finalizers, 1) IS NULL)
               AND r.shard_id BETWEEN shard_lo AND shard_hi
               AND NOT EXISTS (SELECT 1 FROM resources c WHERE c.owner_id = r.id)
               AND NOT EXISTS (
                   SELECT 1 FROM resource_deps d
                   JOIN resources dep ON dep.id = d.dependent_id
                   WHERE d.dependency_id = r.id AND dep.deletion_requested_at IS NOT NULL
               )
             LIMIT max_rows
             FOR UPDATE OF r SKIP LOCKED
        ), gone AS (
            DELETE FROM resources r USING picked
             WHERE r.id = picked.id AND r.shard_id = picked.shard_id
            RETURNING r.id
        )
        SELECT count(*) INTO n FROM gone;

        total := total + n;
        -- Stop when a pass neither removed nor newly-enqueued anything (steady state:
        -- the rest is waiting on in-flight teardowns), or we hit the batch bound.
        EXIT WHEN (n = 0 AND queued = 0) OR total >= max_rows;
    END LOOP;
    RETURN total;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- quarantine_resource: operator action that sets a FAILED/stuck resource ASIDE.
-- It stamps frozen_until = 'infinity' (→ phase='Quarantined'), which FREEZES the
-- row: every scheduler (schedule_eligible, cascade demotion,
-- requeue_failed_and_pending, requeue_for_resync) and both value-flow substitute
-- passes skip a row with frozen_until set, so it stops retrying and can't be
-- resurrected by a dependency's status change. The 'infinity' sentinel ALSO makes
-- the reaper's `frozen_until < now()` sweep never touch it (quarantine is a
-- permanent set-aside, not a pending teardown). It drops any pending reconcile
-- work_queue row (don't run the task we just set aside) — but NOT a delete task
-- (a quarantine must not interfere with an in-progress teardown). The row is NOT
-- deleted (it's still desired); it is simply parked, which ALSO unblocks its
-- root's rollup (frozen children are excluded from the descendant gate + unready
-- count). Quarantining an already-ORPHANED row (finite frozen_until) is allowed
-- and overrides it to 'infinity' (set-aside wins over pending teardown).
-- Returns true if the row existed, was not deleting, and was not already quarantined.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION quarantine_resource(p_id UUID, p_actor TEXT) RETURNS BOOLEAN AS $$
DECLARE
    r_shard SMALLINT;
BEGIN
    UPDATE resources
       SET frozen_until = 'infinity', updated_at = now()
     WHERE id = p_id
       AND deletion_requested_at IS NULL          -- don't quarantine a row mid-teardown
       AND frozen_until IS DISTINCT FROM 'infinity' -- idempotent: already quarantined → no-op
    RETURNING shard_id INTO r_shard;
    IF NOT FOUND THEN
        RETURN false;
    END IF;
    -- Drop the PENDING reconcile task: the row is set aside, so don't run it.
    -- `broker_id IS NULL` = unclaimed; an already-CLAIMED reconcile must NOT be
    -- deleted out from under its running worker — its fenced result (AppendOutbox
    -- checks the row still exists with this broker_id) would be silently discarded.
    -- Let the in-flight reconcile finish and land normally; the freeze stops the
    -- NEXT schedule. (Mirrors the established `broker_id IS NULL` = pending pattern.)
    DELETE FROM work_queue
     WHERE resource_id = p_id AND task_type = 'reconcile'::task_type AND shard_id = r_shard
       AND broker_id IS NULL;
    INSERT INTO resource_events (resource_id, type, actor, reason, message)
    VALUES (p_id, 'Quarantined', p_actor, 'SetAside',
            'quarantined by ' || COALESCE(p_actor, 'unknown'));
    RETURN true;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- unquarantine_resource: clears the quarantine, re-arming the resource. After
-- clearing frozen_until it calls schedule_eligible on the row so a still-
-- lagging resource (generation > synced_gen) re-pends immediately and a settled
-- one is left alone — the row rejoins normal scheduling. This is the ONLY way
-- out: quarantine is a HARD freeze. A spec edit (Apply) or a Resync while
-- quarantined persists the new spec / bumps generation but does NOT lift the
-- freeze and does NOT reschedule — the row reconciles to its latest spec only
-- once explicitly released here. Guarded on frozen_until = 'infinity' so it only
-- releases a QUARANTINED row, never an orphaned one (finite grace stays put; if
-- the row is still composer-dropped the next prune re-orphans it). Returns true
-- if the row existed and was quarantined.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION unquarantine_resource(p_id UUID, p_actor TEXT) RETURNS BOOLEAN AS $$
BEGIN
    UPDATE resources
       SET frozen_until = NULL, updated_at = now()
     WHERE id = p_id AND frozen_until = 'infinity';
    IF NOT FOUND THEN
        RETURN false;
    END IF;
    INSERT INTO resource_events (resource_id, type, actor, reason, message)
    VALUES (p_id, 'Unquarantined', p_actor, 'Released',
            'unquarantined by ' || COALESCE(p_actor, 'unknown'));
    -- Re-arm: schedule_eligible re-pends it iff it still needs work; a settled
    -- row is a no-op. This is the only place a cleared quarantine re-enters
    -- scheduling (the cascade won't have fed it while frozen).
    PERFORM schedule_eligible(ARRAY[p_id]);
    RETURN true;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- drain_outbox_batch: pop up to max_rows from work_outbox (scoped to the
-- worker's contiguous shard range [shard_lo, shard_hi]) and apply
-- transitions. Downstream scheduling is handled by the cascade_ready_change
-- trigger (inline, fires once per UPDATE statement when any row's
-- synced_gen catches up to generation).
--
-- shard_lo/shard_hi (a range), NOT a shards[] array: work_outbox is
-- RANGE-partitioned by shard_id and a bound range predicate gets runtime
-- partition pruning, whereas `shard_id = ANY($array)` scans every
-- partition. See the WorkQueueTakeBatch comment for the measurement.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION drain_outbox_batch(max_rows INTEGER, shard_lo SMALLINT, shard_hi SMALLINT)
RETURNS INTEGER AS $$
DECLARE
    n INTEGER;
    src_ids UUID[];
    src_payloads JSONB[];
BEGIN
    -- Session-scoped scratch table, created once per pooled backend and
    -- truncated (ON COMMIT DELETE ROWS) at the end of every drain tx.
    -- Creating it ON COMMIT DROP per call would, at 1M scale, mean ~20k
    -- catalog churns (pg_class/pg_attribute/pg_type insert+delete)
    -- contending the catalog locks across 12 concurrent drainer backends
    -- (~12ms/call). Reusing the table via ON COMMIT DELETE ROWS makes
    -- CREATE run ~once per backend, so the catalog churn disappears.
    IF to_regclass('pg_temp.drain_popped') IS NULL THEN
        CREATE TEMP TABLE drain_popped (
            work_id             UUID,
            resource_id         UUID,
            op_id               UUID,
            task_type           task_type,
            succeeded           BOOLEAN,
            payload             JSONB,
            error_message       TEXT,
            observed_generation BIGINT,
            advance_synced_gen   BOOLEAN,
            health_ok           BOOLEAN,
            conditions          JSONB,
            failed              BOOLEAN,
            terminal            BOOLEAN,
            finalizer_name      TEXT,
            shard_id            SMALLINT
        ) ON COMMIT DELETE ROWS;
    END IF;

    -- Pop directly from the locked inner scan (no second pass over
    -- work_outbox). The range predicate prunes the Append to the
    -- partition(s) holding [shard_lo, shard_hi]; capturing its columns
    -- straight into drain_popped avoids a by-work_id re-scan that would
    -- touch all 16 partitions. shard_id is carried so the two DELETEs
    -- below prune to the same partition(s).
    INSERT INTO drain_popped
    SELECT work_id, resource_id, op_id, task_type, succeeded,
           payload, error_message, observed_generation, advance_synced_gen,
           health_ok, conditions, failed, terminal, finalizer_name, shard_id
    FROM (
        SELECT work_id, resource_id, op_id, task_type, succeeded,
               payload, error_message, observed_generation, advance_synced_gen,
               health_ok, conditions, failed, terminal, finalizer_name, shard_id
        FROM work_outbox
        WHERE shard_id BETWEEN shard_lo AND shard_hi
        ORDER BY shard_id
        FOR UPDATE SKIP LOCKED
        LIMIT max_rows
    ) popped;

    SELECT count(*) INTO n FROM drain_popped;
    IF n = 0 THEN
        RETURN 0;
    END IF;

    -- Both deletes carry shard_id so the planner prunes to the worker's
    -- partition(s); work_queue is keyed (id, shard_id), work_outbox
    -- (work_id, shard_id), and drain_popped.shard_id matches both.
    DELETE FROM work_outbox o USING drain_popped p
      WHERE o.work_id = p.work_id AND o.shard_id = p.shard_id;
    -- Generation guard on the work_queue delete: an Apply that lands WHILE a
    -- reconcile is in flight re-points this same work_queue row to a higher
    -- generation (schedule_eligible's ON CONFLICT bumps generation + NULLs
    -- broker_id). A stale gen-N completion must NOT delete that re-pointed
    -- gen-(N+1) row, or the live reconcile is silently dropped until the
    -- reaper re-enqueues it ~RetryAfter later. So delete only when the queue
    -- row's generation still matches the completed one. (operate/delete tasks
    -- carry their own generation semantics and are not re-pointed this way,
    -- so they always delete.) observed_generation is already on drain_popped;
    -- one extra predicate on the partition-pruned delete — hot-path-safe.
    DELETE FROM work_queue q USING drain_popped p
      WHERE q.id = p.work_id AND q.shard_id = p.shard_id
        AND (p.task_type <> 'reconcile'::task_type
             OR q.generation = p.observed_generation);

    -- Operate task results: write resource_operations row only.
    UPDATE resource_operations o
       SET state = CASE WHEN p.succeeded THEN 'succeeded' ELSE 'failed' END,
           output = p.payload,
           error_message = p.error_message,
           completed_at = now()
      FROM drain_popped p
     WHERE p.task_type = 'operate'
       AND o.id = p.op_id
       -- Defense-in-depth vs a stale/duplicate operate result: only finalize an
       -- operation that is still 'running'. The AppendOutbox fence (broker_id
       -- ownership) already stops a reaped/reassigned zombie's result from ever
       -- reaching the outbox; this guard ensures that even if a stale row landed,
       -- it can't OVERWRITE a terminal ('succeeded'/'failed') op set by the
       -- authoritative attempt — first finalize wins, no stale-over-fresh output.
       AND o.state = 'running';

    -- Delete task results: on SUCCESS, strip the node's finalizer (idempotent —
    -- array_remove of an absent element is a no-op). The provider's teardown ran; the
    -- node still can't be removed until it also has no remaining children/dependents,
    -- which the gated hard-delete below enforces. A FAILED delete strips nothing and
    -- the reaper re-pends the still-present delete task (STRICT: a node that can't
    -- finish its teardown keeps blocking its parent — no dead-letter unblock).
    --
    -- DETERMINISTIC LOCK ORDERING (same rationale as the substitute + status passes
    -- below). This is the ONE resources-lock in this function that isn't ordered by
    -- the substitute/status passes: a delete-marked row clears frozen_until (see
    -- mark_for_deletion), so the substitute pass's `frozen_until IS NULL` gate does
    -- NOT exclude a delete-in-progress row — the SAME physical row can be this band's
    -- finalizer-strip target AND another band's cross-shard substitute-pass dependent.
    -- A bare UPDATE ... FROM has no ORDER BY and locks in arbitrary heap/join order,
    -- so two bands touching {X,Y} in opposite orders (this strip vs the other's
    -- id-ascending substitute pass) could deadlock. Pre-lock the target rows
    -- id-ascending here so EVERY resources writer in this function acquires its locks
    -- in one monotonic ascending-id order. FOR NO KEY UPDATE (not FOR UPDATE): this
    -- UPDATE changes no key column, matching the substitute/status passes and not
    -- blocking the FOR KEY SHARE that child/meta/condition INSERTs take. This runs
    -- ONLY for task_type='delete' rows in drain_popped — zero rows on an all-reconcile
    -- workload — so the added Sort/LockRows never touches the hot reconcile plan.
    WITH strip AS (
        SELECT r.id
          FROM resources r
          JOIN drain_popped p ON p.resource_id = r.id
         WHERE p.task_type = 'delete'
           AND p.succeeded = true
           AND p.finalizer_name IS NOT NULL
         ORDER BY r.id
         FOR NO KEY UPDATE OF r
    )
    UPDATE resources r
       SET finalizers = array_remove(r.finalizers, p.finalizer_name),
           updated_at = now()
      FROM drain_popped p
     WHERE p.task_type = 'delete'
       AND p.succeeded = true
       AND r.id = p.resource_id
       AND p.finalizer_name IS NOT NULL
       AND r.id IN (SELECT id FROM strip);

    -- Gated hard-delete — the fast-path removal right after a teardown drains (the
    -- reaper's sweep_deletable is the level-triggered backstop for nodes whose
    -- blockers clear without a fresh delete result). A node goes only when ALL THREE
    -- hold: (1) finalizers empty (teardown done, or none), (2) no remaining OWNED
    -- child, (3) no remaining DEPENDENT. So a parent always waits for its children +
    -- dependents; owner_id ON DELETE CASCADE then only ever fires on an
    -- already-childless leaf, never skipping a finalizer.
    --
    -- LOCK the candidate rows FOR UPDATE (SKIP LOCKED) BEFORE the blocker checks, and
    -- re-evaluate the checks in the same locked statement — mirroring sweep_deletable.
    -- Without the lock the NOT EXISTS(dependent) was a check-then-act TOCTOU under
    -- READ COMMITTED: a concurrent tx marking a NEW dependent (or the reaper sweep
    -- removing/adding a sibling) between the snapshot read and the DELETE could let a
    -- dependency be removed with a live dependent. FOR UPDATE serializes the two
    -- removal paths (drain vs sweep) and any concurrent marker on the same rows.
    -- ORDER BY r.id: SKIP LOCKED means this can never be a deadlock WAITER (it skips
    -- a contended row rather than blocking), so it can't itself close a cycle — but
    -- ordering its HELD set id-ascending keeps the whole function's resources-lock
    -- acquisition monotonic, so it can't hold a lock out of order that a peer band's
    -- ordered pass then waits behind. Cost is nil: delete-path only, tiny row set.
    WITH picked AS (
        SELECT r.id, r.shard_id
          FROM resources r
          JOIN drain_popped p ON p.resource_id = r.id AND p.task_type = 'delete'
         WHERE r.deletion_requested_at IS NOT NULL
           AND (r.finalizers IS NULL OR array_length(r.finalizers, 1) IS NULL)
           AND NOT EXISTS (SELECT 1 FROM resources c WHERE c.owner_id = r.id)
           AND NOT EXISTS (
               SELECT 1 FROM resource_deps d
               JOIN resources dep ON dep.id = d.dependent_id
               WHERE d.dependency_id = r.id AND dep.deletion_requested_at IS NOT NULL
           )
         ORDER BY r.id
         FOR UPDATE OF r SKIP LOCKED
    )
    DELETE FROM resources r USING picked
     WHERE r.id = picked.id AND r.shard_id = picked.shard_id;

    -- Re-pend a FAILED delete so its teardown retries (strict: it keeps blocking its
    -- parent until it succeeds). The drain removed its work_queue row at the top of
    -- this function, and requeue_failed_and_pending skips deletion_requested rows, so
    -- without this a failed delete would vanish and its parent wedge silently.
    -- Carry the resource's OWN spec (like the sweep_deletable enqueue) — a real IaC
    -- teardown needs the module source + vars; a NULL spec fails "source required"
    -- and re-pends forever. Idempotent under redelivery (ON CONFLICT DO NOTHING).
    INSERT INTO work_queue (resource_id, task_type, kind, kind_version, generation, spec, provider_config, provider_bundle, manifest_version, shard_id)
    SELECT r.id, 'delete'::task_type, r.kind, r.kind_version, r.generation, r.spec,
           (SELECT pc.spec FROM providerconfigs pc WHERE pc.id = r.provider_config_id),
           (SELECT pc.data FROM providerconfigs pc WHERE pc.id = r.provider_config_id),
           r.manifest_version, r.shard_id
      FROM resources r
      JOIN drain_popped p ON p.resource_id = r.id
     WHERE p.task_type = 'delete'
       AND p.failed
       AND r.deletion_requested_at IS NOT NULL
       AND array_length(r.finalizers, 1) IS NOT NULL
    ON CONFLICT (resource_id, task_type, shard_id) DO NOTHING;

    -- Substitute pass: for every successful reconcile whose status
    -- ACTUALLY CHANGED this batch, write flowed dep.spec patches from
    -- the new payload.
    --
    -- Order matters: this MUST run before the coalesced status UPDATE
    -- below, because it reads "did status change" (r.status IS DISTINCT
    -- FROM p.payload) and that write would overwrite r.status. We
    -- capture the changed flow-sources up front as parallel arrays of
    -- (id, payload) — only sources whose payload differs from their
    -- current status AND that have at least one outgoing value_flow edge.
    --
    -- WHY ARRAYS, NOT A drain_popped JOIN: drain_popped is an unanalyzed
    -- temp table. Joining it against million-row resources/resource_deps
    -- makes the planner hash-join against the big tables — the substitute
    -- pass measured ~1.5M ms cumulative to patch ~1700 rows (≈900 ms per
    -- useful row) because every drain batch re-scanned broadly even
    -- though only a handful of sources changed. Pinning the driver to a
    -- tiny UUID[]/JSONB[] pair turns every downstream access into an
    -- indexed nested-loop (resource_deps by dependency_id, resources by
    -- pk) over tens of rows instead of a hash over millions. The
    -- jsonb_array_length(value_flows)>0 filter drops leaf sources
    -- (routes) and the status-distinct filter drops no-op re-drains
    -- (rollup-root re-pends, retries) — together collapsing the work to
    -- the genuine first-substitution events, matching the pre-v2 design.
    SELECT array_agg(c.id), array_agg(c.payload)
      INTO src_ids, src_payloads
    FROM (
        SELECT DISTINCT p.resource_id AS id, p.payload AS payload
        FROM drain_popped p
        JOIN resources r ON r.id = p.resource_id
        WHERE p.task_type = 'reconcile'
          AND p.succeeded = true
          AND p.payload IS NOT NULL
          AND r.status IS DISTINCT FROM p.payload
          AND EXISTS (
              SELECT 1 FROM resource_deps d
              WHERE d.dependency_id = p.resource_id
                AND jsonb_array_length(d.value_flows) > 0
          )
    ) c;

    IF src_ids IS NOT NULL AND array_length(src_ids, 1) > 0 THEN
        WITH src AS (
            SELECT u.id AS resource_id, u.payload
            FROM unnest(src_ids, src_payloads) AS u(id, payload)
        ), patches AS (
            SELECT
                d.dependent_id,
                pointer_to_path(elem->>'dep_field') AS dep_path,
                s.payload #> pointer_to_path(elem->>'src_field') AS src_value
            FROM src s
            JOIN resource_deps d ON d.dependency_id = s.resource_id
            JOIN LATERAL jsonb_array_elements(d.value_flows) AS elem ON true
            WHERE (s.payload #> pointer_to_path(elem->>'src_field')) IS NOT NULL
              AND (s.payload #> pointer_to_path(elem->>'src_field')) != 'null'::jsonb
        ), applicable AS (
            SELECT pa.dependent_id, pa.dep_path, pa.src_value
              FROM patches pa
              JOIN resources r ON r.id = pa.dependent_id
             WHERE (r.spec #> pa.dep_path) IS DISTINCT FROM pa.src_value
               -- FROZEN dependents are NOT patched/bumped (mirror of
               -- apply_value_flows_for_dependents): an orphaned or quarantined
               -- dependent must not be resurrected by an upstream status change.
               AND r.frozen_until IS NULL          -- orphaned OR quarantined = frozen
        ), per_dep AS (
            SELECT dependent_id,
                   jsonb_agg(jsonb_build_object('p', dep_path, 'v', src_value)) AS patches
            FROM applicable
            GROUP BY dependent_id
        ), locked AS (
            -- DETERMINISTIC LOCK ORDERING. value_flow edges are fully
            -- cross-shard: a drainer band owns a disjoint outbox shard range
            -- but patches DEPENDENTS that may live in any shard, so two bands
            -- can target the SAME dependent rows. UPDATE ... FROM has no
            -- ORDER BY and locks rows in arbitrary heap/join order, so two
            -- bands touching {X,Y} in opposite orders deadlock (the 40P01 in
            -- the drainer log). Acquire the row locks here in ascending
            -- primary-key order instead: the LockRows node sits above this
            -- Sort, so rows are locked id-ascending. ALL FOUR resources
            -- writers in this function now lock id-ascending — this substitute
            -- pass, the coalesced status UPDATE below, AND the two delete-path
            -- writers above (the finalizer-strip `strip` CTE and the hard-delete
            -- `picked` CTE were pre-locked id-ascending too, closing a residual
            -- cross-band cycle: a delete-marked row clears frozen_until, so the
            -- `applicable` gate does NOT exclude it, so it could be one band's
            -- strip target and another's substitute dependent, locked in
            -- opposite orders). With every resources writer on one monotonic
            -- id order, no lock-wait cycle can form on the resources class —
            -- concurrent drainers serialize into plain waits rather than
            -- deadlocking. (The remaining 40P01 the drainer retries is the
            -- IRREDUCIBLE cross-class resources<->work_queue cycle: the
            -- cascade trigger on the status UPDATE below must acquire work_queue
            -- locks while co-holding these resources locks to keep the wake
            -- commit-atomic — retry-as-victim is the correct handling there.)
            -- The set is tiny (genuinely-changed dependents only), so the added
            -- Sort is negligible.
            --
            -- FOR NO KEY UPDATE, not FOR UPDATE: the UPDATE below writes only
            -- non-key columns (spec/generation/updated_at), so FOR NO KEY
            -- UPDATE is the EXACT lock that UPDATE takes on its own — the
            -- pre-lock stops OVER-locking. The payoff: FOR NO KEY UPDATE does
            -- NOT conflict with FOR KEY SHARE, the lock every INSERT into a
            -- resources-referencing table (child resources via owner_id,
            -- resource_meta, resource_conditions, resource_events,
            -- resource_operations) implicitly takes on the referenced row. So a
            -- composer inserting a root's children, or this same drain batch's
            -- own conditions/events inserts, no longer block behind a drainer
            -- holding a dependent's lock. (Two NO-KEY-UPDATE writers still
            -- conflict, so the id-ascending order above is still required.)
            SELECT r.id, pd.patches
            FROM per_dep pd
            JOIN resources r ON r.id = pd.dependent_id
            ORDER BY r.id
            FOR NO KEY UPDATE OF r
        )
        -- Patch resources.spec INLINE. Children carry spec inline and are
        -- never versioned, so this stays the in-place UPDATE it always was.
        -- per_dep is only genuinely-changed dependents (applicable gates
        -- IS DISTINCT FROM), so bump generation unconditionally here — this
        -- replaces the dropped bump_generation trigger for the flow path.
        UPDATE resources r
           SET spec = jsonb_set_many(r.spec, lk.patches),
               generation = r.generation + 1,
               updated_at = now()
          FROM locked lk
         WHERE r.id = lk.id;
    END IF;

    -- Coalesced status + synced_gen + health_ok write for reconcile
    -- rows. The cascade_ready_change AFTER STATEMENT trigger fires once
    -- per this UPDATE and inline-schedules direct dependents of any rows
    -- whose is_ready just flipped on (synced_gen caught up AND healthy).
    --
    -- No-op gate: only touch rows that actually change. The composer-root
    -- rollup-root re-pends repeatedly while descendants converge,
    -- each emitting succeeded=true, advance_synced_gen=false, and the
    -- same status it already has. Those rows neither advance synced_gen
    -- nor change status, so rewriting them just churned the hot root
    -- heap tuple (and bloated the transition table the cascade trigger
    -- scans) for zero effect. A skipped row can't flip is_ready, so the
    -- cascade sees exactly the same upgraded set either way.
    --
    -- health_ok: only written when the worker reported it (p.health_ok
    -- IS NOT NULL) AND it differs. A kind that says nothing about health
    -- (the 1M-stress case: p.health_ok NULL on every row) adds a NULL
    -- predicate that is always false here — the gate and the SET both
    -- fall back to the synced_gen/status-only behaviour, byte-identical
    -- to before this column existed.
    --
    -- failure_gen: stamped = observed_generation when the worker reports a
    -- hard failure (p.failed), cleared → 0 on a success that advances
    -- synced_gen. A failed reconcile now PARTICIPATES in this UPDATE (the
    -- p.failed gate disjunct is the only way a NOT-succeeded row gets
    -- here); on a plain healthy reconcile (p.failed=false, failure_gen
    -- already 0) both new disjuncts are constant-false, so the plan and
    -- the written-tuple set are byte-identical to before this column.
    -- DETERMINISTIC LOCK ORDERING (same rationale as the substitute pass).
    -- A row patched as a cross-shard dependent by another band's substitute
    -- pass is ALSO this band's own reconcile target here, so this UPDATE must
    -- take its resources locks in the SAME id-ascending order to keep the
    -- whole function's lock acquisition cycle-free. UPDATE ... FROM can't
    -- ORDER BY, so pre-lock the exact target rows id-ascending in a FOR NO KEY
    -- UPDATE CTE; the UPDATE below then re-states the identical predicate
    -- (semantics unchanged) and only touches rows already locked in order.
    -- FOR NO KEY UPDATE (not FOR UPDATE): this UPDATE changes no key column, so
    -- it's the lock the UPDATE takes anyway, and unlike FOR UPDATE it never
    -- blocks the FOR KEY SHARE that concurrent child/meta/condition/event
    -- INSERTs take on these rows via their resources FKs.
    WITH to_apply AS (
        SELECT r.id
        FROM resources r
        JOIN drain_popped p ON r.id = p.resource_id
        WHERE p.task_type = 'reconcile'
          AND (
              (p.succeeded AND p.advance_synced_gen
                 AND p.observed_generation > r.synced_gen)
              OR (p.succeeded AND p.payload IS NOT NULL
                 AND r.status IS DISTINCT FROM p.payload)
              OR (p.succeeded AND p.health_ok IS NOT NULL
                 AND p.health_ok IS DISTINCT FROM r.health_ok)
              OR (p.failed
                 AND r.failure_gen IS DISTINCT FROM p.observed_generation)
              OR (p.failed AND p.terminal IS DISTINCT FROM r.failure_terminal)
              OR (p.succeeded AND p.advance_synced_gen AND r.failure_gen <> 0)
              -- a REPEAT transient failure at the same generation bumps the
              -- durable failure_attempts counter (and may cross the poison-pill cap),
              -- so it must apply even though failure_gen/terminal are unchanged.
              -- Excludes an already-dead-lettered row so it doesn't re-apply forever.
              OR (p.failed AND NOT p.terminal
                  AND NOT (r.failure_terminal AND r.failure_gen = r.generation))
          )
        ORDER BY r.id
        FOR NO KEY UPDATE OF r
    )
    UPDATE resources r
       SET status = CASE
               WHEN p.succeeded AND p.payload IS NOT NULL THEN p.payload
               ELSE r.status
           END,
           synced_gen = CASE
               WHEN p.succeeded AND p.advance_synced_gen
                   THEN GREATEST(r.synced_gen, p.observed_generation)
               ELSE r.synced_gen
           END,
           health_ok = CASE
               WHEN p.succeeded AND p.health_ok IS NOT NULL THEN p.health_ok
               ELSE r.health_ok
           END,
           failure_gen = CASE
               WHEN p.failed                             THEN p.observed_generation
               WHEN p.succeeded AND p.advance_synced_gen THEN 0
               ELSE r.failure_gen
           END,
           -- durable transient counter: counts CONSECUTIVE transient failures for
           -- the CURRENT generation. It survives the delete+re-pend of the work_queue
           -- row (which is why it lives here, not on work_queue). Generation-aware so
           -- no bump-generation reset is needed at every mutation site: a transient
           -- failure whose observed_generation differs from the last recorded
           -- failure_gen RESTARTS the count at 1 (a new generation is a fresh start);
           -- a repeat at the same generation increments; a synced success clears it.
           failure_attempts = CASE
               WHEN p.failed AND NOT p.terminal THEN
                   CASE WHEN r.failure_gen = p.observed_generation
                        THEN r.failure_attempts + 1
                        ELSE 1
                   END
               WHEN p.succeeded AND p.advance_synced_gen THEN 0
               ELSE r.failure_attempts
           END,
           failure_terminal = CASE
               -- Worker-declared terminal (framework.Terminal) is terminal outright.
               WHEN p.failed AND p.terminal              THEN true
               -- POISON-PILL: a TRANSIENT failure that reaches the kind's
               -- max_transient_attempts is escalated to TERMINAL (dead-letter) so it
               -- stops re-queuing forever. The cap is read per-row from kind_config via
               -- a correlated lookup on the resource's kind (off the hot path — this
               -- CASE arm only runs for a failed reconcile). cap 0 / no kind_config row
               -- = unbounded (never escalate on count), preserving prior behaviour.
               WHEN p.failed AND NOT p.terminal THEN
                   (COALESCE((SELECT kc.max_transient_attempts FROM kind_config kc
                              WHERE kc.kind = r.kind AND kc.kind_version = r.kind_version), 0) > 0
                    -- effective count = the same generation-aware value written to
                    -- failure_attempts above (restart at 1 on a new generation).
                    AND (CASE WHEN r.failure_gen = p.observed_generation
                              THEN r.failure_attempts + 1 ELSE 1 END)
                        >= (SELECT kc.max_transient_attempts FROM kind_config kc
                            WHERE kc.kind = r.kind AND kc.kind_version = r.kind_version))
               WHEN p.succeeded AND p.advance_synced_gen THEN false
               ELSE r.failure_terminal
           END,
           updated_at = now()
      FROM drain_popped p
     WHERE p.task_type = 'reconcile'
       AND r.id = p.resource_id
       AND r.id IN (SELECT id FROM to_apply)
       AND (
           (p.succeeded AND p.advance_synced_gen
              AND p.observed_generation > r.synced_gen)
           OR (p.succeeded AND p.payload IS NOT NULL
              AND r.status IS DISTINCT FROM p.payload)
           OR (p.succeeded AND p.health_ok IS NOT NULL
              AND p.health_ok IS DISTINCT FROM r.health_ok)
           OR (p.failed
              AND r.failure_gen IS DISTINCT FROM p.observed_generation)
           OR (p.failed AND p.terminal IS DISTINCT FROM r.failure_terminal)
           OR (p.succeeded AND p.advance_synced_gen AND r.failure_gen <> 0)
           -- mirror the to_apply gate — a repeat transient failure bumps the
           -- durable failure_attempts counter (and may cross the poison-pill cap).
           OR (p.failed AND NOT p.terminal
               AND NOT (r.failure_terminal AND r.failure_gen = r.generation))
       );

    -- Conditions pass: upsert the rich K8s-style condition rows the
    -- worker emitted into resource_conditions, ON TRANSITION ONLY.
    -- Gated by an outer EXISTS so when no popped row carried conditions
    -- (the 1M-stress case — workers emit none) the whole block is a
    -- single cheap index-less scan of the small temp table that finds
    -- nothing and skips the LATERAL expansion + upsert entirely.
    IF EXISTS (
        SELECT 1 FROM drain_popped p
        WHERE p.conditions IS NOT NULL
          AND jsonb_array_length(p.conditions) > 0
    ) THEN
        INSERT INTO resource_conditions AS rc
            (resource_id, type, status, reason, message,
             observed_generation, last_transition_at)
        SELECT p.resource_id,
               elem->>'type',
               elem->>'status',
               COALESCE(elem->>'reason', ''),
               COALESCE(elem->>'message', ''),
               p.observed_generation,
               now()
        FROM drain_popped p
        JOIN LATERAL jsonb_array_elements(p.conditions) AS elem ON true
        WHERE p.task_type = 'reconcile'
          AND p.conditions IS NOT NULL
          AND jsonb_array_length(p.conditions) > 0
        ON CONFLICT (resource_id, type) DO UPDATE
            SET status = EXCLUDED.status,
                reason = EXCLUDED.reason,
                message = EXCLUDED.message,
                observed_generation = EXCLUDED.observed_generation,
                -- lastTransitionTime parity: only advance when the
                -- status value actually flips.
                last_transition_at = CASE
                    WHEN rc.status IS DISTINCT FROM EXCLUDED.status THEN now()
                    ELSE rc.last_transition_at
                END
            WHERE rc.status  IS DISTINCT FROM EXCLUDED.status
               OR rc.reason  IS DISTINCT FROM EXCLUDED.reason
               OR rc.message IS DISTINCT FROM EXCLUDED.message
               OR rc.observed_generation IS DISTINCT FROM EXCLUDED.observed_generation;
    END IF;

    -- Recovery pass: a reconcile that previously stored a False condition
    -- and now SUCCEEDS (advancing synced_gen) must clear the stale False —
    -- otherwise it survives forever (the upsert above only fires when the
    -- worker re-emits the condition, but a clean success emits none, so
    -- the old row would leak and the detail view would show a stale
    -- error). Clearing it at the source of recovery is what makes
    -- conditions pure decoration: no reader ever has to compensate with
    -- `AND NOT is_ready` masking, and the table stops growing unbounded
    -- under fail→recover churn.
    --
    -- CRUCIAL exclusion: we must NOT delete a False the worker re-asserted
    -- in THIS SAME batch. A synced composite legitimately emits
    -- Ready=False/ChildrenNotReady with advance_synced_gen=true (it is
    -- Degraded, not Failed); the upsert above just wrote that row, and
    -- blindly deleting every False here would erase it. So we skip any
    -- axis whose `type` appears in this popped row's conditions payload.
    --
    -- Gated by the SAME outer-EXISTS discipline as the upsert: when no
    -- popped row recovered (the steady state / 1M-stress run, where
    -- nothing ever failed so no False rows exist) the IF is false and the
    -- DELETE never runs. When it does run it is driven by the tiny
    -- drain_popped temp table — an indexed nested-loop into the
    -- resource_conditions PK over a handful of recovered ids.
    IF EXISTS (
        SELECT 1 FROM drain_popped p
        WHERE p.task_type = 'reconcile'
          AND p.succeeded AND p.advance_synced_gen
    ) THEN
        DELETE FROM resource_conditions rc
         USING drain_popped p
         WHERE rc.resource_id = p.resource_id
           AND p.task_type = 'reconcile'
           AND p.succeeded AND p.advance_synced_gen
           AND rc.status = 'False'
           -- keep any axis the worker just re-emitted this batch
           AND NOT EXISTS (
               SELECT 1
               FROM jsonb_array_elements(COALESCE(p.conditions, '[]'::jsonb)) AS elem
               WHERE elem->>'type' = rc.type
           );
    END IF;

    -- No DROP: drain_popped is ON COMMIT DELETE ROWS, so it empties
    -- automatically when this tx commits and is reused next call.
    RETURN n;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- drain_rollup_rechecks: the FRESH-SNAPSHOT half of the deferred rollup recheck.
-- The drainer calls this each tick for its shard band. Because it runs in a NEW
-- transaction (a new snapshot taken after the straddling drain batches that
-- armed these rows committed), its schedule_eligible descendant gate now sees
-- every sibling's settle → the gate clears → the straddled root pends and its
-- rollup fires within one drainer tick (ms), instead of waiting for the reaper's
-- 5–10s poll + RetryAfter age gate.
--
-- Claims a batch of queued roots in its band (FOR UPDATE SKIP LOCKED so two
-- drainer bands never double-process one), runs schedule_eligible on them (the
-- ON CONFLICT gen-gate makes a concurrent re-pend idempotent — no convoy: at most
-- one writer lands the work_queue row), and DELETEs the processed rows whether or
-- not they pended this time. A row that STILL can't pend (a genuinely-lagging
-- descendant, not a stale snapshot) is dropped here and left to the next cascade
-- (when that descendant settles) or the reaper backstop — so the recheck queue
-- can't accumulate stale entries. Returns how many rows it processed.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION drain_rollup_rechecks(max_rows INTEGER, shard_lo SMALLINT, shard_hi SMALLINT)
RETURNS INTEGER AS $$
DECLARE
    ids UUID[];
BEGIN
    -- Snapshot the queued roots for this band (don't delete yet — we only remove
    -- the ones that actually pend, so a root whose descendants are STILL settling
    -- this snapshot stays queued for the next tick instead of being dropped on the
    -- floor). FOR UPDATE SKIP LOCKED so two bands never double-process one root.
    SELECT array_agg(resource_id ORDER BY resource_id) INTO ids
      FROM (
        SELECT resource_id FROM rollup_recheck
         WHERE shard_id BETWEEN shard_lo AND shard_hi
         ORDER BY resource_id
         LIMIT max_rows
         FOR UPDATE SKIP LOCKED
      ) p;

    IF ids IS NULL OR array_length(ids, 1) = 0 THEN
        RETURN 0;
    END IF;

    -- Fresh-snapshot re-pend: schedule_eligible re-runs the descendant gate (now
    -- seeing all sibling commits) and fires work_ready for any root that pends.
    PERFORM schedule_eligible(ids);

    -- DELETE only the roots that now have a work_queue reconcile row (either we
    -- just pended them, or a concurrent path did). A root that STILL has a lagging
    -- descendant in this fresh snapshot (a genuine in-progress subtree, not a
    -- stale straddle) is LEFT queued. It does NOT need a re-notify: that lagging
    -- descendant's own settle will fire the cascade → wake the drainer
    -- (outbox_ready) → re-run this recheck at a yet-fresher snapshot → pend it.
    -- (Avoiding a self-re-notify here is deliberate — it would busy-spin the bands
    -- while a subtree is legitimately still converging.) The reaper remains the
    -- durable crash backstop for a row whose waking cascade is somehow lost.
    DELETE FROM rollup_recheck r
     WHERE r.resource_id = ANY(ids)
       AND EXISTS (
           SELECT 1 FROM work_queue q
            WHERE q.resource_id = r.resource_id
              AND q.task_type = 'reconcile'::task_type
              AND q.shard_id = r.shard_id
       );

    RETURN array_length(ids, 1);
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- drain_schedule_rechecks: the drainer-paced half of the deferred post-compose
-- schedule. Replaces the post-commit ScheduleEligible(~1M) that deadlocked vs the
-- drainers. The compose ARMS its composed children into schedule_recheck in-tx;
-- the drainer drains this band each tick, running schedule_eligible on a bounded
-- batch — so scheduling is paced WITH the drains (small, band-scoped statements),
-- never a single multi-second writer overlapping the whole fleet. schedule_eligible's
-- dep-gate enqueues the runnable ones (leaves + any dependent whose upstream is
-- already synced) and fires work_ready; a dependent still waiting on its upstream
-- simply doesn't pend yet and is DROPPED from the queue here — it will be scheduled
-- by the drainer cascade (cascade_on_ready_change → schedule_eligible) the instant
-- its upstream settles, exactly as in steady state. Unconditional delete (unlike
-- rollup_recheck's pend-gated delete) is correct here: re-queuing a not-yet-runnable
-- dependent would just re-probe it every tick for nothing; the cascade owns its
-- eventual schedule, and the reaper backstops any lost cascade wake. FOR UPDATE SKIP
-- LOCKED so two bands never double-process a row.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION drain_schedule_rechecks(max_rows INTEGER, shard_lo SMALLINT, shard_hi SMALLINT)
RETURNS INTEGER AS $$
DECLARE
    ids UUID[];
BEGIN
    SELECT array_agg(resource_id ORDER BY resource_id) INTO ids
      FROM (
        SELECT resource_id FROM schedule_recheck
         WHERE shard_id BETWEEN shard_lo AND shard_hi
         ORDER BY resource_id
         LIMIT max_rows
         FOR UPDATE SKIP LOCKED
      ) p;

    IF ids IS NULL OR array_length(ids, 1) = 0 THEN
        RETURN 0;
    END IF;

    -- Schedule the runnable ones (schedule_eligible's dep-gate filters the rest)
    -- and fire work_ready. Small band-scoped batch → no deadlock convoy.
    PERFORM schedule_eligible(ids);

    -- Drop the whole claimed batch: pended rows are now in work_queue; not-yet-
    -- runnable dependents are owned by the cascade (their upstream's settle
    -- schedules them) with the reaper as backstop — re-probing them here every tick
    -- would busy-spin for nothing.
    DELETE FROM schedule_recheck r WHERE r.resource_id = ANY(ids);

    RETURN array_length(ids, 1);
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- +goose Down
DROP TRIGGER IF EXISTS trg_kind_manifest_sync_config ON kind_manifest;
DROP FUNCTION IF EXISTS sync_kind_config_from_manifest();
DROP FUNCTION IF EXISTS validate_kind_manifest(JSONB);
DROP TABLE IF EXISTS kind_manifest;
DROP TRIGGER IF EXISTS cleanup_orphaned_deps ON resources;
DROP TRIGGER IF EXISTS cascade_ready_change ON resources;
DROP TRIGGER IF EXISTS providerconfigs_changed ON providerconfigs;
DROP TRIGGER IF EXISTS cluster_members_changed ON cluster_members;
DROP FUNCTION IF EXISTS notify_providerconfig_changed();
DROP FUNCTION IF EXISTS assign_member_shards(TEXT, INT, INT);
DROP FUNCTION IF EXISTS notify_cluster_changed();
DROP FUNCTION IF EXISTS cleanup_orphaned_deps();
DROP FUNCTION IF EXISTS drain_outbox_batch(INTEGER, SMALLINT, SMALLINT);
DROP FUNCTION IF EXISTS rollback_to_spec(UUID, BIGINT, TEXT);
DROP FUNCTION IF EXISTS gc_spec_history(INTEGER, INTEGER, SMALLINT[]);
DROP FUNCTION IF EXISTS gc_stale_cluster_members(INTEGER);
DROP FUNCTION IF EXISTS unquarantine_resource(UUID, TEXT);
DROP FUNCTION IF EXISTS quarantine_resource(UUID, TEXT);
DROP FUNCTION IF EXISTS request_resource_deletion(UUID, TEXT, TEXT);
DROP FUNCTION IF EXISTS requeue_for_resync(TEXT, INTEGER, INTEGER, SMALLINT[], BOOLEAN);
DROP FUNCTION IF EXISTS requeue_failed_and_pending(INTEGER, INTEGER, SMALLINT[]);
DROP FUNCTION IF EXISTS cascade_on_ready_change();
DROP FUNCTION IF EXISTS claim_reactor_deliveries(TEXT, INTEGER, SMALLINT, SMALLINT);
DROP FUNCTION IF EXISTS ack_reactor_delivery(UUID, TEXT, BIGINT, TEXT, BIGINT);
DROP FUNCTION IF EXISTS reap_stale_lifecycle(INTEGER, INTEGER, SMALLINT, SMALLINT);
DROP FUNCTION IF EXISTS sweep_expired_orphans(INTEGER, SMALLINT, SMALLINT);
DROP FUNCTION IF EXISTS delete_unclaimed_work(INTEGER, INTEGER, SMALLINT, SMALLINT);
DROP FUNCTION IF EXISTS reap_stale_work(INTEGER, INTEGER, SMALLINT, SMALLINT);
DROP FUNCTION IF EXISTS schedule_eligible(UUID[]);
DROP FUNCTION IF EXISTS notify_gated(TEXT);
DROP FUNCTION IF EXISTS apply_value_flows_for_dependents(UUID[]);
DROP FUNCTION IF EXISTS jsonb_set_many(JSONB, JSONB);
DROP FUNCTION IF EXISTS pointer_to_path(TEXT);
DROP TABLE IF EXISTS resource_conditions;
DROP TABLE IF EXISTS cluster_members;
DROP TABLE IF EXISTS resource_events;
DROP TABLE IF EXISTS resource_operations;
DROP TABLE IF EXISTS lifecycle_outbox;
DROP TABLE IF EXISTS reactor_bindings;
DROP TABLE IF EXISTS work_outbox;
DROP TABLE IF EXISTS work_queue;
DROP TABLE IF EXISTS wake_state;
DROP TABLE IF EXISTS kind_inflight;
DROP TABLE IF EXISTS kind_config;
DROP TABLE IF EXISTS resource_deps;
DROP TABLE IF EXISTS resources;
DROP TABLE IF EXISTS providerconfigs;
DROP TABLE IF EXISTS spec_versions;
DROP TYPE IF EXISTS task_type;
DROP EXTENSION IF EXISTS "pgcrypto";
