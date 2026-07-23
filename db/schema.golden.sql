--
--

--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;

--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;

--
-- Name: task_type; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.task_type AS ENUM (
    'reconcile',
    'delete',
    'operate'
);

--
-- Name: ack_reactor_delivery(uuid, text, bigint, text, bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.ack_reactor_delivery(p_resource_id uuid, p_transition text, p_generation bigint, p_binding_name text, p_claim_epoch bigint) RETURNS void
    LANGUAGE sql
    AS $$
    DELETE FROM lifecycle_outbox
     WHERE resource_id = p_resource_id
       AND transition = p_transition
       AND generation = p_generation
       AND binding_name = p_binding_name
       AND claim_epoch = p_claim_epoch;
$$;

--
-- Name: apply_value_flows_for_dependents(uuid[]); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.apply_value_flows_for_dependents(dep_ids uuid[]) RETURNS void
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: assign_member_shards(text, integer, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.assign_member_shards(p_member_id text, p_total integer, p_liveness_secs integer) RETURNS int4range
    LANGUAGE plpgsql STABLE
    AS $$
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
$$;

--
-- Name: cascade_mark_for_deletion(uuid, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.cascade_mark_for_deletion(p_id uuid, p_actor text) RETURNS void
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: cascade_on_ready_change(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.cascade_on_ready_change() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: claim_reactor_deliveries(text, integer, smallint, smallint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.claim_reactor_deliveries(p_broker_id text, max_rows integer, shard_lo smallint, shard_hi smallint) RETURNS TABLE(resource_id uuid, kind text, name text, transition text, generation bigint, status jsonb, binding_name text, reactor text, reaction text, reactor_kind_version integer, claim_epoch bigint)
    LANGUAGE sql
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
$$;

--
-- Name: cleanup_orphaned_deps(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.cleanup_orphaned_deps() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: delete_unclaimed_work(integer, integer, smallint, smallint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.delete_unclaimed_work(delete_seconds integer, max_rows integer, shard_lo smallint, shard_hi smallint) RETURNS integer
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: drain_outbox_batch(integer, smallint, smallint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.drain_outbox_batch(max_rows integer, shard_lo smallint, shard_hi smallint) RETURNS integer
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: drain_rollup_rechecks(integer, smallint, smallint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.drain_rollup_rechecks(max_rows integer, shard_lo smallint, shard_hi smallint) RETURNS integer
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: drain_schedule_rechecks(integer, smallint, smallint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.drain_schedule_rechecks(max_rows integer, shard_lo smallint, shard_hi smallint) RETURNS integer
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: gc_spec_history(integer, integer, smallint[]); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.gc_spec_history(keep_n integer, max_rows integer, shards smallint[]) RETURNS integer
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: gc_stale_cluster_members(integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.gc_stale_cluster_members(ttl_seconds integer) RETURNS integer
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: jsonb_set_many(jsonb, jsonb); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.jsonb_set_many(doc jsonb, patches jsonb) RETURNS jsonb
    LANGUAGE plpgsql IMMUTABLE
    AS $$
DECLARE
    out JSONB := doc;
    p   JSONB;
BEGIN
    FOR p IN SELECT jsonb_array_elements(patches) LOOP
        out := jsonb_set(out, ARRAY(SELECT jsonb_array_elements_text(p->'p')), p->'v', true);
    END LOOP;
    RETURN out;
END;
$$;

--
-- Name: notify_cluster_changed(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.notify_cluster_changed() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    PERFORM notify_gated('cluster_changed');
    RETURN NULL; -- AFTER trigger: return value ignored
END;
$$;

--
-- Name: notify_gated(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.notify_gated(p_channel text) RETURNS void
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: notify_kind_manifest_deleted(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.notify_kind_manifest_deleted() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    PERFORM pg_notify('kind_manifest_changed', OLD.kind || ':' || OLD.kind_version::text);
    RETURN OLD;
END;
$$;

--
-- Name: notify_providerconfig_changed(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.notify_providerconfig_changed() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF (TG_OP = 'INSERT' AND NEW.is_default)
       OR (TG_OP = 'UPDATE' AND (NEW.is_default OR OLD.is_default))
       OR (TG_OP = 'DELETE' AND OLD.is_default) THEN
        PERFORM notify_gated('providerconfig_changed');
    END IF;
    RETURN NULL; -- AFTER trigger: return value ignored
END;
$$;

--
-- Name: pointer_to_path(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.pointer_to_path(p text) RETURNS text[]
    LANGUAGE plpgsql IMMUTABLE
    AS $$
BEGIN
    IF p IS NULL OR p = '' OR p = '/' THEN
        RETURN ARRAY[]::TEXT[];
    END IF;
    IF left(p, 1) = '/' THEN
        RETURN string_to_array(substring(p from 2), '/');
    END IF;
    RETURN ARRAY[p];
END;
$$;

--
-- Name: quarantine_resource(uuid, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.quarantine_resource(p_id uuid, p_actor text) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: reap_stale_lifecycle(integer, integer, smallint, smallint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.reap_stale_lifecycle(stale_seconds integer, max_rows integer, shard_lo smallint, shard_hi smallint) RETURNS integer
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: reap_stale_work(integer, integer, smallint, smallint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.reap_stale_work(stale_seconds integer, max_rows integer, shard_lo smallint, shard_hi smallint) RETURNS integer
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: recount_inflight(smallint, smallint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.recount_inflight(p_shard_lo smallint, p_shard_hi smallint) RETURNS void
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: request_resource_deletion(uuid, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.request_resource_deletion(p_id uuid, p_finalizer text, p_actor text) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: requeue_failed_and_pending(integer, integer, smallint[]); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.requeue_failed_and_pending(retry_seconds integer, max_rows integer, shards smallint[]) RETURNS integer
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: requeue_for_resync(text, integer, integer, smallint[], boolean); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.requeue_for_resync(p_kind text, resync_seconds integer, max_rows integer, shards smallint[], p_recompose boolean) RETURNS integer
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: rollback_to_spec(uuid, bigint, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.rollback_to_spec(p_id uuid, p_target_gen bigint, p_actor text) RETURNS bigint
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: schedule_eligible(uuid[]); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.schedule_eligible(candidate_ids uuid[]) RETURNS integer
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: shard_of(uuid); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.shard_of(rid uuid) RETURNS smallint
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT abs(mod(hashtext(rid::text), 256))::smallint;
$$;

--
-- Name: sweep_deletable(integer, smallint, smallint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.sweep_deletable(max_rows integer, shard_lo smallint, shard_hi smallint) RETURNS integer
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: sweep_expired_orphans(integer, smallint, smallint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.sweep_expired_orphans(max_rows integer, shard_lo smallint, shard_hi smallint) RETURNS integer
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: sync_kind_config_from_manifest(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.sync_kind_config_from_manifest() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: unquarantine_resource(uuid, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.unquarantine_resource(p_id uuid, p_actor text) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: validate_kind_manifest(jsonb); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.validate_kind_manifest(p_reactions jsonb) RETURNS void
    LANGUAGE plpgsql
    AS $$
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
$$;

--
-- Name: cluster_members; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cluster_members (
    member_id text NOT NULL,
    role text NOT NULL,
    shards int4range,
    config jsonb DEFAULT '{}'::jsonb NOT NULL,
    in_flight integer DEFAULT 0 NOT NULL,
    workers jsonb DEFAULT '[]'::jsonb NOT NULL,
    version text DEFAULT ''::text NOT NULL,
    hostname text DEFAULT ''::text NOT NULL,
    pid integer DEFAULT 0 NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    last_heartbeat timestamp with time zone DEFAULT now() NOT NULL
)
WITH (fillfactor='70', autovacuum_vacuum_threshold='2000', autovacuum_vacuum_scale_factor='0', autovacuum_analyze_threshold='2000', autovacuum_analyze_scale_factor='0');

--

--

--
-- Name: kind_config; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.kind_config (
    kind text NOT NULL,
    kind_version integer NOT NULL,
    max_inflight integer DEFAULT 0 NOT NULL,
    task_deadline_secs integer DEFAULT 0 NOT NULL,
    resync_interval_secs integer DEFAULT 0 NOT NULL,
    resync_recomposes boolean DEFAULT false NOT NULL,
    orphan_grace_secs integer DEFAULT 0 NOT NULL,
    finalizer_name text,
    max_transient_attempts integer DEFAULT 0 NOT NULL,
    retired boolean DEFAULT false NOT NULL,
    manifest_version bigint DEFAULT 0 NOT NULL,
    CONSTRAINT kind_config_kind_version_check CHECK ((kind_version >= 1)),
    CONSTRAINT kind_config_max_inflight_check CHECK ((max_inflight >= 0)),
    CONSTRAINT kind_config_max_transient_attempts_check CHECK ((max_transient_attempts >= 0)),
    CONSTRAINT kind_config_orphan_grace_secs_check CHECK ((orphan_grace_secs >= 0)),
    CONSTRAINT kind_config_resync_interval_secs_check CHECK ((resync_interval_secs >= 0)),
    CONSTRAINT kind_config_task_deadline_secs_check CHECK ((task_deadline_secs >= 0))
);

--
-- Name: kind_inflight; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.kind_inflight (
    kind text NOT NULL,
    kind_version integer NOT NULL,
    range_lo smallint NOT NULL,
    in_flight integer DEFAULT 0 NOT NULL,
    refreshed_at timestamp with time zone DEFAULT now() NOT NULL
);

--
-- Name: kind_manifest; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.kind_manifest (
    kind text NOT NULL,
    kind_version integer NOT NULL,
    description text,
    spec_schema jsonb,
    status_schema jsonb,
    config_schema jsonb,
    reactions jsonb DEFAULT '[]'::jsonb NOT NULL,
    finalizer_name text,
    max_inflight integer DEFAULT 0 NOT NULL,
    task_deadline_secs integer DEFAULT 0 NOT NULL,
    resync_interval_secs integer DEFAULT 0 NOT NULL,
    resync_recomposes boolean DEFAULT false NOT NULL,
    orphan_grace_secs integer DEFAULT 0 NOT NULL,
    max_transient_attempts integer DEFAULT 0 NOT NULL,
    retired boolean DEFAULT false NOT NULL,
    schema_hash text,
    manifest_version bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT kind_manifest_kind_version_check CHECK ((kind_version >= 1))
);

--
-- Name: lifecycle_outbox; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lifecycle_outbox (
    resource_id uuid NOT NULL,
    kind text NOT NULL,
    transition text NOT NULL,
    generation bigint NOT NULL,
    binding_name text NOT NULL,
    broker_id text,
    heartbeat_at timestamp with time zone,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    shard_id smallint NOT NULL,
    CONSTRAINT lifecycle_outbox_transition_check CHECK ((transition = ANY (ARRAY['created'::text, 'synced'::text, 'degraded'::text, 'failed'::text, 'deleted'::text])))
)
PARTITION BY RANGE (shard_id);

--
-- Name: lifecycle_outbox_p0; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lifecycle_outbox_p0 (
    resource_id uuid NOT NULL,
    kind text NOT NULL,
    transition text NOT NULL,
    generation bigint NOT NULL,
    binding_name text NOT NULL,
    broker_id text,
    heartbeat_at timestamp with time zone,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    shard_id smallint NOT NULL,
    CONSTRAINT lifecycle_outbox_transition_check CHECK ((transition = ANY (ARRAY['created'::text, 'synced'::text, 'degraded'::text, 'failed'::text, 'deleted'::text])))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: lifecycle_outbox_p1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lifecycle_outbox_p1 (
    resource_id uuid NOT NULL,
    kind text NOT NULL,
    transition text NOT NULL,
    generation bigint NOT NULL,
    binding_name text NOT NULL,
    broker_id text,
    heartbeat_at timestamp with time zone,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    shard_id smallint NOT NULL,
    CONSTRAINT lifecycle_outbox_transition_check CHECK ((transition = ANY (ARRAY['created'::text, 'synced'::text, 'degraded'::text, 'failed'::text, 'deleted'::text])))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: lifecycle_outbox_p10; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lifecycle_outbox_p10 (
    resource_id uuid NOT NULL,
    kind text NOT NULL,
    transition text NOT NULL,
    generation bigint NOT NULL,
    binding_name text NOT NULL,
    broker_id text,
    heartbeat_at timestamp with time zone,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    shard_id smallint NOT NULL,
    CONSTRAINT lifecycle_outbox_transition_check CHECK ((transition = ANY (ARRAY['created'::text, 'synced'::text, 'degraded'::text, 'failed'::text, 'deleted'::text])))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: lifecycle_outbox_p11; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lifecycle_outbox_p11 (
    resource_id uuid NOT NULL,
    kind text NOT NULL,
    transition text NOT NULL,
    generation bigint NOT NULL,
    binding_name text NOT NULL,
    broker_id text,
    heartbeat_at timestamp with time zone,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    shard_id smallint NOT NULL,
    CONSTRAINT lifecycle_outbox_transition_check CHECK ((transition = ANY (ARRAY['created'::text, 'synced'::text, 'degraded'::text, 'failed'::text, 'deleted'::text])))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: lifecycle_outbox_p12; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lifecycle_outbox_p12 (
    resource_id uuid NOT NULL,
    kind text NOT NULL,
    transition text NOT NULL,
    generation bigint NOT NULL,
    binding_name text NOT NULL,
    broker_id text,
    heartbeat_at timestamp with time zone,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    shard_id smallint NOT NULL,
    CONSTRAINT lifecycle_outbox_transition_check CHECK ((transition = ANY (ARRAY['created'::text, 'synced'::text, 'degraded'::text, 'failed'::text, 'deleted'::text])))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: lifecycle_outbox_p13; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lifecycle_outbox_p13 (
    resource_id uuid NOT NULL,
    kind text NOT NULL,
    transition text NOT NULL,
    generation bigint NOT NULL,
    binding_name text NOT NULL,
    broker_id text,
    heartbeat_at timestamp with time zone,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    shard_id smallint NOT NULL,
    CONSTRAINT lifecycle_outbox_transition_check CHECK ((transition = ANY (ARRAY['created'::text, 'synced'::text, 'degraded'::text, 'failed'::text, 'deleted'::text])))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: lifecycle_outbox_p14; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lifecycle_outbox_p14 (
    resource_id uuid NOT NULL,
    kind text NOT NULL,
    transition text NOT NULL,
    generation bigint NOT NULL,
    binding_name text NOT NULL,
    broker_id text,
    heartbeat_at timestamp with time zone,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    shard_id smallint NOT NULL,
    CONSTRAINT lifecycle_outbox_transition_check CHECK ((transition = ANY (ARRAY['created'::text, 'synced'::text, 'degraded'::text, 'failed'::text, 'deleted'::text])))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: lifecycle_outbox_p15; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lifecycle_outbox_p15 (
    resource_id uuid NOT NULL,
    kind text NOT NULL,
    transition text NOT NULL,
    generation bigint NOT NULL,
    binding_name text NOT NULL,
    broker_id text,
    heartbeat_at timestamp with time zone,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    shard_id smallint NOT NULL,
    CONSTRAINT lifecycle_outbox_transition_check CHECK ((transition = ANY (ARRAY['created'::text, 'synced'::text, 'degraded'::text, 'failed'::text, 'deleted'::text])))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: lifecycle_outbox_p2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lifecycle_outbox_p2 (
    resource_id uuid NOT NULL,
    kind text NOT NULL,
    transition text NOT NULL,
    generation bigint NOT NULL,
    binding_name text NOT NULL,
    broker_id text,
    heartbeat_at timestamp with time zone,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    shard_id smallint NOT NULL,
    CONSTRAINT lifecycle_outbox_transition_check CHECK ((transition = ANY (ARRAY['created'::text, 'synced'::text, 'degraded'::text, 'failed'::text, 'deleted'::text])))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: lifecycle_outbox_p3; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lifecycle_outbox_p3 (
    resource_id uuid NOT NULL,
    kind text NOT NULL,
    transition text NOT NULL,
    generation bigint NOT NULL,
    binding_name text NOT NULL,
    broker_id text,
    heartbeat_at timestamp with time zone,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    shard_id smallint NOT NULL,
    CONSTRAINT lifecycle_outbox_transition_check CHECK ((transition = ANY (ARRAY['created'::text, 'synced'::text, 'degraded'::text, 'failed'::text, 'deleted'::text])))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: lifecycle_outbox_p4; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lifecycle_outbox_p4 (
    resource_id uuid NOT NULL,
    kind text NOT NULL,
    transition text NOT NULL,
    generation bigint NOT NULL,
    binding_name text NOT NULL,
    broker_id text,
    heartbeat_at timestamp with time zone,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    shard_id smallint NOT NULL,
    CONSTRAINT lifecycle_outbox_transition_check CHECK ((transition = ANY (ARRAY['created'::text, 'synced'::text, 'degraded'::text, 'failed'::text, 'deleted'::text])))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: lifecycle_outbox_p5; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lifecycle_outbox_p5 (
    resource_id uuid NOT NULL,
    kind text NOT NULL,
    transition text NOT NULL,
    generation bigint NOT NULL,
    binding_name text NOT NULL,
    broker_id text,
    heartbeat_at timestamp with time zone,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    shard_id smallint NOT NULL,
    CONSTRAINT lifecycle_outbox_transition_check CHECK ((transition = ANY (ARRAY['created'::text, 'synced'::text, 'degraded'::text, 'failed'::text, 'deleted'::text])))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: lifecycle_outbox_p6; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lifecycle_outbox_p6 (
    resource_id uuid NOT NULL,
    kind text NOT NULL,
    transition text NOT NULL,
    generation bigint NOT NULL,
    binding_name text NOT NULL,
    broker_id text,
    heartbeat_at timestamp with time zone,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    shard_id smallint NOT NULL,
    CONSTRAINT lifecycle_outbox_transition_check CHECK ((transition = ANY (ARRAY['created'::text, 'synced'::text, 'degraded'::text, 'failed'::text, 'deleted'::text])))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: lifecycle_outbox_p7; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lifecycle_outbox_p7 (
    resource_id uuid NOT NULL,
    kind text NOT NULL,
    transition text NOT NULL,
    generation bigint NOT NULL,
    binding_name text NOT NULL,
    broker_id text,
    heartbeat_at timestamp with time zone,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    shard_id smallint NOT NULL,
    CONSTRAINT lifecycle_outbox_transition_check CHECK ((transition = ANY (ARRAY['created'::text, 'synced'::text, 'degraded'::text, 'failed'::text, 'deleted'::text])))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: lifecycle_outbox_p8; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lifecycle_outbox_p8 (
    resource_id uuid NOT NULL,
    kind text NOT NULL,
    transition text NOT NULL,
    generation bigint NOT NULL,
    binding_name text NOT NULL,
    broker_id text,
    heartbeat_at timestamp with time zone,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    shard_id smallint NOT NULL,
    CONSTRAINT lifecycle_outbox_transition_check CHECK ((transition = ANY (ARRAY['created'::text, 'synced'::text, 'degraded'::text, 'failed'::text, 'deleted'::text])))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: lifecycle_outbox_p9; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lifecycle_outbox_p9 (
    resource_id uuid NOT NULL,
    kind text NOT NULL,
    transition text NOT NULL,
    generation bigint NOT NULL,
    binding_name text NOT NULL,
    broker_id text,
    heartbeat_at timestamp with time zone,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    shard_id smallint NOT NULL,
    CONSTRAINT lifecycle_outbox_transition_check CHECK ((transition = ANY (ARRAY['created'::text, 'synced'::text, 'degraded'::text, 'failed'::text, 'deleted'::text])))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: providerconfigs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.providerconfigs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    name text NOT NULL,
    kind text NOT NULL,
    kind_version smallint NOT NULL,
    is_default boolean DEFAULT false NOT NULL,
    spec jsonb DEFAULT '{}'::jsonb NOT NULL,
    data bytea DEFAULT '\x'::bytea NOT NULL,
    owner_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT providerconfigs_kind_version_check CHECK ((kind_version >= 1))
);

--
-- Name: reactor_bindings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.reactor_bindings (
    name text NOT NULL,
    watch_kind text NOT NULL,
    watch_kind_version smallint,
    transition text NOT NULL,
    label_match jsonb DEFAULT '{}'::jsonb NOT NULL,
    reactor text NOT NULL,
    reactor_version smallint,
    enabled boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT reactor_bindings_reactor_version_check CHECK (((reactor_version IS NULL) OR (reactor_version >= 1))),
    CONSTRAINT reactor_bindings_transition_check CHECK ((transition = ANY (ARRAY['created'::text, 'synced'::text, 'degraded'::text, 'failed'::text, 'deleted'::text]))),
    CONSTRAINT reactor_bindings_watch_kind_version_check CHECK (((watch_kind_version IS NULL) OR (watch_kind_version >= 1)))
);

--
-- Name: resource_conditions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.resource_conditions (
    resource_id uuid NOT NULL,
    type text NOT NULL,
    status text NOT NULL,
    reason text DEFAULT ''::text NOT NULL,
    message text DEFAULT ''::text NOT NULL,
    observed_generation bigint DEFAULT 0 NOT NULL,
    last_transition_at timestamp with time zone DEFAULT now() NOT NULL
);

--
-- Name: resource_deps; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.resource_deps (
    dependent_id uuid NOT NULL,
    dependency_id uuid NOT NULL,
    value_flows jsonb DEFAULT '[]'::jsonb NOT NULL,
    src_kind_version integer DEFAULT 0 NOT NULL
);

--
-- Name: resource_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.resource_events (
    id bigint NOT NULL,
    resource_id uuid NOT NULL,
    type text NOT NULL,
    actor text,
    reason text,
    message text,
    detail jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

--
-- Name: resource_events_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.resource_events_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

--
-- Name: resource_events_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.resource_events_id_seq OWNED BY public.resource_events.id;

--
-- Name: resource_meta; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.resource_meta (
    id uuid NOT NULL,
    kind text NOT NULL,
    name text NOT NULL,
    labels jsonb DEFAULT '{}'::jsonb NOT NULL
);

--
-- Name: resource_operations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.resource_operations (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    resource_id uuid NOT NULL,
    verb text NOT NULL,
    input jsonb DEFAULT '{}'::jsonb NOT NULL,
    output jsonb,
    state text DEFAULT 'pending'::text NOT NULL,
    error_message text,
    attempts integer DEFAULT 0 NOT NULL,
    requested_by text,
    requested_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT resource_operations_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'running'::text, 'succeeded'::text, 'failed'::text])))
);

--
-- Name: resources; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.resources (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    kind text NOT NULL,
    kind_version smallint NOT NULL,
    owner_id uuid,
    root_id uuid,
    spec jsonb DEFAULT '{}'::jsonb NOT NULL,
    status jsonb,
    provider_config_id uuid,
    generation bigint DEFAULT 1 NOT NULL,
    synced_gen bigint DEFAULT 0 NOT NULL,
    health_ok boolean DEFAULT true NOT NULL,
    composed_gen bigint DEFAULT 0 NOT NULL,
    manifest_version bigint DEFAULT 0 NOT NULL,
    failure_gen bigint DEFAULT 0 NOT NULL,
    failure_terminal boolean DEFAULT false NOT NULL,
    failure_attempts integer DEFAULT 0 NOT NULL,
    is_ready boolean GENERATED ALWAYS AS (((synced_gen >= generation) AND health_ok AND (deletion_requested_at IS NULL))) STORED NOT NULL,
    phase text GENERATED ALWAYS AS (
CASE
    WHEN (deletion_requested_at IS NOT NULL) THEN 'Deleting'::text
    WHEN (frozen_until = 'infinity'::timestamp with time zone) THEN 'Quarantined'::text
    WHEN (failure_gen = generation) THEN 'Failed'::text
    WHEN (frozen_until IS NOT NULL) THEN 'Orphaned'::text
    WHEN ((synced_gen >= generation) AND (NOT health_ok)) THEN 'Degraded'::text
    WHEN (synced_gen >= generation) THEN 'Ready'::text
    ELSE 'Reconciling'::text
END) STORED NOT NULL,
    finalizers text[] DEFAULT '{}'::text[] NOT NULL,
    deletion_requested_at timestamp with time zone,
    frozen_until timestamp with time zone,
    last_reconciled_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    shard_id smallint GENERATED ALWAYS AS (abs(mod(hashtext((id)::text), 256))) STORED NOT NULL,
    CONSTRAINT resources_kind_version_check CHECK ((kind_version >= 1))
)
WITH (fillfactor='90', autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: rollup_recheck; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.rollup_recheck (
    resource_id uuid NOT NULL,
    shard_id smallint NOT NULL
);

--
-- Name: schedule_recheck; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.schedule_recheck (
    resource_id uuid NOT NULL,
    shard_id smallint NOT NULL
);

--
-- Name: spec_versions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.spec_versions (
    resource_id uuid NOT NULL,
    generation bigint NOT NULL,
    spec jsonb NOT NULL,
    source text DEFAULT 'apply'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

--
-- Name: wake_state; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.wake_state (
    channel text NOT NULL,
    last_notify_at timestamp with time zone DEFAULT '-infinity'::timestamp with time zone NOT NULL
);

--
-- Name: work_outbox; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_outbox (
    work_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    op_id uuid,
    task_type public.task_type NOT NULL,
    succeeded boolean NOT NULL,
    payload jsonb,
    error_message text,
    observed_generation bigint DEFAULT 0 NOT NULL,
    advance_synced_gen boolean DEFAULT true NOT NULL,
    health_ok boolean,
    conditions jsonb,
    failed boolean DEFAULT false NOT NULL,
    terminal boolean DEFAULT false NOT NULL,
    finalizer_name text,
    manifest_version bigint DEFAULT 0 NOT NULL,
    shard_id smallint NOT NULL
)
PARTITION BY RANGE (shard_id);

--
-- Name: work_outbox_p0; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_outbox_p0 (
    work_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    op_id uuid,
    task_type public.task_type NOT NULL,
    succeeded boolean NOT NULL,
    payload jsonb,
    error_message text,
    observed_generation bigint DEFAULT 0 NOT NULL,
    advance_synced_gen boolean DEFAULT true NOT NULL,
    health_ok boolean,
    conditions jsonb,
    failed boolean DEFAULT false NOT NULL,
    terminal boolean DEFAULT false NOT NULL,
    finalizer_name text,
    manifest_version bigint DEFAULT 0 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_outbox_p1; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_outbox_p1 (
    work_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    op_id uuid,
    task_type public.task_type NOT NULL,
    succeeded boolean NOT NULL,
    payload jsonb,
    error_message text,
    observed_generation bigint DEFAULT 0 NOT NULL,
    advance_synced_gen boolean DEFAULT true NOT NULL,
    health_ok boolean,
    conditions jsonb,
    failed boolean DEFAULT false NOT NULL,
    terminal boolean DEFAULT false NOT NULL,
    finalizer_name text,
    manifest_version bigint DEFAULT 0 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_outbox_p10; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_outbox_p10 (
    work_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    op_id uuid,
    task_type public.task_type NOT NULL,
    succeeded boolean NOT NULL,
    payload jsonb,
    error_message text,
    observed_generation bigint DEFAULT 0 NOT NULL,
    advance_synced_gen boolean DEFAULT true NOT NULL,
    health_ok boolean,
    conditions jsonb,
    failed boolean DEFAULT false NOT NULL,
    terminal boolean DEFAULT false NOT NULL,
    finalizer_name text,
    manifest_version bigint DEFAULT 0 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_outbox_p11; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_outbox_p11 (
    work_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    op_id uuid,
    task_type public.task_type NOT NULL,
    succeeded boolean NOT NULL,
    payload jsonb,
    error_message text,
    observed_generation bigint DEFAULT 0 NOT NULL,
    advance_synced_gen boolean DEFAULT true NOT NULL,
    health_ok boolean,
    conditions jsonb,
    failed boolean DEFAULT false NOT NULL,
    terminal boolean DEFAULT false NOT NULL,
    finalizer_name text,
    manifest_version bigint DEFAULT 0 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_outbox_p12; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_outbox_p12 (
    work_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    op_id uuid,
    task_type public.task_type NOT NULL,
    succeeded boolean NOT NULL,
    payload jsonb,
    error_message text,
    observed_generation bigint DEFAULT 0 NOT NULL,
    advance_synced_gen boolean DEFAULT true NOT NULL,
    health_ok boolean,
    conditions jsonb,
    failed boolean DEFAULT false NOT NULL,
    terminal boolean DEFAULT false NOT NULL,
    finalizer_name text,
    manifest_version bigint DEFAULT 0 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_outbox_p13; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_outbox_p13 (
    work_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    op_id uuid,
    task_type public.task_type NOT NULL,
    succeeded boolean NOT NULL,
    payload jsonb,
    error_message text,
    observed_generation bigint DEFAULT 0 NOT NULL,
    advance_synced_gen boolean DEFAULT true NOT NULL,
    health_ok boolean,
    conditions jsonb,
    failed boolean DEFAULT false NOT NULL,
    terminal boolean DEFAULT false NOT NULL,
    finalizer_name text,
    manifest_version bigint DEFAULT 0 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_outbox_p14; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_outbox_p14 (
    work_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    op_id uuid,
    task_type public.task_type NOT NULL,
    succeeded boolean NOT NULL,
    payload jsonb,
    error_message text,
    observed_generation bigint DEFAULT 0 NOT NULL,
    advance_synced_gen boolean DEFAULT true NOT NULL,
    health_ok boolean,
    conditions jsonb,
    failed boolean DEFAULT false NOT NULL,
    terminal boolean DEFAULT false NOT NULL,
    finalizer_name text,
    manifest_version bigint DEFAULT 0 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_outbox_p15; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_outbox_p15 (
    work_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    op_id uuid,
    task_type public.task_type NOT NULL,
    succeeded boolean NOT NULL,
    payload jsonb,
    error_message text,
    observed_generation bigint DEFAULT 0 NOT NULL,
    advance_synced_gen boolean DEFAULT true NOT NULL,
    health_ok boolean,
    conditions jsonb,
    failed boolean DEFAULT false NOT NULL,
    terminal boolean DEFAULT false NOT NULL,
    finalizer_name text,
    manifest_version bigint DEFAULT 0 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_outbox_p2; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_outbox_p2 (
    work_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    op_id uuid,
    task_type public.task_type NOT NULL,
    succeeded boolean NOT NULL,
    payload jsonb,
    error_message text,
    observed_generation bigint DEFAULT 0 NOT NULL,
    advance_synced_gen boolean DEFAULT true NOT NULL,
    health_ok boolean,
    conditions jsonb,
    failed boolean DEFAULT false NOT NULL,
    terminal boolean DEFAULT false NOT NULL,
    finalizer_name text,
    manifest_version bigint DEFAULT 0 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_outbox_p3; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_outbox_p3 (
    work_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    op_id uuid,
    task_type public.task_type NOT NULL,
    succeeded boolean NOT NULL,
    payload jsonb,
    error_message text,
    observed_generation bigint DEFAULT 0 NOT NULL,
    advance_synced_gen boolean DEFAULT true NOT NULL,
    health_ok boolean,
    conditions jsonb,
    failed boolean DEFAULT false NOT NULL,
    terminal boolean DEFAULT false NOT NULL,
    finalizer_name text,
    manifest_version bigint DEFAULT 0 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_outbox_p4; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_outbox_p4 (
    work_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    op_id uuid,
    task_type public.task_type NOT NULL,
    succeeded boolean NOT NULL,
    payload jsonb,
    error_message text,
    observed_generation bigint DEFAULT 0 NOT NULL,
    advance_synced_gen boolean DEFAULT true NOT NULL,
    health_ok boolean,
    conditions jsonb,
    failed boolean DEFAULT false NOT NULL,
    terminal boolean DEFAULT false NOT NULL,
    finalizer_name text,
    manifest_version bigint DEFAULT 0 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_outbox_p5; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_outbox_p5 (
    work_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    op_id uuid,
    task_type public.task_type NOT NULL,
    succeeded boolean NOT NULL,
    payload jsonb,
    error_message text,
    observed_generation bigint DEFAULT 0 NOT NULL,
    advance_synced_gen boolean DEFAULT true NOT NULL,
    health_ok boolean,
    conditions jsonb,
    failed boolean DEFAULT false NOT NULL,
    terminal boolean DEFAULT false NOT NULL,
    finalizer_name text,
    manifest_version bigint DEFAULT 0 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_outbox_p6; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_outbox_p6 (
    work_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    op_id uuid,
    task_type public.task_type NOT NULL,
    succeeded boolean NOT NULL,
    payload jsonb,
    error_message text,
    observed_generation bigint DEFAULT 0 NOT NULL,
    advance_synced_gen boolean DEFAULT true NOT NULL,
    health_ok boolean,
    conditions jsonb,
    failed boolean DEFAULT false NOT NULL,
    terminal boolean DEFAULT false NOT NULL,
    finalizer_name text,
    manifest_version bigint DEFAULT 0 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_outbox_p7; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_outbox_p7 (
    work_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    op_id uuid,
    task_type public.task_type NOT NULL,
    succeeded boolean NOT NULL,
    payload jsonb,
    error_message text,
    observed_generation bigint DEFAULT 0 NOT NULL,
    advance_synced_gen boolean DEFAULT true NOT NULL,
    health_ok boolean,
    conditions jsonb,
    failed boolean DEFAULT false NOT NULL,
    terminal boolean DEFAULT false NOT NULL,
    finalizer_name text,
    manifest_version bigint DEFAULT 0 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_outbox_p8; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_outbox_p8 (
    work_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    op_id uuid,
    task_type public.task_type NOT NULL,
    succeeded boolean NOT NULL,
    payload jsonb,
    error_message text,
    observed_generation bigint DEFAULT 0 NOT NULL,
    advance_synced_gen boolean DEFAULT true NOT NULL,
    health_ok boolean,
    conditions jsonb,
    failed boolean DEFAULT false NOT NULL,
    terminal boolean DEFAULT false NOT NULL,
    finalizer_name text,
    manifest_version bigint DEFAULT 0 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_outbox_p9; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_outbox_p9 (
    work_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    op_id uuid,
    task_type public.task_type NOT NULL,
    succeeded boolean NOT NULL,
    payload jsonb,
    error_message text,
    observed_generation bigint DEFAULT 0 NOT NULL,
    advance_synced_gen boolean DEFAULT true NOT NULL,
    health_ok boolean,
    conditions jsonb,
    failed boolean DEFAULT false NOT NULL,
    terminal boolean DEFAULT false NOT NULL,
    finalizer_name text,
    manifest_version bigint DEFAULT 0 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_queue; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_queue (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    resource_id uuid NOT NULL,
    task_type public.task_type NOT NULL,
    op_id uuid,
    kind text NOT NULL,
    generation bigint NOT NULL,
    spec jsonb,
    provider_config jsonb,
    provider_bundle bytea,
    attempts integer DEFAULT 1 NOT NULL,
    broker_id text,
    worker_id text,
    heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    manifest_version bigint DEFAULT 0 NOT NULL,
    kind_version smallint NOT NULL,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    shard_id smallint NOT NULL
)
PARTITION BY RANGE (shard_id);

--
-- Name: work_queue_p0; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_queue_p0 (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    resource_id uuid NOT NULL,
    task_type public.task_type NOT NULL,
    op_id uuid,
    kind text NOT NULL,
    generation bigint NOT NULL,
    spec jsonb,
    provider_config jsonb,
    provider_bundle bytea,
    attempts integer DEFAULT 1 NOT NULL,
    broker_id text,
    worker_id text,
    heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    manifest_version bigint DEFAULT 0 NOT NULL,
    kind_version smallint NOT NULL,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_queue_p1; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_queue_p1 (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    resource_id uuid NOT NULL,
    task_type public.task_type NOT NULL,
    op_id uuid,
    kind text NOT NULL,
    generation bigint NOT NULL,
    spec jsonb,
    provider_config jsonb,
    provider_bundle bytea,
    attempts integer DEFAULT 1 NOT NULL,
    broker_id text,
    worker_id text,
    heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    manifest_version bigint DEFAULT 0 NOT NULL,
    kind_version smallint NOT NULL,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_queue_p10; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_queue_p10 (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    resource_id uuid NOT NULL,
    task_type public.task_type NOT NULL,
    op_id uuid,
    kind text NOT NULL,
    generation bigint NOT NULL,
    spec jsonb,
    provider_config jsonb,
    provider_bundle bytea,
    attempts integer DEFAULT 1 NOT NULL,
    broker_id text,
    worker_id text,
    heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    manifest_version bigint DEFAULT 0 NOT NULL,
    kind_version smallint NOT NULL,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_queue_p11; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_queue_p11 (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    resource_id uuid NOT NULL,
    task_type public.task_type NOT NULL,
    op_id uuid,
    kind text NOT NULL,
    generation bigint NOT NULL,
    spec jsonb,
    provider_config jsonb,
    provider_bundle bytea,
    attempts integer DEFAULT 1 NOT NULL,
    broker_id text,
    worker_id text,
    heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    manifest_version bigint DEFAULT 0 NOT NULL,
    kind_version smallint NOT NULL,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_queue_p12; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_queue_p12 (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    resource_id uuid NOT NULL,
    task_type public.task_type NOT NULL,
    op_id uuid,
    kind text NOT NULL,
    generation bigint NOT NULL,
    spec jsonb,
    provider_config jsonb,
    provider_bundle bytea,
    attempts integer DEFAULT 1 NOT NULL,
    broker_id text,
    worker_id text,
    heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    manifest_version bigint DEFAULT 0 NOT NULL,
    kind_version smallint NOT NULL,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_queue_p13; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_queue_p13 (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    resource_id uuid NOT NULL,
    task_type public.task_type NOT NULL,
    op_id uuid,
    kind text NOT NULL,
    generation bigint NOT NULL,
    spec jsonb,
    provider_config jsonb,
    provider_bundle bytea,
    attempts integer DEFAULT 1 NOT NULL,
    broker_id text,
    worker_id text,
    heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    manifest_version bigint DEFAULT 0 NOT NULL,
    kind_version smallint NOT NULL,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_queue_p14; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_queue_p14 (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    resource_id uuid NOT NULL,
    task_type public.task_type NOT NULL,
    op_id uuid,
    kind text NOT NULL,
    generation bigint NOT NULL,
    spec jsonb,
    provider_config jsonb,
    provider_bundle bytea,
    attempts integer DEFAULT 1 NOT NULL,
    broker_id text,
    worker_id text,
    heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    manifest_version bigint DEFAULT 0 NOT NULL,
    kind_version smallint NOT NULL,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_queue_p15; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_queue_p15 (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    resource_id uuid NOT NULL,
    task_type public.task_type NOT NULL,
    op_id uuid,
    kind text NOT NULL,
    generation bigint NOT NULL,
    spec jsonb,
    provider_config jsonb,
    provider_bundle bytea,
    attempts integer DEFAULT 1 NOT NULL,
    broker_id text,
    worker_id text,
    heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    manifest_version bigint DEFAULT 0 NOT NULL,
    kind_version smallint NOT NULL,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_queue_p2; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_queue_p2 (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    resource_id uuid NOT NULL,
    task_type public.task_type NOT NULL,
    op_id uuid,
    kind text NOT NULL,
    generation bigint NOT NULL,
    spec jsonb,
    provider_config jsonb,
    provider_bundle bytea,
    attempts integer DEFAULT 1 NOT NULL,
    broker_id text,
    worker_id text,
    heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    manifest_version bigint DEFAULT 0 NOT NULL,
    kind_version smallint NOT NULL,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_queue_p3; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_queue_p3 (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    resource_id uuid NOT NULL,
    task_type public.task_type NOT NULL,
    op_id uuid,
    kind text NOT NULL,
    generation bigint NOT NULL,
    spec jsonb,
    provider_config jsonb,
    provider_bundle bytea,
    attempts integer DEFAULT 1 NOT NULL,
    broker_id text,
    worker_id text,
    heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    manifest_version bigint DEFAULT 0 NOT NULL,
    kind_version smallint NOT NULL,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_queue_p4; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_queue_p4 (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    resource_id uuid NOT NULL,
    task_type public.task_type NOT NULL,
    op_id uuid,
    kind text NOT NULL,
    generation bigint NOT NULL,
    spec jsonb,
    provider_config jsonb,
    provider_bundle bytea,
    attempts integer DEFAULT 1 NOT NULL,
    broker_id text,
    worker_id text,
    heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    manifest_version bigint DEFAULT 0 NOT NULL,
    kind_version smallint NOT NULL,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_queue_p5; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_queue_p5 (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    resource_id uuid NOT NULL,
    task_type public.task_type NOT NULL,
    op_id uuid,
    kind text NOT NULL,
    generation bigint NOT NULL,
    spec jsonb,
    provider_config jsonb,
    provider_bundle bytea,
    attempts integer DEFAULT 1 NOT NULL,
    broker_id text,
    worker_id text,
    heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    manifest_version bigint DEFAULT 0 NOT NULL,
    kind_version smallint NOT NULL,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_queue_p6; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_queue_p6 (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    resource_id uuid NOT NULL,
    task_type public.task_type NOT NULL,
    op_id uuid,
    kind text NOT NULL,
    generation bigint NOT NULL,
    spec jsonb,
    provider_config jsonb,
    provider_bundle bytea,
    attempts integer DEFAULT 1 NOT NULL,
    broker_id text,
    worker_id text,
    heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    manifest_version bigint DEFAULT 0 NOT NULL,
    kind_version smallint NOT NULL,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_queue_p7; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_queue_p7 (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    resource_id uuid NOT NULL,
    task_type public.task_type NOT NULL,
    op_id uuid,
    kind text NOT NULL,
    generation bigint NOT NULL,
    spec jsonb,
    provider_config jsonb,
    provider_bundle bytea,
    attempts integer DEFAULT 1 NOT NULL,
    broker_id text,
    worker_id text,
    heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    manifest_version bigint DEFAULT 0 NOT NULL,
    kind_version smallint NOT NULL,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_queue_p8; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_queue_p8 (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    resource_id uuid NOT NULL,
    task_type public.task_type NOT NULL,
    op_id uuid,
    kind text NOT NULL,
    generation bigint NOT NULL,
    spec jsonb,
    provider_config jsonb,
    provider_bundle bytea,
    attempts integer DEFAULT 1 NOT NULL,
    broker_id text,
    worker_id text,
    heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    manifest_version bigint DEFAULT 0 NOT NULL,
    kind_version smallint NOT NULL,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: work_queue_p9; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.work_queue_p9 (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    resource_id uuid NOT NULL,
    task_type public.task_type NOT NULL,
    op_id uuid,
    kind text NOT NULL,
    generation bigint NOT NULL,
    spec jsonb,
    provider_config jsonb,
    provider_bundle bytea,
    attempts integer DEFAULT 1 NOT NULL,
    broker_id text,
    worker_id text,
    heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    manifest_version bigint DEFAULT 0 NOT NULL,
    kind_version smallint NOT NULL,
    claim_epoch bigint DEFAULT 1 NOT NULL,
    shard_id smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='50', autovacuum_vacuum_cost_delay='10', autovacuum_vacuum_cost_limit='1000');

--
-- Name: lifecycle_outbox_p0; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox ATTACH PARTITION public.lifecycle_outbox_p0 FOR VALUES FROM ('0') TO ('16');

--
-- Name: lifecycle_outbox_p1; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox ATTACH PARTITION public.lifecycle_outbox_p1 FOR VALUES FROM ('16') TO ('32');

--
-- Name: lifecycle_outbox_p10; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox ATTACH PARTITION public.lifecycle_outbox_p10 FOR VALUES FROM ('160') TO ('176');

--
-- Name: lifecycle_outbox_p11; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox ATTACH PARTITION public.lifecycle_outbox_p11 FOR VALUES FROM ('176') TO ('192');

--
-- Name: lifecycle_outbox_p12; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox ATTACH PARTITION public.lifecycle_outbox_p12 FOR VALUES FROM ('192') TO ('208');

--
-- Name: lifecycle_outbox_p13; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox ATTACH PARTITION public.lifecycle_outbox_p13 FOR VALUES FROM ('208') TO ('224');

--
-- Name: lifecycle_outbox_p14; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox ATTACH PARTITION public.lifecycle_outbox_p14 FOR VALUES FROM ('224') TO ('240');

--
-- Name: lifecycle_outbox_p15; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox ATTACH PARTITION public.lifecycle_outbox_p15 FOR VALUES FROM ('240') TO ('256');

--
-- Name: lifecycle_outbox_p2; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox ATTACH PARTITION public.lifecycle_outbox_p2 FOR VALUES FROM ('32') TO ('48');

--
-- Name: lifecycle_outbox_p3; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox ATTACH PARTITION public.lifecycle_outbox_p3 FOR VALUES FROM ('48') TO ('64');

--
-- Name: lifecycle_outbox_p4; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox ATTACH PARTITION public.lifecycle_outbox_p4 FOR VALUES FROM ('64') TO ('80');

--
-- Name: lifecycle_outbox_p5; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox ATTACH PARTITION public.lifecycle_outbox_p5 FOR VALUES FROM ('80') TO ('96');

--
-- Name: lifecycle_outbox_p6; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox ATTACH PARTITION public.lifecycle_outbox_p6 FOR VALUES FROM ('96') TO ('112');

--
-- Name: lifecycle_outbox_p7; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox ATTACH PARTITION public.lifecycle_outbox_p7 FOR VALUES FROM ('112') TO ('128');

--
-- Name: lifecycle_outbox_p8; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox ATTACH PARTITION public.lifecycle_outbox_p8 FOR VALUES FROM ('128') TO ('144');

--
-- Name: lifecycle_outbox_p9; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox ATTACH PARTITION public.lifecycle_outbox_p9 FOR VALUES FROM ('144') TO ('160');

--
-- Name: work_outbox_p0; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox ATTACH PARTITION public.work_outbox_p0 FOR VALUES FROM ('0') TO ('16');

--
-- Name: work_outbox_p1; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox ATTACH PARTITION public.work_outbox_p1 FOR VALUES FROM ('16') TO ('32');

--
-- Name: work_outbox_p10; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox ATTACH PARTITION public.work_outbox_p10 FOR VALUES FROM ('160') TO ('176');

--
-- Name: work_outbox_p11; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox ATTACH PARTITION public.work_outbox_p11 FOR VALUES FROM ('176') TO ('192');

--
-- Name: work_outbox_p12; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox ATTACH PARTITION public.work_outbox_p12 FOR VALUES FROM ('192') TO ('208');

--
-- Name: work_outbox_p13; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox ATTACH PARTITION public.work_outbox_p13 FOR VALUES FROM ('208') TO ('224');

--
-- Name: work_outbox_p14; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox ATTACH PARTITION public.work_outbox_p14 FOR VALUES FROM ('224') TO ('240');

--
-- Name: work_outbox_p15; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox ATTACH PARTITION public.work_outbox_p15 FOR VALUES FROM ('240') TO ('256');

--
-- Name: work_outbox_p2; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox ATTACH PARTITION public.work_outbox_p2 FOR VALUES FROM ('32') TO ('48');

--
-- Name: work_outbox_p3; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox ATTACH PARTITION public.work_outbox_p3 FOR VALUES FROM ('48') TO ('64');

--
-- Name: work_outbox_p4; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox ATTACH PARTITION public.work_outbox_p4 FOR VALUES FROM ('64') TO ('80');

--
-- Name: work_outbox_p5; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox ATTACH PARTITION public.work_outbox_p5 FOR VALUES FROM ('80') TO ('96');

--
-- Name: work_outbox_p6; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox ATTACH PARTITION public.work_outbox_p6 FOR VALUES FROM ('96') TO ('112');

--
-- Name: work_outbox_p7; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox ATTACH PARTITION public.work_outbox_p7 FOR VALUES FROM ('112') TO ('128');

--
-- Name: work_outbox_p8; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox ATTACH PARTITION public.work_outbox_p8 FOR VALUES FROM ('128') TO ('144');

--
-- Name: work_outbox_p9; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox ATTACH PARTITION public.work_outbox_p9 FOR VALUES FROM ('144') TO ('160');

--
-- Name: work_queue_p0; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue ATTACH PARTITION public.work_queue_p0 FOR VALUES FROM ('0') TO ('16');

--
-- Name: work_queue_p1; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue ATTACH PARTITION public.work_queue_p1 FOR VALUES FROM ('16') TO ('32');

--
-- Name: work_queue_p10; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue ATTACH PARTITION public.work_queue_p10 FOR VALUES FROM ('160') TO ('176');

--
-- Name: work_queue_p11; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue ATTACH PARTITION public.work_queue_p11 FOR VALUES FROM ('176') TO ('192');

--
-- Name: work_queue_p12; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue ATTACH PARTITION public.work_queue_p12 FOR VALUES FROM ('192') TO ('208');

--
-- Name: work_queue_p13; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue ATTACH PARTITION public.work_queue_p13 FOR VALUES FROM ('208') TO ('224');

--
-- Name: work_queue_p14; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue ATTACH PARTITION public.work_queue_p14 FOR VALUES FROM ('224') TO ('240');

--
-- Name: work_queue_p15; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue ATTACH PARTITION public.work_queue_p15 FOR VALUES FROM ('240') TO ('256');

--
-- Name: work_queue_p2; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue ATTACH PARTITION public.work_queue_p2 FOR VALUES FROM ('32') TO ('48');

--
-- Name: work_queue_p3; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue ATTACH PARTITION public.work_queue_p3 FOR VALUES FROM ('48') TO ('64');

--
-- Name: work_queue_p4; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue ATTACH PARTITION public.work_queue_p4 FOR VALUES FROM ('64') TO ('80');

--
-- Name: work_queue_p5; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue ATTACH PARTITION public.work_queue_p5 FOR VALUES FROM ('80') TO ('96');

--
-- Name: work_queue_p6; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue ATTACH PARTITION public.work_queue_p6 FOR VALUES FROM ('96') TO ('112');

--
-- Name: work_queue_p7; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue ATTACH PARTITION public.work_queue_p7 FOR VALUES FROM ('112') TO ('128');

--
-- Name: work_queue_p8; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue ATTACH PARTITION public.work_queue_p8 FOR VALUES FROM ('128') TO ('144');

--
-- Name: work_queue_p9; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue ATTACH PARTITION public.work_queue_p9 FOR VALUES FROM ('144') TO ('160');

--
-- Name: resource_events id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_events ALTER COLUMN id SET DEFAULT nextval('public.resource_events_id_seq'::regclass);

--
-- Name: cluster_members cluster_members_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cluster_members
    ADD CONSTRAINT cluster_members_pkey PRIMARY KEY (member_id);

--

--
-- Name: kind_config kind_config_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.kind_config
    ADD CONSTRAINT kind_config_pkey PRIMARY KEY (kind, kind_version);

--
-- Name: kind_inflight kind_inflight_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.kind_inflight
    ADD CONSTRAINT kind_inflight_pkey PRIMARY KEY (kind, kind_version, range_lo);

--
-- Name: kind_manifest kind_manifest_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.kind_manifest
    ADD CONSTRAINT kind_manifest_pkey PRIMARY KEY (kind, kind_version);

--
-- Name: lifecycle_outbox lifecycle_outbox_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox
    ADD CONSTRAINT lifecycle_outbox_pkey PRIMARY KEY (resource_id, transition, generation, binding_name, shard_id);

--
-- Name: lifecycle_outbox_p0 lifecycle_outbox_p0_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox_p0
    ADD CONSTRAINT lifecycle_outbox_p0_pkey PRIMARY KEY (resource_id, transition, generation, binding_name, shard_id);

--
-- Name: lifecycle_outbox_p10 lifecycle_outbox_p10_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox_p10
    ADD CONSTRAINT lifecycle_outbox_p10_pkey PRIMARY KEY (resource_id, transition, generation, binding_name, shard_id);

--
-- Name: lifecycle_outbox_p11 lifecycle_outbox_p11_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox_p11
    ADD CONSTRAINT lifecycle_outbox_p11_pkey PRIMARY KEY (resource_id, transition, generation, binding_name, shard_id);

--
-- Name: lifecycle_outbox_p12 lifecycle_outbox_p12_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox_p12
    ADD CONSTRAINT lifecycle_outbox_p12_pkey PRIMARY KEY (resource_id, transition, generation, binding_name, shard_id);

--
-- Name: lifecycle_outbox_p13 lifecycle_outbox_p13_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox_p13
    ADD CONSTRAINT lifecycle_outbox_p13_pkey PRIMARY KEY (resource_id, transition, generation, binding_name, shard_id);

--
-- Name: lifecycle_outbox_p14 lifecycle_outbox_p14_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox_p14
    ADD CONSTRAINT lifecycle_outbox_p14_pkey PRIMARY KEY (resource_id, transition, generation, binding_name, shard_id);

--
-- Name: lifecycle_outbox_p15 lifecycle_outbox_p15_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox_p15
    ADD CONSTRAINT lifecycle_outbox_p15_pkey PRIMARY KEY (resource_id, transition, generation, binding_name, shard_id);

--
-- Name: lifecycle_outbox_p1 lifecycle_outbox_p1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox_p1
    ADD CONSTRAINT lifecycle_outbox_p1_pkey PRIMARY KEY (resource_id, transition, generation, binding_name, shard_id);

--
-- Name: lifecycle_outbox_p2 lifecycle_outbox_p2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox_p2
    ADD CONSTRAINT lifecycle_outbox_p2_pkey PRIMARY KEY (resource_id, transition, generation, binding_name, shard_id);

--
-- Name: lifecycle_outbox_p3 lifecycle_outbox_p3_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox_p3
    ADD CONSTRAINT lifecycle_outbox_p3_pkey PRIMARY KEY (resource_id, transition, generation, binding_name, shard_id);

--
-- Name: lifecycle_outbox_p4 lifecycle_outbox_p4_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox_p4
    ADD CONSTRAINT lifecycle_outbox_p4_pkey PRIMARY KEY (resource_id, transition, generation, binding_name, shard_id);

--
-- Name: lifecycle_outbox_p5 lifecycle_outbox_p5_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox_p5
    ADD CONSTRAINT lifecycle_outbox_p5_pkey PRIMARY KEY (resource_id, transition, generation, binding_name, shard_id);

--
-- Name: lifecycle_outbox_p6 lifecycle_outbox_p6_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox_p6
    ADD CONSTRAINT lifecycle_outbox_p6_pkey PRIMARY KEY (resource_id, transition, generation, binding_name, shard_id);

--
-- Name: lifecycle_outbox_p7 lifecycle_outbox_p7_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox_p7
    ADD CONSTRAINT lifecycle_outbox_p7_pkey PRIMARY KEY (resource_id, transition, generation, binding_name, shard_id);

--
-- Name: lifecycle_outbox_p8 lifecycle_outbox_p8_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox_p8
    ADD CONSTRAINT lifecycle_outbox_p8_pkey PRIMARY KEY (resource_id, transition, generation, binding_name, shard_id);

--
-- Name: lifecycle_outbox_p9 lifecycle_outbox_p9_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lifecycle_outbox_p9
    ADD CONSTRAINT lifecycle_outbox_p9_pkey PRIMARY KEY (resource_id, transition, generation, binding_name, shard_id);

--
-- Name: providerconfigs providerconfigs_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.providerconfigs
    ADD CONSTRAINT providerconfigs_name_key UNIQUE (name);

--
-- Name: providerconfigs providerconfigs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.providerconfigs
    ADD CONSTRAINT providerconfigs_pkey PRIMARY KEY (id);

--
-- Name: reactor_bindings reactor_bindings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.reactor_bindings
    ADD CONSTRAINT reactor_bindings_pkey PRIMARY KEY (name);

--
-- Name: resource_conditions resource_conditions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_conditions
    ADD CONSTRAINT resource_conditions_pkey PRIMARY KEY (resource_id, type);

--
-- Name: resource_deps resource_deps_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_deps
    ADD CONSTRAINT resource_deps_pkey PRIMARY KEY (dependent_id, dependency_id);

--
-- Name: resource_events resource_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_events
    ADD CONSTRAINT resource_events_pkey PRIMARY KEY (id);

--
-- Name: resource_meta resource_meta_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_meta
    ADD CONSTRAINT resource_meta_pkey PRIMARY KEY (id);

--
-- Name: resource_operations resource_operations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_operations
    ADD CONSTRAINT resource_operations_pkey PRIMARY KEY (id);

--
-- Name: resources resources_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_pkey PRIMARY KEY (id);

--
-- Name: rollup_recheck rollup_recheck_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.rollup_recheck
    ADD CONSTRAINT rollup_recheck_pkey PRIMARY KEY (resource_id);

--
-- Name: schedule_recheck schedule_recheck_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schedule_recheck
    ADD CONSTRAINT schedule_recheck_pkey PRIMARY KEY (resource_id);

--
-- Name: spec_versions spec_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.spec_versions
    ADD CONSTRAINT spec_versions_pkey PRIMARY KEY (resource_id, generation);

--
-- Name: wake_state wake_state_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wake_state
    ADD CONSTRAINT wake_state_pkey PRIMARY KEY (channel);

--
-- Name: work_outbox work_outbox_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox
    ADD CONSTRAINT work_outbox_pkey PRIMARY KEY (work_id, shard_id);

--
-- Name: work_outbox_p0 work_outbox_p0_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox_p0
    ADD CONSTRAINT work_outbox_p0_pkey PRIMARY KEY (work_id, shard_id);

--
-- Name: work_outbox_p10 work_outbox_p10_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox_p10
    ADD CONSTRAINT work_outbox_p10_pkey PRIMARY KEY (work_id, shard_id);

--
-- Name: work_outbox_p11 work_outbox_p11_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox_p11
    ADD CONSTRAINT work_outbox_p11_pkey PRIMARY KEY (work_id, shard_id);

--
-- Name: work_outbox_p12 work_outbox_p12_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox_p12
    ADD CONSTRAINT work_outbox_p12_pkey PRIMARY KEY (work_id, shard_id);

--
-- Name: work_outbox_p13 work_outbox_p13_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox_p13
    ADD CONSTRAINT work_outbox_p13_pkey PRIMARY KEY (work_id, shard_id);

--
-- Name: work_outbox_p14 work_outbox_p14_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox_p14
    ADD CONSTRAINT work_outbox_p14_pkey PRIMARY KEY (work_id, shard_id);

--
-- Name: work_outbox_p15 work_outbox_p15_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox_p15
    ADD CONSTRAINT work_outbox_p15_pkey PRIMARY KEY (work_id, shard_id);

--
-- Name: work_outbox_p1 work_outbox_p1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox_p1
    ADD CONSTRAINT work_outbox_p1_pkey PRIMARY KEY (work_id, shard_id);

--
-- Name: work_outbox_p2 work_outbox_p2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox_p2
    ADD CONSTRAINT work_outbox_p2_pkey PRIMARY KEY (work_id, shard_id);

--
-- Name: work_outbox_p3 work_outbox_p3_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox_p3
    ADD CONSTRAINT work_outbox_p3_pkey PRIMARY KEY (work_id, shard_id);

--
-- Name: work_outbox_p4 work_outbox_p4_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox_p4
    ADD CONSTRAINT work_outbox_p4_pkey PRIMARY KEY (work_id, shard_id);

--
-- Name: work_outbox_p5 work_outbox_p5_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox_p5
    ADD CONSTRAINT work_outbox_p5_pkey PRIMARY KEY (work_id, shard_id);

--
-- Name: work_outbox_p6 work_outbox_p6_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox_p6
    ADD CONSTRAINT work_outbox_p6_pkey PRIMARY KEY (work_id, shard_id);

--
-- Name: work_outbox_p7 work_outbox_p7_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox_p7
    ADD CONSTRAINT work_outbox_p7_pkey PRIMARY KEY (work_id, shard_id);

--
-- Name: work_outbox_p8 work_outbox_p8_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox_p8
    ADD CONSTRAINT work_outbox_p8_pkey PRIMARY KEY (work_id, shard_id);

--
-- Name: work_outbox_p9 work_outbox_p9_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_outbox_p9
    ADD CONSTRAINT work_outbox_p9_pkey PRIMARY KEY (work_id, shard_id);

--
-- Name: work_queue work_queue_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue
    ADD CONSTRAINT work_queue_pkey PRIMARY KEY (id, shard_id);

--
-- Name: work_queue_p0 work_queue_p0_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p0
    ADD CONSTRAINT work_queue_p0_pkey PRIMARY KEY (id, shard_id);

--
-- Name: work_queue work_queue_resource_id_task_type_shard_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue
    ADD CONSTRAINT work_queue_resource_id_task_type_shard_id_key UNIQUE (resource_id, task_type, shard_id);

--
-- Name: work_queue_p0 work_queue_p0_resource_id_task_type_shard_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p0
    ADD CONSTRAINT work_queue_p0_resource_id_task_type_shard_id_key UNIQUE (resource_id, task_type, shard_id);

--
-- Name: work_queue_p10 work_queue_p10_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p10
    ADD CONSTRAINT work_queue_p10_pkey PRIMARY KEY (id, shard_id);

--
-- Name: work_queue_p10 work_queue_p10_resource_id_task_type_shard_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p10
    ADD CONSTRAINT work_queue_p10_resource_id_task_type_shard_id_key UNIQUE (resource_id, task_type, shard_id);

--
-- Name: work_queue_p11 work_queue_p11_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p11
    ADD CONSTRAINT work_queue_p11_pkey PRIMARY KEY (id, shard_id);

--
-- Name: work_queue_p11 work_queue_p11_resource_id_task_type_shard_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p11
    ADD CONSTRAINT work_queue_p11_resource_id_task_type_shard_id_key UNIQUE (resource_id, task_type, shard_id);

--
-- Name: work_queue_p12 work_queue_p12_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p12
    ADD CONSTRAINT work_queue_p12_pkey PRIMARY KEY (id, shard_id);

--
-- Name: work_queue_p12 work_queue_p12_resource_id_task_type_shard_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p12
    ADD CONSTRAINT work_queue_p12_resource_id_task_type_shard_id_key UNIQUE (resource_id, task_type, shard_id);

--
-- Name: work_queue_p13 work_queue_p13_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p13
    ADD CONSTRAINT work_queue_p13_pkey PRIMARY KEY (id, shard_id);

--
-- Name: work_queue_p13 work_queue_p13_resource_id_task_type_shard_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p13
    ADD CONSTRAINT work_queue_p13_resource_id_task_type_shard_id_key UNIQUE (resource_id, task_type, shard_id);

--
-- Name: work_queue_p14 work_queue_p14_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p14
    ADD CONSTRAINT work_queue_p14_pkey PRIMARY KEY (id, shard_id);

--
-- Name: work_queue_p14 work_queue_p14_resource_id_task_type_shard_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p14
    ADD CONSTRAINT work_queue_p14_resource_id_task_type_shard_id_key UNIQUE (resource_id, task_type, shard_id);

--
-- Name: work_queue_p15 work_queue_p15_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p15
    ADD CONSTRAINT work_queue_p15_pkey PRIMARY KEY (id, shard_id);

--
-- Name: work_queue_p15 work_queue_p15_resource_id_task_type_shard_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p15
    ADD CONSTRAINT work_queue_p15_resource_id_task_type_shard_id_key UNIQUE (resource_id, task_type, shard_id);

--
-- Name: work_queue_p1 work_queue_p1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p1
    ADD CONSTRAINT work_queue_p1_pkey PRIMARY KEY (id, shard_id);

--
-- Name: work_queue_p1 work_queue_p1_resource_id_task_type_shard_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p1
    ADD CONSTRAINT work_queue_p1_resource_id_task_type_shard_id_key UNIQUE (resource_id, task_type, shard_id);

--
-- Name: work_queue_p2 work_queue_p2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p2
    ADD CONSTRAINT work_queue_p2_pkey PRIMARY KEY (id, shard_id);

--
-- Name: work_queue_p2 work_queue_p2_resource_id_task_type_shard_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p2
    ADD CONSTRAINT work_queue_p2_resource_id_task_type_shard_id_key UNIQUE (resource_id, task_type, shard_id);

--
-- Name: work_queue_p3 work_queue_p3_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p3
    ADD CONSTRAINT work_queue_p3_pkey PRIMARY KEY (id, shard_id);

--
-- Name: work_queue_p3 work_queue_p3_resource_id_task_type_shard_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p3
    ADD CONSTRAINT work_queue_p3_resource_id_task_type_shard_id_key UNIQUE (resource_id, task_type, shard_id);

--
-- Name: work_queue_p4 work_queue_p4_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p4
    ADD CONSTRAINT work_queue_p4_pkey PRIMARY KEY (id, shard_id);

--
-- Name: work_queue_p4 work_queue_p4_resource_id_task_type_shard_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p4
    ADD CONSTRAINT work_queue_p4_resource_id_task_type_shard_id_key UNIQUE (resource_id, task_type, shard_id);

--
-- Name: work_queue_p5 work_queue_p5_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p5
    ADD CONSTRAINT work_queue_p5_pkey PRIMARY KEY (id, shard_id);

--
-- Name: work_queue_p5 work_queue_p5_resource_id_task_type_shard_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p5
    ADD CONSTRAINT work_queue_p5_resource_id_task_type_shard_id_key UNIQUE (resource_id, task_type, shard_id);

--
-- Name: work_queue_p6 work_queue_p6_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p6
    ADD CONSTRAINT work_queue_p6_pkey PRIMARY KEY (id, shard_id);

--
-- Name: work_queue_p6 work_queue_p6_resource_id_task_type_shard_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p6
    ADD CONSTRAINT work_queue_p6_resource_id_task_type_shard_id_key UNIQUE (resource_id, task_type, shard_id);

--
-- Name: work_queue_p7 work_queue_p7_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p7
    ADD CONSTRAINT work_queue_p7_pkey PRIMARY KEY (id, shard_id);

--
-- Name: work_queue_p7 work_queue_p7_resource_id_task_type_shard_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p7
    ADD CONSTRAINT work_queue_p7_resource_id_task_type_shard_id_key UNIQUE (resource_id, task_type, shard_id);

--
-- Name: work_queue_p8 work_queue_p8_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p8
    ADD CONSTRAINT work_queue_p8_pkey PRIMARY KEY (id, shard_id);

--
-- Name: work_queue_p8 work_queue_p8_resource_id_task_type_shard_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p8
    ADD CONSTRAINT work_queue_p8_resource_id_task_type_shard_id_key UNIQUE (resource_id, task_type, shard_id);

--
-- Name: work_queue_p9 work_queue_p9_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p9
    ADD CONSTRAINT work_queue_p9_pkey PRIMARY KEY (id, shard_id);

--
-- Name: work_queue_p9 work_queue_p9_resource_id_task_type_shard_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_queue_p9
    ADD CONSTRAINT work_queue_p9_resource_id_task_type_shard_id_key UNIQUE (resource_id, task_type, shard_id);

--
-- Name: idx_lifecycle_outbox_claim; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_lifecycle_outbox_claim ON ONLY public.lifecycle_outbox USING btree (shard_id) WHERE (broker_id IS NULL);

--
-- Name: idx_lifecycle_outbox_heartbeat; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_lifecycle_outbox_heartbeat ON ONLY public.lifecycle_outbox USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: idx_providerconfigs_name_trgm; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_providerconfigs_name_trgm ON public.providerconfigs USING gin (name public.gin_trgm_ops);

--
-- Name: idx_providerconfigs_owner; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_providerconfigs_owner ON public.providerconfigs USING btree (owner_id) WHERE (owner_id IS NOT NULL);

--
-- Name: idx_reactor_bindings_match; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_reactor_bindings_match ON public.reactor_bindings USING btree (watch_kind, transition) WHERE enabled;

--
-- Name: idx_resource_deps_dependency; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_resource_deps_dependency ON public.resource_deps USING btree (dependency_id);

--
-- Name: idx_resource_deps_dependent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_resource_deps_dependent ON public.resource_deps USING btree (dependent_id);

--
-- Name: idx_resource_events_by_resource; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_resource_events_by_resource ON public.resource_events USING btree (resource_id, created_at DESC, id DESC);

--
-- Name: idx_resource_meta_labels; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_resource_meta_labels ON public.resource_meta USING gin (labels jsonb_path_ops);

--
-- Name: idx_resource_meta_name_trgm; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_resource_meta_name_trgm ON public.resource_meta USING gin (name public.gin_trgm_ops);

--
-- Name: idx_resource_operations_by_resource; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_resource_operations_by_resource ON public.resource_operations USING btree (resource_id, requested_at DESC);

--
-- Name: idx_resources_deleting; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_resources_deleting ON public.resources USING btree (shard_id) WHERE (deletion_requested_at IS NOT NULL);

--
-- Name: idx_resources_failed; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_resources_failed ON public.resources USING btree (created_at DESC, id DESC) WHERE (failure_gen = generation);

--
-- Name: idx_resources_frozen_sweep; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_resources_frozen_sweep ON public.resources USING btree (frozen_until) WHERE (frozen_until IS NOT NULL);

--
-- Name: idx_resources_kind_pagination; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_resources_kind_pagination ON public.resources USING btree (kind, created_at DESC, id DESC);

--
-- Name: idx_resources_owner; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_resources_owner ON public.resources USING btree (owner_id);

--
-- Name: idx_resources_provider_config; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_resources_provider_config ON public.resources USING btree (provider_config_id) WHERE (provider_config_id IS NOT NULL);

--
-- Name: idx_resources_resync; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_resources_resync ON public.resources USING btree (kind, last_reconciled_at) WHERE (last_reconciled_at IS NOT NULL);

--
-- Name: idx_resources_root_descendants; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_resources_root_descendants ON public.resources USING btree (root_id, created_at, id);

--
-- Name: idx_resources_root_lagging; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_resources_root_lagging ON public.resources USING btree (root_id) WHERE (synced_gen < generation);

--
-- Name: idx_resources_roots_pagination; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_resources_roots_pagination ON public.resources USING btree (created_at DESC, id DESC) WHERE (owner_id IS NULL);

--
-- Name: idx_resources_shard_lagging; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_resources_shard_lagging ON public.resources USING btree (shard_id) WHERE (synced_gen < generation);

--
-- Name: idx_rollup_recheck_shard; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_rollup_recheck_shard ON public.rollup_recheck USING btree (shard_id);

--
-- Name: idx_schedule_recheck_shard; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_schedule_recheck_shard ON public.schedule_recheck USING btree (shard_id);

--
-- Name: idx_spec_versions_history; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_spec_versions_history ON public.spec_versions USING btree (resource_id, generation DESC);

--
-- Name: idx_work_outbox_drain; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_work_outbox_drain ON ONLY public.work_outbox USING btree (shard_id, work_id);

--
-- Name: idx_work_queue_heartbeat; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_work_queue_heartbeat ON ONLY public.work_queue USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: idx_work_queue_leased; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_work_queue_leased ON ONLY public.work_queue USING btree (shard_id, kind) WHERE (broker_id IS NOT NULL);

--
-- Name: idx_work_queue_pending; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_work_queue_pending ON ONLY public.work_queue USING btree (kind, task_type, shard_id) WHERE (broker_id IS NULL);

--
-- Name: lifecycle_outbox_p0_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p0_heartbeat_at_idx ON public.lifecycle_outbox_p0 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: lifecycle_outbox_p0_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p0_shard_id_idx ON public.lifecycle_outbox_p0 USING btree (shard_id) WHERE (broker_id IS NULL);

--
-- Name: lifecycle_outbox_p10_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p10_heartbeat_at_idx ON public.lifecycle_outbox_p10 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: lifecycle_outbox_p10_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p10_shard_id_idx ON public.lifecycle_outbox_p10 USING btree (shard_id) WHERE (broker_id IS NULL);

--
-- Name: lifecycle_outbox_p11_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p11_heartbeat_at_idx ON public.lifecycle_outbox_p11 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: lifecycle_outbox_p11_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p11_shard_id_idx ON public.lifecycle_outbox_p11 USING btree (shard_id) WHERE (broker_id IS NULL);

--
-- Name: lifecycle_outbox_p12_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p12_heartbeat_at_idx ON public.lifecycle_outbox_p12 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: lifecycle_outbox_p12_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p12_shard_id_idx ON public.lifecycle_outbox_p12 USING btree (shard_id) WHERE (broker_id IS NULL);

--
-- Name: lifecycle_outbox_p13_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p13_heartbeat_at_idx ON public.lifecycle_outbox_p13 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: lifecycle_outbox_p13_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p13_shard_id_idx ON public.lifecycle_outbox_p13 USING btree (shard_id) WHERE (broker_id IS NULL);

--
-- Name: lifecycle_outbox_p14_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p14_heartbeat_at_idx ON public.lifecycle_outbox_p14 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: lifecycle_outbox_p14_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p14_shard_id_idx ON public.lifecycle_outbox_p14 USING btree (shard_id) WHERE (broker_id IS NULL);

--
-- Name: lifecycle_outbox_p15_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p15_heartbeat_at_idx ON public.lifecycle_outbox_p15 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: lifecycle_outbox_p15_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p15_shard_id_idx ON public.lifecycle_outbox_p15 USING btree (shard_id) WHERE (broker_id IS NULL);

--
-- Name: lifecycle_outbox_p1_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p1_heartbeat_at_idx ON public.lifecycle_outbox_p1 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: lifecycle_outbox_p1_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p1_shard_id_idx ON public.lifecycle_outbox_p1 USING btree (shard_id) WHERE (broker_id IS NULL);

--
-- Name: lifecycle_outbox_p2_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p2_heartbeat_at_idx ON public.lifecycle_outbox_p2 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: lifecycle_outbox_p2_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p2_shard_id_idx ON public.lifecycle_outbox_p2 USING btree (shard_id) WHERE (broker_id IS NULL);

--
-- Name: lifecycle_outbox_p3_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p3_heartbeat_at_idx ON public.lifecycle_outbox_p3 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: lifecycle_outbox_p3_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p3_shard_id_idx ON public.lifecycle_outbox_p3 USING btree (shard_id) WHERE (broker_id IS NULL);

--
-- Name: lifecycle_outbox_p4_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p4_heartbeat_at_idx ON public.lifecycle_outbox_p4 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: lifecycle_outbox_p4_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p4_shard_id_idx ON public.lifecycle_outbox_p4 USING btree (shard_id) WHERE (broker_id IS NULL);

--
-- Name: lifecycle_outbox_p5_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p5_heartbeat_at_idx ON public.lifecycle_outbox_p5 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: lifecycle_outbox_p5_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p5_shard_id_idx ON public.lifecycle_outbox_p5 USING btree (shard_id) WHERE (broker_id IS NULL);

--
-- Name: lifecycle_outbox_p6_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p6_heartbeat_at_idx ON public.lifecycle_outbox_p6 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: lifecycle_outbox_p6_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p6_shard_id_idx ON public.lifecycle_outbox_p6 USING btree (shard_id) WHERE (broker_id IS NULL);

--
-- Name: lifecycle_outbox_p7_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p7_heartbeat_at_idx ON public.lifecycle_outbox_p7 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: lifecycle_outbox_p7_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p7_shard_id_idx ON public.lifecycle_outbox_p7 USING btree (shard_id) WHERE (broker_id IS NULL);

--
-- Name: lifecycle_outbox_p8_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p8_heartbeat_at_idx ON public.lifecycle_outbox_p8 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: lifecycle_outbox_p8_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p8_shard_id_idx ON public.lifecycle_outbox_p8 USING btree (shard_id) WHERE (broker_id IS NULL);

--
-- Name: lifecycle_outbox_p9_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p9_heartbeat_at_idx ON public.lifecycle_outbox_p9 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: lifecycle_outbox_p9_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lifecycle_outbox_p9_shard_id_idx ON public.lifecycle_outbox_p9 USING btree (shard_id) WHERE (broker_id IS NULL);

--
-- Name: uniq_resource_meta; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uniq_resource_meta ON public.resource_meta USING btree (kind, name);

--
-- Name: uq_providerconfigs_default_per_kind; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_providerconfigs_default_per_kind ON public.providerconfigs USING btree (kind, kind_version) WHERE is_default;

--
-- Name: work_outbox_p0_shard_id_work_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_outbox_p0_shard_id_work_id_idx ON public.work_outbox_p0 USING btree (shard_id, work_id);

--
-- Name: work_outbox_p10_shard_id_work_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_outbox_p10_shard_id_work_id_idx ON public.work_outbox_p10 USING btree (shard_id, work_id);

--
-- Name: work_outbox_p11_shard_id_work_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_outbox_p11_shard_id_work_id_idx ON public.work_outbox_p11 USING btree (shard_id, work_id);

--
-- Name: work_outbox_p12_shard_id_work_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_outbox_p12_shard_id_work_id_idx ON public.work_outbox_p12 USING btree (shard_id, work_id);

--
-- Name: work_outbox_p13_shard_id_work_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_outbox_p13_shard_id_work_id_idx ON public.work_outbox_p13 USING btree (shard_id, work_id);

--
-- Name: work_outbox_p14_shard_id_work_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_outbox_p14_shard_id_work_id_idx ON public.work_outbox_p14 USING btree (shard_id, work_id);

--
-- Name: work_outbox_p15_shard_id_work_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_outbox_p15_shard_id_work_id_idx ON public.work_outbox_p15 USING btree (shard_id, work_id);

--
-- Name: work_outbox_p1_shard_id_work_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_outbox_p1_shard_id_work_id_idx ON public.work_outbox_p1 USING btree (shard_id, work_id);

--
-- Name: work_outbox_p2_shard_id_work_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_outbox_p2_shard_id_work_id_idx ON public.work_outbox_p2 USING btree (shard_id, work_id);

--
-- Name: work_outbox_p3_shard_id_work_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_outbox_p3_shard_id_work_id_idx ON public.work_outbox_p3 USING btree (shard_id, work_id);

--
-- Name: work_outbox_p4_shard_id_work_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_outbox_p4_shard_id_work_id_idx ON public.work_outbox_p4 USING btree (shard_id, work_id);

--
-- Name: work_outbox_p5_shard_id_work_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_outbox_p5_shard_id_work_id_idx ON public.work_outbox_p5 USING btree (shard_id, work_id);

--
-- Name: work_outbox_p6_shard_id_work_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_outbox_p6_shard_id_work_id_idx ON public.work_outbox_p6 USING btree (shard_id, work_id);

--
-- Name: work_outbox_p7_shard_id_work_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_outbox_p7_shard_id_work_id_idx ON public.work_outbox_p7 USING btree (shard_id, work_id);

--
-- Name: work_outbox_p8_shard_id_work_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_outbox_p8_shard_id_work_id_idx ON public.work_outbox_p8 USING btree (shard_id, work_id);

--
-- Name: work_outbox_p9_shard_id_work_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_outbox_p9_shard_id_work_id_idx ON public.work_outbox_p9 USING btree (shard_id, work_id);

--
-- Name: work_queue_p0_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p0_heartbeat_at_idx ON public.work_queue_p0 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p0_kind_task_type_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p0_kind_task_type_shard_id_idx ON public.work_queue_p0 USING btree (kind, task_type, shard_id) WHERE (broker_id IS NULL);

--
-- Name: work_queue_p0_shard_id_kind_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p0_shard_id_kind_idx ON public.work_queue_p0 USING btree (shard_id, kind) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p10_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p10_heartbeat_at_idx ON public.work_queue_p10 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p10_kind_task_type_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p10_kind_task_type_shard_id_idx ON public.work_queue_p10 USING btree (kind, task_type, shard_id) WHERE (broker_id IS NULL);

--
-- Name: work_queue_p10_shard_id_kind_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p10_shard_id_kind_idx ON public.work_queue_p10 USING btree (shard_id, kind) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p11_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p11_heartbeat_at_idx ON public.work_queue_p11 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p11_kind_task_type_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p11_kind_task_type_shard_id_idx ON public.work_queue_p11 USING btree (kind, task_type, shard_id) WHERE (broker_id IS NULL);

--
-- Name: work_queue_p11_shard_id_kind_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p11_shard_id_kind_idx ON public.work_queue_p11 USING btree (shard_id, kind) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p12_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p12_heartbeat_at_idx ON public.work_queue_p12 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p12_kind_task_type_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p12_kind_task_type_shard_id_idx ON public.work_queue_p12 USING btree (kind, task_type, shard_id) WHERE (broker_id IS NULL);

--
-- Name: work_queue_p12_shard_id_kind_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p12_shard_id_kind_idx ON public.work_queue_p12 USING btree (shard_id, kind) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p13_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p13_heartbeat_at_idx ON public.work_queue_p13 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p13_kind_task_type_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p13_kind_task_type_shard_id_idx ON public.work_queue_p13 USING btree (kind, task_type, shard_id) WHERE (broker_id IS NULL);

--
-- Name: work_queue_p13_shard_id_kind_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p13_shard_id_kind_idx ON public.work_queue_p13 USING btree (shard_id, kind) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p14_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p14_heartbeat_at_idx ON public.work_queue_p14 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p14_kind_task_type_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p14_kind_task_type_shard_id_idx ON public.work_queue_p14 USING btree (kind, task_type, shard_id) WHERE (broker_id IS NULL);

--
-- Name: work_queue_p14_shard_id_kind_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p14_shard_id_kind_idx ON public.work_queue_p14 USING btree (shard_id, kind) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p15_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p15_heartbeat_at_idx ON public.work_queue_p15 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p15_kind_task_type_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p15_kind_task_type_shard_id_idx ON public.work_queue_p15 USING btree (kind, task_type, shard_id) WHERE (broker_id IS NULL);

--
-- Name: work_queue_p15_shard_id_kind_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p15_shard_id_kind_idx ON public.work_queue_p15 USING btree (shard_id, kind) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p1_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p1_heartbeat_at_idx ON public.work_queue_p1 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p1_kind_task_type_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p1_kind_task_type_shard_id_idx ON public.work_queue_p1 USING btree (kind, task_type, shard_id) WHERE (broker_id IS NULL);

--
-- Name: work_queue_p1_shard_id_kind_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p1_shard_id_kind_idx ON public.work_queue_p1 USING btree (shard_id, kind) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p2_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p2_heartbeat_at_idx ON public.work_queue_p2 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p2_kind_task_type_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p2_kind_task_type_shard_id_idx ON public.work_queue_p2 USING btree (kind, task_type, shard_id) WHERE (broker_id IS NULL);

--
-- Name: work_queue_p2_shard_id_kind_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p2_shard_id_kind_idx ON public.work_queue_p2 USING btree (shard_id, kind) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p3_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p3_heartbeat_at_idx ON public.work_queue_p3 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p3_kind_task_type_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p3_kind_task_type_shard_id_idx ON public.work_queue_p3 USING btree (kind, task_type, shard_id) WHERE (broker_id IS NULL);

--
-- Name: work_queue_p3_shard_id_kind_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p3_shard_id_kind_idx ON public.work_queue_p3 USING btree (shard_id, kind) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p4_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p4_heartbeat_at_idx ON public.work_queue_p4 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p4_kind_task_type_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p4_kind_task_type_shard_id_idx ON public.work_queue_p4 USING btree (kind, task_type, shard_id) WHERE (broker_id IS NULL);

--
-- Name: work_queue_p4_shard_id_kind_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p4_shard_id_kind_idx ON public.work_queue_p4 USING btree (shard_id, kind) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p5_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p5_heartbeat_at_idx ON public.work_queue_p5 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p5_kind_task_type_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p5_kind_task_type_shard_id_idx ON public.work_queue_p5 USING btree (kind, task_type, shard_id) WHERE (broker_id IS NULL);

--
-- Name: work_queue_p5_shard_id_kind_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p5_shard_id_kind_idx ON public.work_queue_p5 USING btree (shard_id, kind) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p6_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p6_heartbeat_at_idx ON public.work_queue_p6 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p6_kind_task_type_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p6_kind_task_type_shard_id_idx ON public.work_queue_p6 USING btree (kind, task_type, shard_id) WHERE (broker_id IS NULL);

--
-- Name: work_queue_p6_shard_id_kind_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p6_shard_id_kind_idx ON public.work_queue_p6 USING btree (shard_id, kind) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p7_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p7_heartbeat_at_idx ON public.work_queue_p7 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p7_kind_task_type_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p7_kind_task_type_shard_id_idx ON public.work_queue_p7 USING btree (kind, task_type, shard_id) WHERE (broker_id IS NULL);

--
-- Name: work_queue_p7_shard_id_kind_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p7_shard_id_kind_idx ON public.work_queue_p7 USING btree (shard_id, kind) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p8_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p8_heartbeat_at_idx ON public.work_queue_p8 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p8_kind_task_type_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p8_kind_task_type_shard_id_idx ON public.work_queue_p8 USING btree (kind, task_type, shard_id) WHERE (broker_id IS NULL);

--
-- Name: work_queue_p8_shard_id_kind_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p8_shard_id_kind_idx ON public.work_queue_p8 USING btree (shard_id, kind) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p9_heartbeat_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p9_heartbeat_at_idx ON public.work_queue_p9 USING btree (heartbeat_at) WHERE (broker_id IS NOT NULL);

--
-- Name: work_queue_p9_kind_task_type_shard_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p9_kind_task_type_shard_id_idx ON public.work_queue_p9 USING btree (kind, task_type, shard_id) WHERE (broker_id IS NULL);

--
-- Name: work_queue_p9_shard_id_kind_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX work_queue_p9_shard_id_kind_idx ON public.work_queue_p9 USING btree (shard_id, kind) WHERE (broker_id IS NOT NULL);

--
-- Name: lifecycle_outbox_p0_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_heartbeat ATTACH PARTITION public.lifecycle_outbox_p0_heartbeat_at_idx;

--
-- Name: lifecycle_outbox_p0_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.lifecycle_outbox_pkey ATTACH PARTITION public.lifecycle_outbox_p0_pkey;

--
-- Name: lifecycle_outbox_p0_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_claim ATTACH PARTITION public.lifecycle_outbox_p0_shard_id_idx;

--
-- Name: lifecycle_outbox_p10_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_heartbeat ATTACH PARTITION public.lifecycle_outbox_p10_heartbeat_at_idx;

--
-- Name: lifecycle_outbox_p10_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.lifecycle_outbox_pkey ATTACH PARTITION public.lifecycle_outbox_p10_pkey;

--
-- Name: lifecycle_outbox_p10_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_claim ATTACH PARTITION public.lifecycle_outbox_p10_shard_id_idx;

--
-- Name: lifecycle_outbox_p11_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_heartbeat ATTACH PARTITION public.lifecycle_outbox_p11_heartbeat_at_idx;

--
-- Name: lifecycle_outbox_p11_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.lifecycle_outbox_pkey ATTACH PARTITION public.lifecycle_outbox_p11_pkey;

--
-- Name: lifecycle_outbox_p11_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_claim ATTACH PARTITION public.lifecycle_outbox_p11_shard_id_idx;

--
-- Name: lifecycle_outbox_p12_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_heartbeat ATTACH PARTITION public.lifecycle_outbox_p12_heartbeat_at_idx;

--
-- Name: lifecycle_outbox_p12_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.lifecycle_outbox_pkey ATTACH PARTITION public.lifecycle_outbox_p12_pkey;

--
-- Name: lifecycle_outbox_p12_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_claim ATTACH PARTITION public.lifecycle_outbox_p12_shard_id_idx;

--
-- Name: lifecycle_outbox_p13_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_heartbeat ATTACH PARTITION public.lifecycle_outbox_p13_heartbeat_at_idx;

--
-- Name: lifecycle_outbox_p13_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.lifecycle_outbox_pkey ATTACH PARTITION public.lifecycle_outbox_p13_pkey;

--
-- Name: lifecycle_outbox_p13_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_claim ATTACH PARTITION public.lifecycle_outbox_p13_shard_id_idx;

--
-- Name: lifecycle_outbox_p14_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_heartbeat ATTACH PARTITION public.lifecycle_outbox_p14_heartbeat_at_idx;

--
-- Name: lifecycle_outbox_p14_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.lifecycle_outbox_pkey ATTACH PARTITION public.lifecycle_outbox_p14_pkey;

--
-- Name: lifecycle_outbox_p14_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_claim ATTACH PARTITION public.lifecycle_outbox_p14_shard_id_idx;

--
-- Name: lifecycle_outbox_p15_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_heartbeat ATTACH PARTITION public.lifecycle_outbox_p15_heartbeat_at_idx;

--
-- Name: lifecycle_outbox_p15_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.lifecycle_outbox_pkey ATTACH PARTITION public.lifecycle_outbox_p15_pkey;

--
-- Name: lifecycle_outbox_p15_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_claim ATTACH PARTITION public.lifecycle_outbox_p15_shard_id_idx;

--
-- Name: lifecycle_outbox_p1_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_heartbeat ATTACH PARTITION public.lifecycle_outbox_p1_heartbeat_at_idx;

--
-- Name: lifecycle_outbox_p1_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.lifecycle_outbox_pkey ATTACH PARTITION public.lifecycle_outbox_p1_pkey;

--
-- Name: lifecycle_outbox_p1_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_claim ATTACH PARTITION public.lifecycle_outbox_p1_shard_id_idx;

--
-- Name: lifecycle_outbox_p2_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_heartbeat ATTACH PARTITION public.lifecycle_outbox_p2_heartbeat_at_idx;

--
-- Name: lifecycle_outbox_p2_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.lifecycle_outbox_pkey ATTACH PARTITION public.lifecycle_outbox_p2_pkey;

--
-- Name: lifecycle_outbox_p2_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_claim ATTACH PARTITION public.lifecycle_outbox_p2_shard_id_idx;

--
-- Name: lifecycle_outbox_p3_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_heartbeat ATTACH PARTITION public.lifecycle_outbox_p3_heartbeat_at_idx;

--
-- Name: lifecycle_outbox_p3_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.lifecycle_outbox_pkey ATTACH PARTITION public.lifecycle_outbox_p3_pkey;

--
-- Name: lifecycle_outbox_p3_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_claim ATTACH PARTITION public.lifecycle_outbox_p3_shard_id_idx;

--
-- Name: lifecycle_outbox_p4_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_heartbeat ATTACH PARTITION public.lifecycle_outbox_p4_heartbeat_at_idx;

--
-- Name: lifecycle_outbox_p4_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.lifecycle_outbox_pkey ATTACH PARTITION public.lifecycle_outbox_p4_pkey;

--
-- Name: lifecycle_outbox_p4_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_claim ATTACH PARTITION public.lifecycle_outbox_p4_shard_id_idx;

--
-- Name: lifecycle_outbox_p5_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_heartbeat ATTACH PARTITION public.lifecycle_outbox_p5_heartbeat_at_idx;

--
-- Name: lifecycle_outbox_p5_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.lifecycle_outbox_pkey ATTACH PARTITION public.lifecycle_outbox_p5_pkey;

--
-- Name: lifecycle_outbox_p5_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_claim ATTACH PARTITION public.lifecycle_outbox_p5_shard_id_idx;

--
-- Name: lifecycle_outbox_p6_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_heartbeat ATTACH PARTITION public.lifecycle_outbox_p6_heartbeat_at_idx;

--
-- Name: lifecycle_outbox_p6_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.lifecycle_outbox_pkey ATTACH PARTITION public.lifecycle_outbox_p6_pkey;

--
-- Name: lifecycle_outbox_p6_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_claim ATTACH PARTITION public.lifecycle_outbox_p6_shard_id_idx;

--
-- Name: lifecycle_outbox_p7_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_heartbeat ATTACH PARTITION public.lifecycle_outbox_p7_heartbeat_at_idx;

--
-- Name: lifecycle_outbox_p7_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.lifecycle_outbox_pkey ATTACH PARTITION public.lifecycle_outbox_p7_pkey;

--
-- Name: lifecycle_outbox_p7_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_claim ATTACH PARTITION public.lifecycle_outbox_p7_shard_id_idx;

--
-- Name: lifecycle_outbox_p8_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_heartbeat ATTACH PARTITION public.lifecycle_outbox_p8_heartbeat_at_idx;

--
-- Name: lifecycle_outbox_p8_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.lifecycle_outbox_pkey ATTACH PARTITION public.lifecycle_outbox_p8_pkey;

--
-- Name: lifecycle_outbox_p8_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_claim ATTACH PARTITION public.lifecycle_outbox_p8_shard_id_idx;

--
-- Name: lifecycle_outbox_p9_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_heartbeat ATTACH PARTITION public.lifecycle_outbox_p9_heartbeat_at_idx;

--
-- Name: lifecycle_outbox_p9_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.lifecycle_outbox_pkey ATTACH PARTITION public.lifecycle_outbox_p9_pkey;

--
-- Name: lifecycle_outbox_p9_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_lifecycle_outbox_claim ATTACH PARTITION public.lifecycle_outbox_p9_shard_id_idx;

--
-- Name: work_outbox_p0_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_outbox_pkey ATTACH PARTITION public.work_outbox_p0_pkey;

--
-- Name: work_outbox_p0_shard_id_work_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_outbox_drain ATTACH PARTITION public.work_outbox_p0_shard_id_work_id_idx;

--
-- Name: work_outbox_p10_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_outbox_pkey ATTACH PARTITION public.work_outbox_p10_pkey;

--
-- Name: work_outbox_p10_shard_id_work_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_outbox_drain ATTACH PARTITION public.work_outbox_p10_shard_id_work_id_idx;

--
-- Name: work_outbox_p11_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_outbox_pkey ATTACH PARTITION public.work_outbox_p11_pkey;

--
-- Name: work_outbox_p11_shard_id_work_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_outbox_drain ATTACH PARTITION public.work_outbox_p11_shard_id_work_id_idx;

--
-- Name: work_outbox_p12_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_outbox_pkey ATTACH PARTITION public.work_outbox_p12_pkey;

--
-- Name: work_outbox_p12_shard_id_work_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_outbox_drain ATTACH PARTITION public.work_outbox_p12_shard_id_work_id_idx;

--
-- Name: work_outbox_p13_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_outbox_pkey ATTACH PARTITION public.work_outbox_p13_pkey;

--
-- Name: work_outbox_p13_shard_id_work_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_outbox_drain ATTACH PARTITION public.work_outbox_p13_shard_id_work_id_idx;

--
-- Name: work_outbox_p14_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_outbox_pkey ATTACH PARTITION public.work_outbox_p14_pkey;

--
-- Name: work_outbox_p14_shard_id_work_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_outbox_drain ATTACH PARTITION public.work_outbox_p14_shard_id_work_id_idx;

--
-- Name: work_outbox_p15_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_outbox_pkey ATTACH PARTITION public.work_outbox_p15_pkey;

--
-- Name: work_outbox_p15_shard_id_work_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_outbox_drain ATTACH PARTITION public.work_outbox_p15_shard_id_work_id_idx;

--
-- Name: work_outbox_p1_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_outbox_pkey ATTACH PARTITION public.work_outbox_p1_pkey;

--
-- Name: work_outbox_p1_shard_id_work_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_outbox_drain ATTACH PARTITION public.work_outbox_p1_shard_id_work_id_idx;

--
-- Name: work_outbox_p2_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_outbox_pkey ATTACH PARTITION public.work_outbox_p2_pkey;

--
-- Name: work_outbox_p2_shard_id_work_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_outbox_drain ATTACH PARTITION public.work_outbox_p2_shard_id_work_id_idx;

--
-- Name: work_outbox_p3_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_outbox_pkey ATTACH PARTITION public.work_outbox_p3_pkey;

--
-- Name: work_outbox_p3_shard_id_work_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_outbox_drain ATTACH PARTITION public.work_outbox_p3_shard_id_work_id_idx;

--
-- Name: work_outbox_p4_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_outbox_pkey ATTACH PARTITION public.work_outbox_p4_pkey;

--
-- Name: work_outbox_p4_shard_id_work_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_outbox_drain ATTACH PARTITION public.work_outbox_p4_shard_id_work_id_idx;

--
-- Name: work_outbox_p5_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_outbox_pkey ATTACH PARTITION public.work_outbox_p5_pkey;

--
-- Name: work_outbox_p5_shard_id_work_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_outbox_drain ATTACH PARTITION public.work_outbox_p5_shard_id_work_id_idx;

--
-- Name: work_outbox_p6_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_outbox_pkey ATTACH PARTITION public.work_outbox_p6_pkey;

--
-- Name: work_outbox_p6_shard_id_work_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_outbox_drain ATTACH PARTITION public.work_outbox_p6_shard_id_work_id_idx;

--
-- Name: work_outbox_p7_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_outbox_pkey ATTACH PARTITION public.work_outbox_p7_pkey;

--
-- Name: work_outbox_p7_shard_id_work_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_outbox_drain ATTACH PARTITION public.work_outbox_p7_shard_id_work_id_idx;

--
-- Name: work_outbox_p8_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_outbox_pkey ATTACH PARTITION public.work_outbox_p8_pkey;

--
-- Name: work_outbox_p8_shard_id_work_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_outbox_drain ATTACH PARTITION public.work_outbox_p8_shard_id_work_id_idx;

--
-- Name: work_outbox_p9_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_outbox_pkey ATTACH PARTITION public.work_outbox_p9_pkey;

--
-- Name: work_outbox_p9_shard_id_work_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_outbox_drain ATTACH PARTITION public.work_outbox_p9_shard_id_work_id_idx;

--
-- Name: work_queue_p0_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_heartbeat ATTACH PARTITION public.work_queue_p0_heartbeat_at_idx;

--
-- Name: work_queue_p0_kind_task_type_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_pending ATTACH PARTITION public.work_queue_p0_kind_task_type_shard_id_idx;

--
-- Name: work_queue_p0_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_pkey ATTACH PARTITION public.work_queue_p0_pkey;

--
-- Name: work_queue_p0_resource_id_task_type_shard_id_key; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_resource_id_task_type_shard_id_key ATTACH PARTITION public.work_queue_p0_resource_id_task_type_shard_id_key;

--
-- Name: work_queue_p0_shard_id_kind_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_leased ATTACH PARTITION public.work_queue_p0_shard_id_kind_idx;

--
-- Name: work_queue_p10_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_heartbeat ATTACH PARTITION public.work_queue_p10_heartbeat_at_idx;

--
-- Name: work_queue_p10_kind_task_type_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_pending ATTACH PARTITION public.work_queue_p10_kind_task_type_shard_id_idx;

--
-- Name: work_queue_p10_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_pkey ATTACH PARTITION public.work_queue_p10_pkey;

--
-- Name: work_queue_p10_resource_id_task_type_shard_id_key; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_resource_id_task_type_shard_id_key ATTACH PARTITION public.work_queue_p10_resource_id_task_type_shard_id_key;

--
-- Name: work_queue_p10_shard_id_kind_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_leased ATTACH PARTITION public.work_queue_p10_shard_id_kind_idx;

--
-- Name: work_queue_p11_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_heartbeat ATTACH PARTITION public.work_queue_p11_heartbeat_at_idx;

--
-- Name: work_queue_p11_kind_task_type_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_pending ATTACH PARTITION public.work_queue_p11_kind_task_type_shard_id_idx;

--
-- Name: work_queue_p11_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_pkey ATTACH PARTITION public.work_queue_p11_pkey;

--
-- Name: work_queue_p11_resource_id_task_type_shard_id_key; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_resource_id_task_type_shard_id_key ATTACH PARTITION public.work_queue_p11_resource_id_task_type_shard_id_key;

--
-- Name: work_queue_p11_shard_id_kind_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_leased ATTACH PARTITION public.work_queue_p11_shard_id_kind_idx;

--
-- Name: work_queue_p12_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_heartbeat ATTACH PARTITION public.work_queue_p12_heartbeat_at_idx;

--
-- Name: work_queue_p12_kind_task_type_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_pending ATTACH PARTITION public.work_queue_p12_kind_task_type_shard_id_idx;

--
-- Name: work_queue_p12_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_pkey ATTACH PARTITION public.work_queue_p12_pkey;

--
-- Name: work_queue_p12_resource_id_task_type_shard_id_key; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_resource_id_task_type_shard_id_key ATTACH PARTITION public.work_queue_p12_resource_id_task_type_shard_id_key;

--
-- Name: work_queue_p12_shard_id_kind_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_leased ATTACH PARTITION public.work_queue_p12_shard_id_kind_idx;

--
-- Name: work_queue_p13_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_heartbeat ATTACH PARTITION public.work_queue_p13_heartbeat_at_idx;

--
-- Name: work_queue_p13_kind_task_type_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_pending ATTACH PARTITION public.work_queue_p13_kind_task_type_shard_id_idx;

--
-- Name: work_queue_p13_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_pkey ATTACH PARTITION public.work_queue_p13_pkey;

--
-- Name: work_queue_p13_resource_id_task_type_shard_id_key; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_resource_id_task_type_shard_id_key ATTACH PARTITION public.work_queue_p13_resource_id_task_type_shard_id_key;

--
-- Name: work_queue_p13_shard_id_kind_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_leased ATTACH PARTITION public.work_queue_p13_shard_id_kind_idx;

--
-- Name: work_queue_p14_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_heartbeat ATTACH PARTITION public.work_queue_p14_heartbeat_at_idx;

--
-- Name: work_queue_p14_kind_task_type_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_pending ATTACH PARTITION public.work_queue_p14_kind_task_type_shard_id_idx;

--
-- Name: work_queue_p14_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_pkey ATTACH PARTITION public.work_queue_p14_pkey;

--
-- Name: work_queue_p14_resource_id_task_type_shard_id_key; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_resource_id_task_type_shard_id_key ATTACH PARTITION public.work_queue_p14_resource_id_task_type_shard_id_key;

--
-- Name: work_queue_p14_shard_id_kind_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_leased ATTACH PARTITION public.work_queue_p14_shard_id_kind_idx;

--
-- Name: work_queue_p15_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_heartbeat ATTACH PARTITION public.work_queue_p15_heartbeat_at_idx;

--
-- Name: work_queue_p15_kind_task_type_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_pending ATTACH PARTITION public.work_queue_p15_kind_task_type_shard_id_idx;

--
-- Name: work_queue_p15_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_pkey ATTACH PARTITION public.work_queue_p15_pkey;

--
-- Name: work_queue_p15_resource_id_task_type_shard_id_key; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_resource_id_task_type_shard_id_key ATTACH PARTITION public.work_queue_p15_resource_id_task_type_shard_id_key;

--
-- Name: work_queue_p15_shard_id_kind_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_leased ATTACH PARTITION public.work_queue_p15_shard_id_kind_idx;

--
-- Name: work_queue_p1_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_heartbeat ATTACH PARTITION public.work_queue_p1_heartbeat_at_idx;

--
-- Name: work_queue_p1_kind_task_type_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_pending ATTACH PARTITION public.work_queue_p1_kind_task_type_shard_id_idx;

--
-- Name: work_queue_p1_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_pkey ATTACH PARTITION public.work_queue_p1_pkey;

--
-- Name: work_queue_p1_resource_id_task_type_shard_id_key; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_resource_id_task_type_shard_id_key ATTACH PARTITION public.work_queue_p1_resource_id_task_type_shard_id_key;

--
-- Name: work_queue_p1_shard_id_kind_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_leased ATTACH PARTITION public.work_queue_p1_shard_id_kind_idx;

--
-- Name: work_queue_p2_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_heartbeat ATTACH PARTITION public.work_queue_p2_heartbeat_at_idx;

--
-- Name: work_queue_p2_kind_task_type_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_pending ATTACH PARTITION public.work_queue_p2_kind_task_type_shard_id_idx;

--
-- Name: work_queue_p2_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_pkey ATTACH PARTITION public.work_queue_p2_pkey;

--
-- Name: work_queue_p2_resource_id_task_type_shard_id_key; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_resource_id_task_type_shard_id_key ATTACH PARTITION public.work_queue_p2_resource_id_task_type_shard_id_key;

--
-- Name: work_queue_p2_shard_id_kind_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_leased ATTACH PARTITION public.work_queue_p2_shard_id_kind_idx;

--
-- Name: work_queue_p3_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_heartbeat ATTACH PARTITION public.work_queue_p3_heartbeat_at_idx;

--
-- Name: work_queue_p3_kind_task_type_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_pending ATTACH PARTITION public.work_queue_p3_kind_task_type_shard_id_idx;

--
-- Name: work_queue_p3_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_pkey ATTACH PARTITION public.work_queue_p3_pkey;

--
-- Name: work_queue_p3_resource_id_task_type_shard_id_key; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_resource_id_task_type_shard_id_key ATTACH PARTITION public.work_queue_p3_resource_id_task_type_shard_id_key;

--
-- Name: work_queue_p3_shard_id_kind_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_leased ATTACH PARTITION public.work_queue_p3_shard_id_kind_idx;

--
-- Name: work_queue_p4_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_heartbeat ATTACH PARTITION public.work_queue_p4_heartbeat_at_idx;

--
-- Name: work_queue_p4_kind_task_type_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_pending ATTACH PARTITION public.work_queue_p4_kind_task_type_shard_id_idx;

--
-- Name: work_queue_p4_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_pkey ATTACH PARTITION public.work_queue_p4_pkey;

--
-- Name: work_queue_p4_resource_id_task_type_shard_id_key; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_resource_id_task_type_shard_id_key ATTACH PARTITION public.work_queue_p4_resource_id_task_type_shard_id_key;

--
-- Name: work_queue_p4_shard_id_kind_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_leased ATTACH PARTITION public.work_queue_p4_shard_id_kind_idx;

--
-- Name: work_queue_p5_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_heartbeat ATTACH PARTITION public.work_queue_p5_heartbeat_at_idx;

--
-- Name: work_queue_p5_kind_task_type_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_pending ATTACH PARTITION public.work_queue_p5_kind_task_type_shard_id_idx;

--
-- Name: work_queue_p5_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_pkey ATTACH PARTITION public.work_queue_p5_pkey;

--
-- Name: work_queue_p5_resource_id_task_type_shard_id_key; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_resource_id_task_type_shard_id_key ATTACH PARTITION public.work_queue_p5_resource_id_task_type_shard_id_key;

--
-- Name: work_queue_p5_shard_id_kind_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_leased ATTACH PARTITION public.work_queue_p5_shard_id_kind_idx;

--
-- Name: work_queue_p6_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_heartbeat ATTACH PARTITION public.work_queue_p6_heartbeat_at_idx;

--
-- Name: work_queue_p6_kind_task_type_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_pending ATTACH PARTITION public.work_queue_p6_kind_task_type_shard_id_idx;

--
-- Name: work_queue_p6_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_pkey ATTACH PARTITION public.work_queue_p6_pkey;

--
-- Name: work_queue_p6_resource_id_task_type_shard_id_key; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_resource_id_task_type_shard_id_key ATTACH PARTITION public.work_queue_p6_resource_id_task_type_shard_id_key;

--
-- Name: work_queue_p6_shard_id_kind_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_leased ATTACH PARTITION public.work_queue_p6_shard_id_kind_idx;

--
-- Name: work_queue_p7_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_heartbeat ATTACH PARTITION public.work_queue_p7_heartbeat_at_idx;

--
-- Name: work_queue_p7_kind_task_type_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_pending ATTACH PARTITION public.work_queue_p7_kind_task_type_shard_id_idx;

--
-- Name: work_queue_p7_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_pkey ATTACH PARTITION public.work_queue_p7_pkey;

--
-- Name: work_queue_p7_resource_id_task_type_shard_id_key; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_resource_id_task_type_shard_id_key ATTACH PARTITION public.work_queue_p7_resource_id_task_type_shard_id_key;

--
-- Name: work_queue_p7_shard_id_kind_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_leased ATTACH PARTITION public.work_queue_p7_shard_id_kind_idx;

--
-- Name: work_queue_p8_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_heartbeat ATTACH PARTITION public.work_queue_p8_heartbeat_at_idx;

--
-- Name: work_queue_p8_kind_task_type_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_pending ATTACH PARTITION public.work_queue_p8_kind_task_type_shard_id_idx;

--
-- Name: work_queue_p8_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_pkey ATTACH PARTITION public.work_queue_p8_pkey;

--
-- Name: work_queue_p8_resource_id_task_type_shard_id_key; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_resource_id_task_type_shard_id_key ATTACH PARTITION public.work_queue_p8_resource_id_task_type_shard_id_key;

--
-- Name: work_queue_p8_shard_id_kind_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_leased ATTACH PARTITION public.work_queue_p8_shard_id_kind_idx;

--
-- Name: work_queue_p9_heartbeat_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_heartbeat ATTACH PARTITION public.work_queue_p9_heartbeat_at_idx;

--
-- Name: work_queue_p9_kind_task_type_shard_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_pending ATTACH PARTITION public.work_queue_p9_kind_task_type_shard_id_idx;

--
-- Name: work_queue_p9_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_pkey ATTACH PARTITION public.work_queue_p9_pkey;

--
-- Name: work_queue_p9_resource_id_task_type_shard_id_key; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.work_queue_resource_id_task_type_shard_id_key ATTACH PARTITION public.work_queue_p9_resource_id_task_type_shard_id_key;

--
-- Name: work_queue_p9_shard_id_kind_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_work_queue_leased ATTACH PARTITION public.work_queue_p9_shard_id_kind_idx;

--
-- Name: resources cascade_ready_change; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER cascade_ready_change AFTER UPDATE ON public.resources REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.cascade_on_ready_change();

--
-- Name: resources cleanup_orphaned_deps; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER cleanup_orphaned_deps AFTER DELETE ON public.resources REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION public.cleanup_orphaned_deps();

--
-- Name: cluster_members cluster_members_changed; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER cluster_members_changed AFTER INSERT OR DELETE ON public.cluster_members FOR EACH ROW EXECUTE FUNCTION public.notify_cluster_changed();

--
-- Name: providerconfigs providerconfigs_changed; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER providerconfigs_changed AFTER INSERT OR DELETE OR UPDATE ON public.providerconfigs FOR EACH ROW EXECUTE FUNCTION public.notify_providerconfig_changed();

--
-- Name: kind_manifest trg_kind_manifest_notify_delete; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_kind_manifest_notify_delete AFTER DELETE ON public.kind_manifest FOR EACH ROW EXECUTE FUNCTION public.notify_kind_manifest_deleted();

--
-- Name: kind_manifest trg_kind_manifest_sync_config; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_kind_manifest_sync_config AFTER INSERT OR UPDATE ON public.kind_manifest FOR EACH ROW EXECUTE FUNCTION public.sync_kind_config_from_manifest();

--
-- Name: providerconfigs providerconfigs_owner_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.providerconfigs
    ADD CONSTRAINT providerconfigs_owner_fk FOREIGN KEY (owner_id) REFERENCES public.resources(id) ON DELETE CASCADE;

--
-- Name: resource_conditions resource_conditions_resource_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_conditions
    ADD CONSTRAINT resource_conditions_resource_id_fkey FOREIGN KEY (resource_id) REFERENCES public.resources(id) ON DELETE CASCADE;

--
-- Name: resource_events resource_events_resource_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_events
    ADD CONSTRAINT resource_events_resource_id_fkey FOREIGN KEY (resource_id) REFERENCES public.resources(id) ON DELETE CASCADE;

--
-- Name: resource_meta resource_meta_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_meta
    ADD CONSTRAINT resource_meta_id_fkey FOREIGN KEY (id) REFERENCES public.resources(id) ON DELETE CASCADE;

--
-- Name: resource_operations resource_operations_resource_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_operations
    ADD CONSTRAINT resource_operations_resource_id_fkey FOREIGN KEY (resource_id) REFERENCES public.resources(id) ON DELETE CASCADE;

--
-- Name: resources resources_owner_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_owner_id_fkey FOREIGN KEY (owner_id) REFERENCES public.resources(id) ON DELETE CASCADE;

--
-- Name: resources resources_provider_config_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_provider_config_id_fkey FOREIGN KEY (provider_config_id) REFERENCES public.providerconfigs(id) ON DELETE SET NULL;

--
--
