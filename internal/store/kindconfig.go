package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/model"
)

// ─────────────────────────────────────────────────────────────────────────
// Per-kind config (kind_config): the SINGLE operational-settings table — the
// global in-flight cap + the drift-resync policy. The SOURCE OF TRUTH, read
// directly with NO derived copy: the work_queue claim reads max_inflight from
// kind_config inline (capped ⇔ max_inflight > 0); the control plane reads the
// resync fields to build the Resyncer. The kind_manifest AFTER trigger derives
// these rows when a manifest is applied; operators UPSERT it at runtime. See
// kind_config in 00001_schema.sql. Off every hot path except the claim's PK probe.
// ─────────────────────────────────────────────────────────────────────────

// KindConfig is one kind's operational settings. MaxInflight == 0 means uncapped
// (the claim skips all cap logic); TaskDeadline == 0 means tasks run with no
// deadline; ResyncInterval == 0 means the kind opts out of drift detection;
// OrphanGracePeriod == 0 means a dropped child of this kind is pruned
// immediately (no grace window). FinalizerName is the kind's delete finalizer
// ("" = leaf kind, hard-deleted) — stored here so the in-DB orphan sweep can
// pick soft-vs-hard delete without a Go registry.
type KindConfig struct {
	Kind              model.Kind
	KindVersion       int
	MaxInflight       int
	TaskDeadline      time.Duration
	ResyncInterval    time.Duration
	ResyncRecomposes  bool
	OrphanGracePeriod time.Duration
	FinalizerName     string
	// MaxTransientAttempts caps consecutive transient reconcile failures before the
	// failure dead-letters (marked terminal). 0 = unbounded.
	MaxTransientAttempts int
	// Retired SUNSETS this web-API version: NEW creates/flips onto (kind, KindVersion)
	// are rejected (freeze-new), while EXISTING resources on it keep reconciling
	// (drain-existing). Operator-editable at runtime via UpsertKindConfig. Off the
	// claim hot path (the claim never reads it).
	Retired bool
}

// SeedKindConfig INSERT-IF-ABSENT writes one kind's settings. It never
// overwrites an existing row, so an operator's runtime edit survives a restart —
// the applied manifest is the SEED, the row is the truth.
func (s *Store) SeedKindConfig(ctx context.Context, k KindConfig) error {
	if k.KindVersion < 1 {
		return fmt.Errorf("seed kind_config %q: kind_version is required and must be >= 1 (got %d)", k.Kind, k.KindVersion)
	}
	return s.queries().SeedKindConfig(ctx, dbq.SeedKindConfigParams{
		Kind:                 string(k.Kind),
		KindVersion:          int32(k.KindVersion),
		MaxInflight:          int32(k.MaxInflight),
		TaskDeadlineSecs:     durSecs(k.TaskDeadline),
		ResyncIntervalSecs:   durSecs(k.ResyncInterval),
		ResyncRecomposes:     k.ResyncRecomposes,
		OrphanGraceSecs:      durSecs(k.OrphanGracePeriod),
		FinalizerName:        nilIfEmpty(k.FinalizerName),
		MaxTransientAttempts: int32(k.MaxTransientAttempts),
		Retired:              k.Retired,
	})
}

// UpsertKindConfig AUTHORITATIVELY writes one kind's settings — the OPERATOR
// edit path (vs SeedKindConfig's insert-if-absent boot seed). The cap change is
// LIVE: the claim reads kind_config directly, so the next claim sees the new
// ceiling with no restart and no reconcile.
func (s *Store) UpsertKindConfig(ctx context.Context, k KindConfig) error {
	if k.KindVersion < 1 {
		return fmt.Errorf("upsert kind_config %q: kind_version is required and must be >= 1 (got %d)", k.Kind, k.KindVersion)
	}
	return s.queries().UpsertKindConfig(ctx, dbq.UpsertKindConfigParams{
		Kind:                 string(k.Kind),
		KindVersion:          int32(k.KindVersion),
		MaxInflight:          int32(k.MaxInflight),
		TaskDeadlineSecs:     durSecs(k.TaskDeadline),
		ResyncIntervalSecs:   durSecs(k.ResyncInterval),
		ResyncRecomposes:     k.ResyncRecomposes,
		OrphanGraceSecs:      durSecs(k.OrphanGracePeriod),
		MaxTransientAttempts: int32(k.MaxTransientAttempts),
		// retired IS operator-editable — this is the runtime retire/un-retire path
		// (freeze-new / drain-existing), written authoritatively here.
		Retired: k.Retired,
		// finalizer_name is provider-declared, not operator-editable: a NULL here
		// is COALESCEd by the query to preserve the seeded value. The operator
		// edit path leaves k.FinalizerName == "" → nilIfEmpty → NULL → preserved.
		FinalizerName: nilIfEmpty(k.FinalizerName),
	})
}

// ListKindConfig returns every kind's settings — the set the control plane reads
// to build the Resyncer kind map. It dials no client. (The claim reads
// max_inflight from kind_config directly; nobody copies it anywhere.)
func (s *Store) ListKindConfig(ctx context.Context) ([]KindConfig, error) {
	rows, err := s.queries().ListKindConfig(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]KindConfig, len(rows))
	for i, r := range rows {
		out[i] = KindConfig{
			Kind:                 model.Kind(r.Kind),
			KindVersion:          int(r.KindVersion),
			MaxInflight:          int(r.MaxInflight),
			TaskDeadline:         secsToDur(r.TaskDeadlineSecs),
			ResyncInterval:       secsToDur(r.ResyncIntervalSecs),
			ResyncRecomposes:     r.ResyncRecomposes,
			OrphanGracePeriod:    secsToDur(r.OrphanGraceSecs),
			FinalizerName:        derefString(r.FinalizerName),
			MaxTransientAttempts: int(r.MaxTransientAttempts),
			Retired:              r.Retired,
		}
	}
	return out, nil
}

// ConfiguredKinds lists every kind that has a kind_config row (i.e. an applied
// manifest). The ProviderConfigCache's dynamic mode uses it to discover the live kind
// set to serve worker configs for, with no compiled-in provider list.
func (s *Store) ConfiguredKinds(ctx context.Context) ([]model.Kind, error) {
	rows, err := s.queries().ListKindConfig(ctx)
	if err != nil {
		return nil, err
	}
	// ListKindConfig returns one row per (kind, kindVersion); the ProviderConfigCache wants the
	// distinct KIND set (it serves worker config by kind), so dedup across kind versions.
	seen := make(map[model.Kind]struct{}, len(rows))
	out := make([]model.Kind, 0, len(rows))
	for _, r := range rows {
		k := model.Kind(r.Kind)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, k)
	}
	return out, nil
}

// ConfiguredKindVersions lists every (kind, kindVersion) that has a kind_config row (an
// applied manifest at that kindVersion). Defaults are now per-(kind, kindVersion), so the
// ProviderConfigCache's dynamic mode discovers the live PAIR set (not just the kind set)
// to serve a v2 worker vpc/v2's default and a v1 worker vpc/v1's — with no
// compiled-in list, so a CRD applied after boot at a new kindVersion is served with no
// restart. The row set is already distinct (kind_config PK is (kind, kindVersion)).
func (s *Store) ConfiguredKindVersions(ctx context.Context) ([]model.KindVersion, error) {
	rows, err := s.queries().ListConfiguredKindVersions(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]model.KindVersion, len(rows))
	for i, r := range rows {
		out[i] = model.KindVersion{Kind: model.Kind(r.Kind), Version: int(r.KindVersion)}
	}
	return out, nil
}

// GetKindConfig returns one (kind, kindVersion)'s settings by PK; found=false when the
// (kind, kindVersion) has no row (uncapped, no resync). The API's per-kind config panel
// reads this.
func (s *Store) GetKindConfig(ctx context.Context, kind model.Kind, kindVersion int) (KindConfig, bool, error) {
	r, err := s.queries().GetKindConfig(ctx, dbq.GetKindConfigParams{
		Kind:        string(kind),
		KindVersion: int32(kindVersion),
	})
	if err != nil {
		if isNoRows(err) {
			return KindConfig{}, false, nil
		}
		return KindConfig{}, false, err
	}
	return KindConfig{
		Kind:                 model.Kind(r.Kind),
		KindVersion:          int(r.KindVersion),
		MaxInflight:          int(r.MaxInflight),
		TaskDeadline:         secsToDur(r.TaskDeadlineSecs),
		ResyncInterval:       secsToDur(r.ResyncIntervalSecs),
		ResyncRecomposes:     r.ResyncRecomposes,
		OrphanGracePeriod:    secsToDur(r.OrphanGraceSecs),
		FinalizerName:        derefString(r.FinalizerName),
		MaxTransientAttempts: int(r.MaxTransientAttempts),
		Retired:              r.Retired,
	}, true, nil
}

// durSecs converts a duration to the non-negative whole seconds a kind_config
// duration column stores (task_deadline_secs, resync_interval_secs,
// orphan_grace_secs).
func durSecs(d time.Duration) int32 {
	secs := int32(d.Seconds())
	if secs < 0 {
		secs = 0
	}
	return secs
}

// secsToDur is durSecs' inverse: a kind_config seconds column → Duration. It
// clamps so an operator-supplied value can't wrap: a negative seconds value
// becomes 0 (no deadline/interval), and a value so large the `* time.Second`
// multiply would overflow int64 is capped at the max representable Duration
// (~292 years — effectively "no deadline"). Without the clamp a fat-fingered
// huge task_deadline_secs could overflow to a negative Duration, which downstream
// treats as ≤0 = UNBOUNDED — the opposite of the operator's intent.
func secsToDur(secs int32) time.Duration {
	if secs <= 0 {
		return 0
	}
	// int32 seconds × 1e9 ns fits int64 comfortably (max ~2.1e9 × 1e9 ≈ 2.1e18 <
	// 9.2e18), so the multiply can't overflow for any int32 — but go through
	// time.Duration explicitly so the intent (and the bound) is clear at the
	// call site.
	return time.Duration(secs) * time.Second
}

// RequeueFailedAndPending re-schedules aged failed/working-orphan rows
// whose dependencies are now ready, plus bootstrap pending rows the
// trigger missed.
func (s *Store) RequeueFailedAndPending(ctx context.Context, retryAfter time.Duration, limit int, shards []int16) (int, error) {
	retrySec := secsAtLeast1(retryAfter)
	n, err := s.queries().RequeueFailedAndPending(ctx, dbq.RequeueFailedAndPendingParams{
		Column1: retrySec,
		Column2: int32(limit),
		Column3: shards,
	})
	return int(n), err
}

// RequeueForResync re-pends settled resources of one kind whose last
// drift re-check is older than resyncAfter, WITHOUT bumping generation,
// so their provider re-observes live health. When recompose is true the
// re-pend also forces a composer re-run (drift-correcting composed
// children); see requeue_for_resync in 00001_schema.sql. Backs the
// per-kind drift sweeper.
func (s *Store) RequeueForResync(ctx context.Context, kind model.Kind, resyncAfter time.Duration, limit int, shards []int16, recompose bool) (int, error) {
	resyncSec := secsAtLeast1(resyncAfter)
	n, err := s.queries().RequeueForResync(ctx, dbq.RequeueForResyncParams{
		Column1: string(kind),
		Column2: resyncSec,
		Column3: int32(limit),
		Column4: shards,
		Column5: recompose,
	})
	return int(n), err
}

// GCSpecHistory reclaims superseded immutable spec bodies for the given
// shards: roots keep keepN revisions, children keep latest-1. Off the hot
// path (driven by the SpecGC sweeper). See gc_spec_history in the schema.
func (s *Store) GCSpecHistory(ctx context.Context, keepN, limit int, shards []int16) (int, error) {
	n, err := s.queries().GCSpecHistory(ctx, dbq.GCSpecHistoryParams{
		Column1: int32(keepN),
		Column2: int32(limit),
		Column3: shards,
	})
	return int(n), err
}

// SpecHistoryRow is one entry in a root's spec revision history (no body —
// the UI lazy-fetches a body via GetSpecRevision).
type SpecHistoryRow = dbq.ListSpecHistoryRow

// ListSpecHistory returns a root's spec revisions newest-first, bodies
// elided. Meaningful only for roots (children keep latest-1).
func (s *Store) ListSpecHistory(ctx context.Context, kind model.Kind, name string, limit int) ([]SpecHistoryRow, error) {
	if limit <= 0 {
		limit = 50
	}
	return s.queries().ListSpecHistory(ctx, dbq.ListSpecHistoryParams{
		Kind:  string(kind),
		Name:  name,
		Limit: int32(limit),
	})
}

// GetSpecRevision returns one historic spec body by (kind, name, generation) —
// the raw revision download and the checkout prefill source.
func (s *Store) GetSpecRevision(ctx context.Context, kind model.Kind, name string, generation int64) (json.RawMessage, error) {
	return s.queries().GetSpecRevision(ctx, dbq.GetSpecRevisionParams{
		Kind:       string(kind),
		Name:       name,
		Generation: generation,
	})
}

// RollbackSpec checks out the historic revision (by authored generation) as
// the live spec of resource id, returning the resource's generation after
// checkout (or 0 if (id, generation) doesn't exist). The SQL function copies
// the body inline, bumps generation, and re-schedules.
func (s *Store) RollbackSpec(ctx context.Context, id uuid.UUID, generation int64, actor string) (int64, error) {
	gen, err := s.queries().RollbackToSpec(ctx, dbq.RollbackToSpecParams{
		PID:        id,
		PTargetGen: generation,
		PActor:     actor,
	})
	if err != nil {
		return 0, err
	}
	if gen < 0 {
		return 0, nil // not found
	}
	// rollback_to_spec re-pends via schedule_eligible, which fires
	// pg_notify('work_ready') in the DB — no Go-side wake needed.
	return gen, nil
}
