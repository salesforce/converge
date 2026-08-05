package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/kindschema"
	"github.com/salesforce/converge/internal/model"
)

// kind_manifest is the kind definition (CRD): the operator-applied schemas +
// reactions + policy a worker's kind runs against.
// The store converts between the model.KindManifest domain type (reactions
// as typed []ReactionDecl) and the dbq row (reactions as JSONB), and computes
// the two content hashes:
//
//   - manifest_version: a content hash over (reactions + schemas), the in-flight
//     FENCE. An idempotent re-apply that changes nothing yields the SAME version,
//     so it does NOT invalidate in-flight work; only a real change bumps it.
//   - schema_hash: a hash over JUST the three JSON Schemas, the SCHEMA-STABILITY
//     gate — the API rejects a spec/status/config change to an existing kind
//     unless the operator explicitly accepts the new hash (Phase 6).

// UpsertKindManifest validates the manifest's reaction lattice (SDK side; the DB
// trigger re-validates), computes the content hashes, and writes the row. The
// two AFTER triggers derive kind_config + reactor_bindings. Returns the computed
// manifest_version so the caller can report it.
func (s *Store) UpsertKindManifest(ctx context.Context, m model.KindManifest) (int64, error) {
	if err := model.ValidateManifest(m); err != nil {
		return 0, fmt.Errorf("invalid manifest for %q: %w", m.Kind, err)
	}
	// kind_version is REQUIRED and explicit (>= 1): a publish must name its web-API
	// version — never a silent v1. The DB column is NOT NULL with no default and a
	// >= 1 CHECK; reject here so the caller gets a clear error, not a constraint code.
	if m.KindVersion < 1 {
		return 0, fmt.Errorf("kind %q: kind_version is required and must be >= 1 (got %d)", m.Kind, m.KindVersion)
	}
	reactions, err := json.Marshal(m.Reactions)
	if err != nil {
		return 0, fmt.Errorf("marshal reactions: %w", err)
	}
	if len(reactions) == 0 || string(reactions) == "null" {
		reactions = json.RawMessage("[]")
	}
	schemaHash := hashSchemas(m)
	version := manifestVersion(reactions, schemaHash)

	if err := s.queries().UpsertKindManifest(ctx, dbq.UpsertKindManifestParams{
		Kind:                 string(m.Kind),
		KindVersion:          int32(m.KindVersion),
		Description:          nilIfEmpty(m.Description),
		SpecSchema:           m.SpecSchema,
		StatusSchema:         m.StatusSchema,
		ConfigSchema:         m.ConfigSchema,
		Reactions:            reactions,
		FinalizerName:        nilIfEmpty(m.FinalizerName),
		MaxInflight:          int32(m.MaxInflight),
		TaskDeadlineSecs:     int32(m.TaskDeadlineSecs),
		ResyncIntervalSecs:   int32(m.ResyncIntervalSecs),
		ResyncRecomposes:     m.ResyncRecomposes,
		OrphanGraceSecs:      int32(m.OrphanGraceSecs),
		MaxTransientAttempts: int32(m.MaxTransientAttempts),
		Retired:              m.Retired,
		SchemaHash:           &schemaHash,
		ManifestVersion:      version,
	}); err != nil {
		return 0, fmt.Errorf("upsert kind_manifest %q: %w", m.Kind, err)
	}
	return version, nil
}

// GetKindManifest reads one (kind, kindVersion) manifest (nil, false if absent).
func (s *Store) GetKindManifest(ctx context.Context, kind model.Kind, kindVersion int) (model.KindManifest, bool, error) {
	row, err := s.queries().GetKindManifest(ctx, dbq.GetKindManifestParams{
		Kind:        string(kind),
		KindVersion: int32(kindVersion),
	})
	if err != nil {
		if isNoRows(err) {
			return model.KindManifest{}, false, nil
		}
		return model.KindManifest{}, false, fmt.Errorf("get kind_manifest %q/v%d: %w", kind, kindVersion, err)
	}
	m, err := manifestFromRow(row)
	return m, true, err
}

// DeleteKindManifest deletes the manifest (and its derived kind_config + its
// provider configs) for one (kind, kindVersion) — the operator retiring a version
// nothing references. It FAILS LOUD (ErrKindVersionInUse) if the version still has:
//   - any resource (INCLUDING soft-deleting/draining ones — a version tearing
//     anything down is not deletable), or
//   - a reactor binding that EXACTLY pins it (watch_kind_version = V or
//     reactor_version = V). Unpinned bindings resolve to the other versions and are
//     not counted.
//
// Provider configs at the (kind, kindVersion) are CASCADE-deleted (they only ever
// serve resources of their own (kind, kindVersion), so once no resource references
// the version they are dead). All checks + deletes run in ONE short transaction so
// a concurrent apply can't slip a resource in between the guard and the delete; the
// AFTER DELETE trigger's NOTIFY wakes every pod's KindManifestCache on commit.
//
// found=false (with a nil error) means the manifest was already gone — an
// idempotent no-op the API maps to 404. deletedConfigs is how many provider configs
// were cascaded.
func (s *Store) DeleteKindManifest(ctx context.Context, kind model.Kind, kindVersion int) (found bool, deletedConfigs int64, err error) {
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return false, 0, fmt.Errorf("store: underlying DBTX cannot begin a transaction")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return false, 0, fmt.Errorf("begin delete-manifest tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after Commit

	q := s.WithTx(tx).queries()
	kv := int32(kindVersion)

	// Guard 1: any resource (incl. draining) pinned to this (kind, kindVersion)?
	// EXISTS probe — short-circuits at the first row, never counts.
	hasResources, err := q.KindVersionHasResources(ctx, dbq.KindVersionHasResourcesParams{Kind: kind, KindVersion: int16(kv)})
	if err != nil {
		return false, 0, fmt.Errorf("check resources at %s/v%d: %w", kind, kindVersion, err)
	}
	if hasResources {
		return false, 0, fmt.Errorf("%w: %s/v%d still has resources; delete or migrate them first", ErrKindVersionInUse, kind, kindVersion)
	}

	// Guard 2: any reactor binding EXACTLY pinned to this (kind, kindVersion)?
	pinned, err := q.KindVersionPinnedByBinding(ctx, dbq.KindVersionPinnedByBindingParams{WatchKind: string(kind), KindVersion: int16(kv)})
	if err != nil {
		return false, 0, fmt.Errorf("check bindings pinned to %s/v%d: %w", kind, kindVersion, err)
	}
	if pinned {
		return false, 0, fmt.Errorf("%w: %s/v%d is pinned by a reactor binding; repin or delete it first", ErrKindVersionInUse, kind, kindVersion)
	}

	// Clear to delete. Cascade the version's provider configs, drop the derived
	// kind_config, then the manifest (whose AFTER DELETE trigger NOTIFYs).
	deletedConfigs, err = q.DeleteProviderConfigsAtKindVersion(ctx, dbq.DeleteProviderConfigsAtKindVersionParams{Kind: string(kind), KindVersion: int16(kv)})
	if err != nil {
		return false, 0, fmt.Errorf("delete provider configs at %s/v%d: %w", kind, kindVersion, err)
	}
	if _, err = q.DeleteKindConfig(ctx, dbq.DeleteKindConfigParams{Kind: string(kind), KindVersion: int16(kv)}); err != nil {
		return false, 0, fmt.Errorf("delete kind_config %s/v%d: %w", kind, kindVersion, err)
	}
	manifestRows, err := q.DeleteKindManifest(ctx, dbq.DeleteKindManifestParams{Kind: string(kind), KindVersion: int16(kv)})
	if err != nil {
		return false, 0, fmt.Errorf("delete kind_manifest %s/v%d: %w", kind, kindVersion, err)
	}
	if err = tx.Commit(ctx); err != nil {
		return false, 0, fmt.Errorf("commit delete-manifest %s/v%d: %w", kind, kindVersion, err)
	}
	return manifestRows > 0, deletedConfigs, nil
}

// ListKindManifests reads every manifest (KindManifestCache full load, API list).
func (s *Store) ListKindManifests(ctx context.Context) ([]model.KindManifest, error) {
	rows, err := s.queries().ListKindManifests(ctx)
	if err != nil {
		return nil, fmt.Errorf("list kind_manifests: %w", err)
	}
	out := make([]model.KindManifest, 0, len(rows))
	for _, r := range rows {
		m, err := manifestFromRow(r)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

// KindHashes carries the two content identities for one published (kind, kindVersion):
// the schema-only hash (schema-stability gate) and the folded content-hash
// (manifest_version — the reactions+schemas fence). The publish-time
// (kind, kindVersion)→content-hash invariant compares an incoming manifest against
// these before allowing an in-place same-kindVersion overwrite.
type KindHashes struct {
	SchemaHash      string
	ManifestVersion int64
}

// GetKindHashes reads the stored schema hash + content hash for a (kind, kindVersion),
// for the schema-stability gate and the publish invariant. (false if the
// (kind, kindVersion) has no manifest yet.)
func (s *Store) GetKindHashes(ctx context.Context, kind model.Kind, kindVersion int) (KindHashes, bool, error) {
	row, err := s.queries().GetKindSchemaHash(ctx, dbq.GetKindSchemaHashParams{
		Kind:        string(kind),
		KindVersion: int32(kindVersion),
	})
	if err != nil {
		if isNoRows(err) {
			return KindHashes{}, false, nil
		}
		return KindHashes{}, false, fmt.Errorf("get hashes %q/v%d: %w", kind, kindVersion, err)
	}
	out := KindHashes{ManifestVersion: row.ManifestVersion}
	if row.SchemaHash != nil {
		out.SchemaHash = *row.SchemaHash
	}
	return out, true, nil
}

// KindPolicy is one (kind, kindVersion)'s orphan-grace + finalizer, batch-loaded.
type KindPolicy struct {
	OrphanGraceSecs int
	FinalizerName   string
}

// BatchKindPolicy reads (orphan_grace, finalizer) for many kinds in ONE query,
// so the composer's orphan-prune loop can build an in-memory map before its
// per-child loop — never N per-child round-trips at 1M children. The returned map
// is keyed by (kind, kindVersion): the query returns EVERY kindVersion of each requested kind
// (a handful of rows), and the composer indexes it by the child's concrete
// (kind, kindVersion).
func (s *Store) BatchKindPolicy(ctx context.Context, kinds []model.Kind) (map[model.KindVersion]KindPolicy, error) {
	if len(kinds) == 0 {
		return map[model.KindVersion]KindPolicy{}, nil
	}
	ks := make([]string, len(kinds))
	for i, k := range kinds {
		ks[i] = string(k)
	}
	rows, err := s.queries().BatchKindPolicy(ctx, ks)
	if err != nil {
		return nil, fmt.Errorf("batch kind policy: %w", err)
	}
	out := make(map[model.KindVersion]KindPolicy, len(rows))
	for _, r := range rows {
		fin := ""
		if r.FinalizerName != nil {
			fin = *r.FinalizerName
		}
		out[model.KindVersion{Kind: model.Kind(r.Kind), Version: int(r.KindVersion)}] =
			KindPolicy{OrphanGraceSecs: int(r.OrphanGraceSecs), FinalizerName: fin}
	}
	return out, nil
}

// ── helpers ──

func manifestFromRow(r dbq.KindManifest) (model.KindManifest, error) {
	var reactions []model.ReactionDecl
	if len(r.Reactions) > 0 {
		if err := json.Unmarshal(r.Reactions, &reactions); err != nil {
			return model.KindManifest{}, fmt.Errorf("unmarshal reactions for %q: %w", r.Kind, err)
		}
	}
	return model.KindManifest{
		Kind:                 model.Kind(r.Kind),
		KindVersion:          int(r.KindVersion),
		Description:          derefString(r.Description),
		SpecSchema:           r.SpecSchema,
		StatusSchema:         r.StatusSchema,
		ConfigSchema:         r.ConfigSchema,
		Reactions:            reactions,
		FinalizerName:        derefString(r.FinalizerName),
		MaxInflight:          int(r.MaxInflight),
		TaskDeadlineSecs:     int(r.TaskDeadlineSecs),
		ResyncIntervalSecs:   int(r.ResyncIntervalSecs),
		ResyncRecomposes:     r.ResyncRecomposes,
		OrphanGraceSecs:      int(r.OrphanGraceSecs),
		MaxTransientAttempts: int(r.MaxTransientAttempts),
		Retired:              r.Retired,
	}, nil
}

// manifestVersion is the content hash (reactions + schema hash) that fences
// in-flight work: a re-apply with identical content yields the same version.
func manifestVersion(reactions json.RawMessage, schemaHash string) int64 {
	h := sha256.New()
	h.Write(reactions)
	h.Write([]byte{0})
	h.Write([]byte(schemaHash))
	sum := h.Sum(nil)
	// Fold the 256-bit digest into a positive int64 (BIGINT column). Collisions
	// are astronomically unlikely and only cost a spurious non-fence (the
	// worker_id ownership clause is the primary fence).
	var v int64
	for i := 0; i < 8; i++ {
		v = (v << 8) | int64(sum[i])
	}
	if v < 0 {
		v = -v
	}
	if v == 0 {
		v = 1 // 0 is the "no manifest / inert fence" sentinel; never collide with it
	}
	return v
}

// hashSchemas hashes the three JSON Schemas for the schema-stability gate. It
// delegates to kindschema.SchemaHashOf so the persisted schema_hash and the API
// gate's recomputed hash can never drift.
func hashSchemas(m model.KindManifest) string {
	return kindschema.SchemaHashOf(m)
}
