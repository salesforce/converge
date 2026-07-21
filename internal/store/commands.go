package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/model"
)

// commands.go is the write/use-case face of the persistence boundary:
// the resource mutations the API issues (create / patch / re-pend). They
// live here, beside RequestResourceDeletion, rather than on the engine —
// the engine owns process lifecycle (pools, goroutines, migrations), not
// the per-request data mutations. Each is "do the write, then schedule":
// the bump_generation trigger advances generation, and schedule_eligible
// enqueues the reconcile (its gates decide what actually pends).

// txBeginner is satisfied by *pgxpool.Pool and pgx.Tx (savepoints). It lets
// the write commands open their own transaction so a multi-statement
// use-case (upsert + enqueue) commits atomically — no window where the
// spec bumped but the reconcile was never queued.
type txBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// ApplyResult reports the outcome of an ApplySpec, mirroring the
// kubectl-apply states: a row is Created, Changed (spec/labels differed
// → "configured"), or neither (an identical re-apply → "unchanged").
type ApplyResult struct {
	ID         uuid.UUID
	Created    bool  // row was inserted
	Changed    bool  // spec or labels actually differed (true on create)
	Generation int64 // the resulting generation (authored by this apply)
}

// ApplySpec create-or-updates a resource keyed by the global (kind,
// name) unique index — the single write behind the API's "Apply"
// (a ResourceManifest decomposed into kind/name/labels/spec at this
// boundary). Because names are globally unique (the k8s/Crossplane
// model), (kind, name) addresses exactly one resource whether root or
// owned child: a new (kind,name) inserts a root, an existing one
// overwrites that exact row in place WITHOUT reparenting (applying to a
// child updates the child, never spawns a stray root). UpsertResource
// bumps generation only on a genuine spec change (the body_changed CTE),
// so a re-apply of an identical spec is a true no-op — in that case we
// also skip schedule_eligible (nothing to re-pend).
//
// Runs in ONE transaction: UpsertResource (which FOR-UPDATE-locks the
// existing row, serializing concurrent updates to the same (kind,name))
// and the Changed-gated ScheduleEligible commit together, so there is no
// window where generation bumped but the reconcile was never enqueued.
// A unique-violation from a create-vs-create race is mapped to a single
// retry (the row is now visible → the update path runs deterministically);
// a residual conflict surfaces as ErrConflict rather than a raw 500.
func (s *Store) ApplySpec(ctx context.Context, kind model.Kind, name string, spec json.RawMessage, labels map[string]string) (ApplyResult, error) {
	return s.ApplySpecKindVersionWithConfig(ctx, kind, 1, name, spec, labels, "")
}

// ApplySpecWithConfig is ApplySpec (kindVersion=1) plus a CUSTOM config ref. Kept for
// callers that don't carry a kindVersion (ingestion, reactor chains) — they apply v1.
func (s *Store) ApplySpecWithConfig(ctx context.Context, kind model.Kind, name string, spec json.RawMessage, labels map[string]string, providerConfigName string) (ApplyResult, error) {
	return s.ApplySpecKindVersionWithConfig(ctx, kind, 1, name, spec, labels, providerConfigName)
}

// ApplySpecWithConfig is ApplySpec plus the per-resource CUSTOM config ref:
// providerConfigName names a row in the providerconfigs table whose spec is
// cloned into work_queue at schedule time and overrides the kind default for
// this resource's tasks. Empty string ⇒ no custom config (NULL; the resource
// uses its kind default only). A change to ONLY the ref still re-pends the
// resource (UpsertResource bumps generation on a ref change) but appends no
// spec_versions row — config is orthogonal to spec. A non-existent config name
// is rejected (the FK requires the row to exist), so typos fail fast.
// ApplySpecKindVersionWithConfig is the full create-or-update apply: it pins `kindVersion`
// (the web-API version) on CREATE. On an EXISTING (kind, name) it overwrites the
// same-kindVersion row in place; changing the kindVersion of an existing resource is a
// BREAKING FLIP handled by FlipResourceKindVersion (the API detects the mismatch and
// routes there), NOT this path — here kindVersion is only authoritative on insert.
func (s *Store) ApplySpecKindVersionWithConfig(ctx context.Context, kind model.Kind, kindVersion int, name string, spec json.RawMessage, labels map[string]string, providerConfigName string) (ApplyResult, error) {
	// kind_version is REQUIRED and explicit (>= 1) at the DATA GATEWAY: every apply
	// — HTTP, ingestion, reactor-chained — must name the web-API version it targets.
	// There is no implicit v1 default; a missing/0 kind_version is a hard error here,
	// never silently coerced (the DB column is NOT NULL with no default anyway).
	if kindVersion < 1 {
		return ApplyResult{}, fmt.Errorf("%w: %s/%s: kind_version is required and must be >= 1 (got %d)", ErrInvalidSpec, kind, name, kindVersion)
	}
	// Validate the spec against the kind's declared schema at the DATA GATEWAY,
	// so an author bypassing the HTTP boundary — an ingestion duty pumping an
	// external (SQS) BOM, a reactor chaining a resource — is checked exactly like
	// an /api/resources POST. Fails fast before the tx. nil validator (tests) ⇒
	// skip. ErrInvalidSpec wraps the typed *specschema.Error so the API maps it
	// to 400 and other callers can detect it.
	if s.validator != nil {
		if err := s.validator.ValidateSpec(kind, kindVersion, spec); err != nil {
			return ApplyResult{}, fmt.Errorf("%w: %w", ErrInvalidSpec, err)
		}
	}

	labelsBytes := json.RawMessage(`{}`)
	if len(labels) > 0 {
		b, err := json.Marshal(labels)
		if err != nil {
			return ApplyResult{}, fmt.Errorf("marshal labels: %w", err)
		}
		labelsBytes = b
	}

	res, err := s.applySpecOnce(ctx, kind, kindVersion, name, spec, labelsBytes, providerConfigName)
	if err != nil && isUniqueViolation(err) {
		// create-vs-create (or a same-generation spec_versions) race: the
		// losing tx aborted on a unique/PK violation. The winner is now
		// committed and visible, so a single retry takes the deterministic
		// update path (prev non-empty, FOR UPDATE serialized).
		res, err = s.applySpecOnce(ctx, kind, kindVersion, name, spec, labelsBytes, providerConfigName)
		if err != nil && isUniqueViolation(err) {
			return ApplyResult{}, ErrConflict
		}
	}
	return res, err
}

// applySpecOnce runs the upsert + gated enqueue in a single transaction. kindVersion
// pins the web-API version on CREATE (UpsertResource stamps it + the (kind,
// kindVersion) manifest_version); it is inert on the update path (an existing row keeps
// its pinned kindVersion — a kindVersion change is a FlipResourceKindVersion, not this path).
func (s *Store) applySpecOnce(ctx context.Context, kind model.Kind, kindVersion int, name string, spec, labelsBytes json.RawMessage, providerConfigName string) (ApplyResult, error) {
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return ApplyResult{}, fmt.Errorf("store: underlying DBTX cannot begin a transaction")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return ApplyResult{}, fmt.Errorf("begin apply tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after Commit

	q := s.WithTx(tx).queries()

	// Resolve the optional CUSTOM config ref (name → providerconfig id) inside
	// the tx so the FK target is observed consistently. Empty name → NULL.
	var providerConfigID pgtype.UUID
	if providerConfigName != "" {
		cfg, err := q.GetProviderConfigByName(ctx, providerConfigName)
		if err != nil {
			if isNoRows(err) {
				return ApplyResult{}, fmt.Errorf("%w: provider_config %q not found", ErrConfigNotFound, providerConfigName)
			}
			return ApplyResult{}, fmt.Errorf("resolve provider_config %q: %w", providerConfigName, err)
		}
		// A config parameterises exactly one (kind, kindVersion). Attaching a foreign-KIND
		// config would merge meaningless fields; attaching a config for a DIFFERENT
		// KIND VERSION (e.g. a vpc/v2 config onto a vpc/v1 resource) would apply a config
		// shaped for the wrong schema — so reject BOTH. This is the correctness gate:
		// a v1 resource can only carry a v1 config, a v2 resource a v2 config.
		if cfg.Kind != string(kind) {
			return ApplyResult{}, fmt.Errorf("%w: provider_config %q is for kind %q, not %q", ErrConfigKindMismatch, providerConfigName, cfg.Kind, kind)
		}
		if int(cfg.KindVersion) != kindVersion {
			return ApplyResult{}, fmt.Errorf("%w: provider_config %q is for %s/v%d, not %s/v%d", ErrConfigKindMismatch, providerConfigName, cfg.Kind, cfg.KindVersion, kind, kindVersion)
		}
		providerConfigID = toUUID(cfg.ID)
	}

	// Pre-allocate the resource id (used only on create, so the resources
	// row and its first spec_versions entry share it). On update/identical
	// re-apply it's harmless — the existing row's id is returned.
	row, err := q.UpsertResource(ctx, dbq.UpsertResourceParams{
		ID:               uuid.New(),
		Kind:             string(kind),
		KindVersion:      int32(kindVersion),
		Name:             name,
		Spec:             spec,
		Labels:           labelsBytes,
		ProviderConfigID: providerConfigID,
	})
	if err != nil {
		return ApplyResult{}, fmt.Errorf("upsert resource: %w", err)
	}
	// Identical re-apply: generation didn't move, so there's nothing to
	// reconcile. Skip the enqueue entirely.
	if row.Changed {
		if _, err := q.ScheduleEligible(ctx, []uuid.UUID{row.ID}); err != nil {
			return ApplyResult{}, fmt.Errorf("schedule resource: %w", err)
		}
		// LIFECYCLE REACTOR: emit a 'created' transition in the SAME tx (so it
		// commits atomically with the resource), gated by an EXISTS-on-bindings
		// probe inside the statement — a no-op when no 'created' binding exists.
		// Lets a saga react to "a new resource was submitted" (e.g. enrich it),
		// distinct from the 'synced' edge (the cascade trigger handles that).
		if err := q.EmitLifecycleCreated(ctx, dbq.EmitLifecycleCreatedParams{
			Column1: row.ID,
			Column2: string(kind),
			Column3: row.Generation,
			// The applied resource's version, so a version-scoped 'created' binding
			// (watch_kind_version set) fires only for its version; a NULL-scoped
			// binding matches all versions regardless.
			KindVersion: int16(kindVersion),
		}); err != nil {
			return ApplyResult{}, fmt.Errorf("emit lifecycle created: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ApplyResult{}, fmt.Errorf("commit apply tx: %w", err)
	}
	// Workers are woken by schedule_eligible's pg_notify('work_ready') fired
	// inside the tx above (delivered on this commit) — no Go-side wake needed.
	return ApplyResult{ID: row.ID, Created: row.Created, Changed: row.Changed, Generation: row.Generation}, nil
}

// Reconcile re-pends a resource so its kind's provider runs again even
// when the spec is unchanged (ReconcileResource bumps generation; see
// db/queries/engine.sql for why a bump rather than a direct enqueue).
// Co-transactional like ApplySpec: the bump and the enqueue commit
// together so there's no un-queued window.
func (s *Store) Reconcile(ctx context.Context, id uuid.UUID) error {
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return fmt.Errorf("store: underlying DBTX cannot begin a transaction")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin reconcile tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	q := s.WithTx(tx).queries()
	if err := q.ReconcileResource(ctx, id); err != nil {
		return err
	}
	if _, err := q.ScheduleEligible(ctx, []uuid.UUID{id}); err != nil {
		return err
	}
	// ScheduleEligible fired pg_notify('work_ready') in-tx; commit delivers it.
	return tx.Commit(ctx)
}

// FlipResult reports the outcome of a uuid-stable kindVersion flip.
type FlipResult struct {
	Flipped    bool  // false = already at newKindVersion / wrong oldKindVersion / frozen (idempotent no-op)
	Generation int64 // the resulting generation when flipped (0 when not)
	// SkewEdges is the count of value-flow edges into this resource whose recorded
	// src_kind_version no longer matches the new kindVersion: each was WARNed, so an
	// operator can re-validate a dependent whose upstream field may have moved.
	SkewEdges int
}

// FlipResourceKindVersion performs the uuid-stable kindVersion flip: rewrite a
// resource's spec to the target kindVersion's shape and bump its kindVersion + generation IN
// PLACE, so value-flows survive and the composer never orphan-tears-down live
// infra. The reject-on-schema-fail gate runs FIRST (an incomplete new-kindVersion
// spec never reconciles into real infra); on failure the resource stays on its
// current kindVersion. Idempotent: a redelivered flip whose row already moved past oldKindVersion
// is a no-op (Flipped=false). After the flip it re-schedules the row and WARNs on
// any value-flow skew so a stale dependent surfaces instead of silently
// carrying a stale value.
func (s *Store) FlipResourceKindVersion(ctx context.Context, id uuid.UUID, oldKindVersion, newKindVersion int, newSpec json.RawMessage, kind model.Kind) (FlipResult, error) {
	if newKindVersion <= 0 {
		return FlipResult{}, fmt.Errorf("flip: new kindVersion must be >= 1 (got %d)", newKindVersion)
	}
	// reject-on-schema-fail: validate the rewritten spec against the TARGET
	// kindVersion's schema before the flip commits — the validator is per-(kind, kindVersion),
	// so a v1→v2 flip is checked against v2's spec_schema, exactly the shape the
	// resource will reconcile under. An incomplete/mistyped new-kindVersion spec never
	// reaches real infra; the resource stays on its current kindVersion.
	if s.validator != nil {
		if err := s.validator.ValidateSpec(kind, newKindVersion, newSpec); err != nil {
			return FlipResult{}, fmt.Errorf("%w: %w", ErrInvalidSpec, err)
		}
	}

	beginner, ok := s.db.(txBeginner)
	if !ok {
		return FlipResult{}, fmt.Errorf("store: underlying DBTX cannot begin a transaction")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return FlipResult{}, fmt.Errorf("begin flip tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// The flip UPDATE is idempotent via the `kindVersion = oldKindVersion` guard.
	var res FlipResult
	row := tx.QueryRow(ctx, dbq.FlipResourceKindVersionSQL, id, int32(oldKindVersion), int32(newKindVersion), newSpec)
	var flippedID uuid.UUID
	if err := row.Scan(&flippedID); err != nil {
		if isNoRows(err) {
			// Already flipped / wrong old kindVersion / frozen — a benign no-op.
			return FlipResult{Flipped: false}, tx.Commit(ctx)
		}
		return FlipResult{}, fmt.Errorf("flip resource kindVersion: %w", err)
	}
	res.Flipped = true

	q := s.WithTx(tx).queries()
	if _, err := q.ScheduleEligible(ctx, []uuid.UUID{id}); err != nil {
		return FlipResult{}, fmt.Errorf("schedule flipped resource: %w", err)
	}

	// skew WARN: an upstream kindVersion bump can rename/relocate a status field the
	// dependent's value-flow pointer targets. List every edge whose src_kind_version no
	// longer matches the new kindVersion and WARN so a stale dependent is visible; the
	// fail-loud substitution in drain_outbox_batch handles the runtime data path.
	skewRows, err := tx.Query(ctx, dbq.FlipSkewEdgesSQL, id, int32(newKindVersion))
	if err != nil {
		return FlipResult{}, fmt.Errorf("scan flip skew edges: %w", err)
	}
	type skew struct {
		dependent, dependency uuid.UUID
		srcKindVersion        int32
	}
	var skews []skew
	for skewRows.Next() {
		var sk skew
		if err := skewRows.Scan(&sk.dependent, &sk.dependency, &sk.srcKindVersion); err != nil {
			skewRows.Close()
			return FlipResult{}, fmt.Errorf("scan flip skew edge: %w", err)
		}
		skews = append(skews, sk)
	}
	skewRows.Close()
	if err := skewRows.Err(); err != nil {
		return FlipResult{}, fmt.Errorf("iterate flip skew edges: %w", err)
	}
	for _, sk := range skews {
		slog.Warn("value-flow skew after kindVersion flip: dependent's src_field pointer was validated against an older upstream kindVersion and may now resolve NULL",
			"upstream", id, "kind", kind, "new_kind_version", newKindVersion,
			"dependent", sk.dependent, "edge_src_kind_version", sk.srcKindVersion)
	}
	res.SkewEdges = len(skews)

	// Re-baseline src_kind_version on every edge into the flipped upstream so a FUTURE
	// flip measures skew against this kindVersion, not a stale one.
	if _, err := tx.Exec(ctx, dbq.RecordEdgeSrcKindVersionSQL, id, int32(newKindVersion)); err != nil {
		return FlipResult{}, fmt.Errorf("record edge src_kind_version: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return FlipResult{}, fmt.Errorf("commit flip tx: %w", err)
	}
	return res, nil
}

// ProviderConfig is one row of the providerconfigs table, with its composer
// owner's kind+name resolved (empty when unowned / user-created).
type ProviderConfig struct {
	ID   uuid.UUID
	Name string
	Kind model.Kind
	// KindVersion is the web-API version of the consumer kind this config is FOR. A
	// config is per-(kind, kindVersion); the resource-apply guard requires an attached
	// config's (kind, kindVersion) to match the resource's.
	KindVersion int
	IsDefault   bool
	Spec        json.RawMessage
	// Data is the opaque provider BUNDLE (bytea): a zip of Starlark .star files or
	// similar the worker materialises. Carried on the read/detail + upsert paths so
	// the API can round-trip it (base64 as the `data` JSON field). Empty for a
	// config that uses only spec.
	Data      []byte
	OwnerID   *uuid.UUID
	OwnerKind string
	OwnerName string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ProviderConfigFilter is the optional filter set shared by ListProviderConfigs
// and CountProviderConfigs. Zero value (all empty) means "no filter".
type ProviderConfigFilter struct {
	Kind        string // exact kind ("" = any)
	KindVersion int    // exact kindVersion (0 = any)
	Name        string // substring match on name ("" = any)
	IsDefault   *bool  // tri-state (nil = any, &true / &false = that role)
}

// DefaultProviderConfig is a kind's DEFAULT providerconfig as the config plane
// loads it: the operational Spec (structured config, merged with a per-resource
// override downstream) and the opaque Data bundle (a zip of provider artifacts —
// e.g. Starlark .star files — the worker materialises; kind-wide, no override).
type DefaultProviderConfig struct {
	Spec json.RawMessage
	Data []byte
}

// GetDefaultProviderConfig returns a kind's DEFAULT config document + bundle.
// found=false (nil error) when the kind has no default — callers (boot/reconfigure)
// treat that as "no default config", not an error.
func (s *Store) GetDefaultProviderConfig(ctx context.Context, kind model.Kind, kindVersion int) (DefaultProviderConfig, bool, error) {
	row, err := s.queries().GetDefaultProviderConfig(ctx, dbq.GetDefaultProviderConfigParams{
		Kind:        string(kind),
		KindVersion: int32(kindVersion),
	})
	if err != nil {
		if isNoRows(err) {
			return DefaultProviderConfig{}, false, nil
		}
		return DefaultProviderConfig{}, false, err
	}
	return DefaultProviderConfig{Spec: row.Spec, Data: row.Data}, true, nil
}

// defaultConfigName returns the NAME of a kind's current DEFAULT config (if
// any). found=false (nil error) when the kind has no default. Used by the
// composer config diff to demote a conflicting emitted default to a custom
// override before upsert, so a second default for a kind never aborts the
// compose transaction.
func (s *Store) defaultConfigName(ctx context.Context, kind model.Kind, kindVersion int) (string, bool, error) {
	name, err := s.queries().GetDefaultProviderConfigName(ctx, dbq.GetDefaultProviderConfigNameParams{
		Kind:        string(kind),
		KindVersion: int32(kindVersion),
	})
	if err != nil {
		if isNoRows(err) {
			return "", false, nil
		}
		return "", false, err
	}
	return name, true, nil
}

// UpsertProviderConfig create-or-updates a config by name and returns the
// persisted row (so the API needs no read-after-write). created reports insert
// vs update. A second default for a kind → ErrConfigDefaultExists. owner_* are
// not resolved here (an API-applied config is owner-less); the read path joins
// them in.
func (s *Store) UpsertProviderConfig(ctx context.Context, name string, kind model.Kind, kindVersion int, isDefault bool, spec json.RawMessage, data []byte) (ProviderConfig, bool, error) {
	if len(spec) == 0 {
		spec = json.RawMessage(`{}`)
	}
	// data is BYTEA NOT NULL DEFAULT '': a nil slice binds as SQL NULL (the column
	// default does NOT apply when the column is present in the INSERT, only when
	// omitted), which the NOT NULL constraint rejects. Coalesce nil → empty bytes so
	// a config with no bundle (the common case — every config that uses only spec)
	// inserts a clean empty bundle, mirroring spec's {} coalesce above.
	if data == nil {
		data = []byte{}
	}
	row, err := s.queries().UpsertProviderConfig(ctx, dbq.UpsertProviderConfigParams{
		Name:        name,
		Kind:        string(kind),
		KindVersion: int32(kindVersion),
		IsDefault:   isDefault,
		Spec:        spec,
		Data:        data,
	})
	if err != nil {
		// The only unique constraint besides name (handled by ON CONFLICT) is
		// the one-default-per-(kind,kindVersion) partial index.
		if isUniqueViolation(err) {
			return ProviderConfig{}, false, ErrConfigDefaultExists
		}
		return ProviderConfig{}, false, err
	}
	return ProviderConfig{
		ID:          row.ID,
		Name:        row.Name,
		Kind:        model.Kind(row.Kind),
		KindVersion: int(row.KindVersion),
		IsDefault:   row.IsDefault,
		Spec:        row.Spec,
		Data:        row.Data,
		OwnerID:     uuidPtr(row.OwnerID),
		CreatedAt:   row.CreatedAt.Time,
		UpdatedAt:   row.UpdatedAt.Time,
	}, row.Created, nil
}

// GetProviderConfig returns one config by name (found=false when absent).
func (s *Store) GetProviderConfig(ctx context.Context, name string) (ProviderConfig, bool, error) {
	row, err := s.queries().GetProviderConfig(ctx, name)
	if err != nil {
		if isNoRows(err) {
			return ProviderConfig{}, false, nil
		}
		return ProviderConfig{}, false, err
	}
	return ProviderConfig{
		ID:          row.ID,
		Name:        row.Name,
		Kind:        model.Kind(row.Kind),
		KindVersion: int(row.KindVersion),
		IsDefault:   row.IsDefault,
		Spec:        row.Spec,
		Data:        row.Data,
		OwnerID:     uuidPtr(row.OwnerID),
		OwnerKind:   derefString(row.OwnerKind),
		OwnerName:   derefString(row.OwnerName),
		CreatedAt:   row.CreatedAt.Time,
		UpdatedAt:   row.UpdatedAt.Time,
	}, true, nil
}

// ListProviderConfigs returns one PAGINATED page of configs matching f, ordered
// (kind, name). Pair with CountProviderConfigs for the page total.
func (s *Store) ListProviderConfigs(ctx context.Context, f ProviderConfigFilter, limit, offset int) ([]ProviderConfig, error) {
	rows, err := s.queries().ListProviderConfigs(ctx, dbq.ListProviderConfigsParams{
		KindFilter:        f.Kind,
		KindVersionFilter: int32(f.KindVersion), // 0 = any kindVersion
		NameFilter:        f.Name,
		DefaultFilter:     f.IsDefault,
		Lim:               int32(limit),
		Off:               int32(offset),
	})
	if err != nil {
		return nil, err
	}
	out := make([]ProviderConfig, len(rows))
	for i, r := range rows {
		// SLIM: ListProviderConfigs does not select the document (spec + data
		// bundle) — Spec/Data stay nil here. The detail read (GetProviderConfig)
		// carries the full manifest.
		out[i] = ProviderConfig{
			ID:          r.ID,
			Name:        r.Name,
			Kind:        model.Kind(r.Kind),
			KindVersion: int(r.KindVersion),
			IsDefault:   r.IsDefault,
			OwnerID:     uuidPtr(r.OwnerID),
			OwnerKind:   derefString(r.OwnerKind),
			OwnerName:   derefString(r.OwnerName),
			CreatedAt:   r.CreatedAt.Time,
			UpdatedAt:   r.UpdatedAt.Time,
		}
	}
	return out, nil
}

// CountProviderConfigs returns the total rows matching f (the page total behind
// ListProviderConfigs).
func (s *Store) CountProviderConfigs(ctx context.Context, f ProviderConfigFilter) (int64, error) {
	return s.queries().CountProviderConfigs(ctx, dbq.CountProviderConfigsParams{
		KindFilter:        f.Kind,
		KindVersionFilter: int32(f.KindVersion),
		NameFilter:        f.Name,
		DefaultFilter:     f.IsDefault,
	})
}

// DeleteProviderConfig removes a config by name. deleted=false when none existed.
func (s *Store) DeleteProviderConfig(ctx context.Context, name string) (bool, error) {
	if _, err := s.queries().DeleteProviderConfig(ctx, name); err != nil {
		if isNoRows(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// uuidPtr converts a nullable pgtype.UUID to *uuid.UUID (nil when invalid).
func uuidPtr(u pgtype.UUID) *uuid.UUID {
	if !u.Valid {
		return nil
	}
	id := uuid.UUID(u.Bytes)
	return &id
}

// isUniqueViolation reports whether err is a Postgres 23505
// (unique_violation) — uniq_resource_meta on a create-vs-create race, or
// spec_versions_pkey on a same-generation version-append race.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
