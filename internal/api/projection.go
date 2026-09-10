package api

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/salesforce/converge/internal/store"
)

// projection.go holds the store resource-row → DTO mappers and the elision /
// tiny-utility helpers they use. Owner resolution lives in projection_owner.go,
// label synthesis + query parsing in projection_labels.go, the cluster-view
// projection in projection_cluster.go, and the work-block projection in
// projection_work.go.

func ptrFromBytes(b [16]byte) *uuid.UUID {
	id := uuid.UUID(b)
	return &id
}

// strVal dereferences a nullable *string to "" when nil.
func strVal(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func coerceJSONInterface(v interface{}) json.RawMessage {
	if v == nil {
		return nil
	}
	switch t := v.(type) {
	case []byte:
		return t
	case string:
		return json.RawMessage(t)
	case json.RawMessage:
		return t
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return b
}

const maxInlineJSONBytes = 100 * 1024

// elideFromSize replaces an oversized JSON body with a marker pointing at the
// dedicated download endpoint, keeping list/detail responses bounded.
func elideFromSize(raw json.RawMessage, sizeBytes int64, kind string) json.RawMessage {
	if sizeBytes <= maxInlineJSONBytes {
		return raw
	}
	return elidedMarker(sizeBytes, kind)
}

func elidedMarker(sizeBytes int64, kind string) json.RawMessage {
	marker, _ := json.Marshal(map[string]any{
		"_elided":    true,
		"size_bytes": sizeBytes,
		"reason":     fmt.Sprintf("%s exceeds %d byte inline limit; use the dedicated download endpoint", kind, maxInlineJSONBytes),
	})
	return marker
}

// elidableRow is the projection-neutral resource row: the union of fields the
// detail / list / apply reads produce, so one rowToFull maps them all. Paths
// that don't read a field (e.g. the info read carries no spec/owner) leave it
// zero; rowToFull handles the absences.
type elidableRow struct {
	ID          uuid.UUID // internal uuid — surfaced ONLY as resourceFull.UID
	Kind        string
	KindVersion int // pinned web-API version (0 when a projection path doesn't read it)
	Name        string
	// Owner/Root as PUBLIC (kind, name) refs. On the detail read they come from
	// GetResource's meta joins; on the apply/info read they're left empty (the
	// client re-fetches the full resource). Empty kind = root/no owner.
	OwnerKind           string
	OwnerName           string
	RootKind            string
	RootName            string
	Generation          int64
	SyncedGen           int64
	IsReady             bool
	HealthOK            bool
	Phase               string
	Finalizers          []string
	DeletionRequestedAt pgtype.Timestamptz
	Labels              []byte
	CreatedAt           pgtype.Timestamptz
	UpdatedAt           pgtype.Timestamptz

	Spec       json.RawMessage
	SpecSize   int64
	Status     json.RawMessage
	StatusSize int64

	// Attached CUSTOM provider config (empty when none). Only the detail read
	// (GetResource) populates these; the list/apply paths leave them empty.
	ProviderConfigName string
	ProviderConfigKind string

	// manifest_version drift: the version this resource is pinned to, and the
	// kind's current applied version (nil if the kind has no manifest). Only the
	// detail read (GetResource) populates these; other paths leave them zero/nil.
	ManifestVersion     int64
	KindManifestVersion *int64

	// Failure classification (detail read only): FailureTerminal = current-gen
	// failure won't auto-retry (provider-terminal OR poison-pill at the cap);
	// FailureAttempts = durable consecutive-transient-failure count for this
	// generation, climbing toward MaxTransientAttempts (the kind's cap, nil when the
	// kind has no config or the cap is unset).
	FailureTerminal      bool
	FailureAttempts      int32
	MaxTransientAttempts *int32

	// Conditions is the stored resource_conditions rows; rowToFull folds
	// them with the synthesized Synced/Ready axes. Loaded by the detail
	// handler (nil for the list/create paths, which don't fetch them).
	Conditions []conditionDTO
}

func rowToFull(r elidableRow) resourceFull {
	// Labels carry the synthesized owner_kind/owner_name (public identity) when
	// this resource has an owner, mirroring the list-row synthLabels — but from
	// the row's own resolved owner ref, so no owner-cache round-trip is needed.
	labels := decodeJSONMap(r.Labels)
	if r.OwnerKind != "" {
		labels["owner_kind"] = r.OwnerKind
		labels["owner_name"] = r.OwnerName
	}
	return resourceFull{
		UID:                  r.ID.String(),
		Kind:                 r.Kind,
		KindVersion:          r.KindVersion,
		Name:                 r.Name,
		OwnerKind:            r.OwnerKind,
		OwnerName:            r.OwnerName,
		RootKind:             r.RootKind,
		RootName:             r.RootName,
		Generation:           r.Generation,
		SyncedGen:            r.SyncedGen,
		IsReady:              r.IsReady,
		HealthOK:             r.HealthOK,
		Phase:                r.Phase,
		Conditions:           synthesizeConditions(r.Generation, r.SyncedGen, r.HealthOK, r.Phase, r.Conditions),
		Finalizers:           r.Finalizers,
		DeletionRequestedAt:  r.DeletionRequestedAt,
		Labels:               labels,
		CreatedAt:            r.CreatedAt,
		UpdatedAt:            r.UpdatedAt,
		Spec:                 elideFromSize(r.Spec, r.SpecSize, "spec"),
		Status:               elideFromSize(r.Status, r.StatusSize, "status"),
		ManifestDrift:        r.KindManifestVersion != nil && r.ManifestVersion != *r.KindManifestVersion,
		ProviderConfig:       providerConfigRef(r.ProviderConfigName, r.ProviderConfigKind),
		FailureTerminal:      r.FailureTerminal,
		FailureAttempts:      r.FailureAttempts,
		MaxTransientAttempts: r.MaxTransientAttempts,
	}
}

// providerConfigRef builds the resource-detail link DTO for an attached custom
// config, or nil when the resource has none (the common case).
func providerConfigRef(name, kind string) *providerConfigRefDTO {
	if name == "" {
		return nil
	}
	return &providerConfigRefDTO{Name: name, Kind: kind}
}

func fromGetResourceRow(r store.GetResourceRow) elidableRow {
	return elidableRow{
		ID:                   r.ID,
		Kind:                 string(r.Kind),
		KindVersion:          int(r.KindVersion),
		Name:                 r.Name,
		OwnerKind:            strVal(r.OwnerKind),
		OwnerName:            strVal(r.OwnerName),
		RootKind:             strVal(r.RootKind),
		RootName:             strVal(r.RootName),
		Generation:           r.Generation,
		SyncedGen:            r.SyncedGen,
		IsReady:              r.IsReady,
		HealthOK:             r.HealthOk,
		Phase:                r.Phase,
		Finalizers:           r.Finalizers,
		DeletionRequestedAt:  r.DeletionRequestedAt,
		Labels:               r.Labels,
		CreatedAt:            r.CreatedAt,
		UpdatedAt:            r.UpdatedAt,
		Spec:                 coerceJSONInterface(r.Spec),
		SpecSize:             r.SpecSize,
		Status:               coerceJSONInterface(r.Status),
		StatusSize:           r.StatusSize,
		ManifestVersion:      r.ManifestVersion,
		KindManifestVersion:  r.KindManifestVersion,
		ProviderConfigName:   strVal(r.ProviderConfigName),
		ProviderConfigKind:   strVal(r.ProviderConfigKind),
		FailureTerminal:      r.FailureTerminal,
		FailureAttempts:      r.FailureAttempts,
		MaxTransientAttempts: r.MaxTransientAttempts,
	}
}

// fromGetResourceInfoRow projects the lightweight GetResourceInfo row (no
// spec/status, so no pg_column_size read of the body) into elidableRow. Used
// by the apply response: it only needs identity + state, and crucially must
// NOT re-read a freshly-written multi-MB spec just to elide it. Spec/Status
// are left empty (the client re-fetches the full resource on detail open).
func fromGetResourceInfoRow(r store.GetResourceInfoRow) elidableRow {
	return elidableRow{
		ID:                  r.ID,
		Kind:                string(r.Kind),
		KindVersion:         int(r.KindVersion),
		Name:                r.Name,
		Generation:          r.Generation,
		SyncedGen:           r.SyncedGen,
		IsReady:             r.IsReady,
		HealthOK:            r.HealthOk,
		Phase:               r.Phase,
		DeletionRequestedAt: r.DeletionRequestedAt,
		Labels:              r.Labels,
		CreatedAt:           r.CreatedAt,
		UpdatedAt:           r.UpdatedAt,
		// Owner/Root refs, Finalizers, Spec/Status intentionally omitted —
		// GetResourceInfo doesn't carry them and the apply response doesn't
		// need them (the client re-fetches the full resource on detail open).
	}
}
