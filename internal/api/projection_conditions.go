package api

import (
	"github.com/salesforce/converge/internal/store"
)

func aggregateReadinessKind(rows []store.CountChildrenByReadinessRow) (readinessTotals, []readinessKindCount) {
	totals := readinessTotals{}
	byKind := make([]readinessKindCount, 0, len(rows))
	for _, r := range rows {
		totals.Ready += r.Ready
		totals.Reconciling += r.Reconciling
		totals.Degraded += r.Degraded
		totals.Failed += r.Failed
		totals.Deleting += r.Deleting
		totals.Orphaned += r.Orphaned
		totals.Quarantined += r.Quarantined
		byKind = append(byKind, readinessKindCount{
			Kind:        string(r.Kind),
			Ready:       r.Ready,
			Reconciling: r.Reconciling,
			Degraded:    r.Degraded,
			Failed:      r.Failed,
			Deleting:    r.Deleting,
			Orphaned:    r.Orphaned,
			Quarantined: r.Quarantined,
		})
	}
	return totals, byKind
}

func aggregateReadinessKindScoped(rows []store.CountResourcesByReadinessScopedRow) (readinessTotals, []readinessKindCount) {
	totals := readinessTotals{}
	byKind := make([]readinessKindCount, 0, len(rows))
	for _, r := range rows {
		totals.Ready += r.Ready
		totals.Reconciling += r.Reconciling
		totals.Degraded += r.Degraded
		totals.Failed += r.Failed
		totals.Deleting += r.Deleting
		totals.Orphaned += r.Orphaned
		totals.Quarantined += r.Quarantined
		byKind = append(byKind, readinessKindCount{
			Kind:        string(r.Kind),
			Ready:       r.Ready,
			Reconciling: r.Reconciling,
			Degraded:    r.Degraded,
			Failed:      r.Failed,
			Deleting:    r.Deleting,
			Orphaned:    r.Orphaned,
			Quarantined: r.Quarantined,
		})
	}
	return totals, byKind
}

// synthesizeConditions builds the full K8s/Crossplane conditions array
// for one resource from its scalar axes + the generated `phase` + any
// stored condition rows.
//
// `phase` is the source of truth; the conditions array is its human-
// readable expansion. The key correctness rule: a stored False row is
// surfaced ONLY when the live phase still agrees with it. This is what
// makes a recovered resource (phase back to Ready) stop showing a stale
// error in the detail panel — the stale False is gated out, not displayed.
//
//   - Synced: stored Synced=False is shown only when phase='Failed' (its
//     reason/message is the "why"); otherwise synthesized from
//     synced_gen vs generation (True when synced, Unknown/Reconciling
//     when not — never False, since "not reconciled yet" is in-progress).
//   - Ready: stored Ready=False is shown only when phase is Degraded or
//     Failed; a stored Ready row with any other status passes through;
//     otherwise synthesized from health_ok.
//   - custom stored conditions pass through unchanged.
//
// `stored` is the resource_conditions rows (usually empty — the common
// case, where both axes are synthesized and nothing was persisted).
func synthesizeConditions(generation, syncedGen int64, healthOK bool, phase string, stored []conditionDTO) []conditionDTO {
	synced := syncedGen >= generation
	var storedSynced, storedReady *conditionDTO
	custom := make([]conditionDTO, 0, len(stored))
	for i := range stored {
		switch stored[i].Type {
		case conditionTypeSynced:
			storedSynced = &stored[i]
		case conditionTypeReady:
			storedReady = &stored[i]
		default:
			custom = append(custom, stored[i])
		}
	}

	out := make([]conditionDTO, 0, len(stored)+2)

	// Synced axis. A stored False is honored only if the live phase still
	// says Failed; otherwise the resource recovered and we synthesize.
	switch {
	case storedSynced != nil && storedSynced.Status == conditionStatusFalse && phase == phaseFailed:
		out = append(out, *storedSynced)
	case synced:
		out = append(out, conditionDTO{Type: conditionTypeSynced, Status: conditionStatusTrue, Reason: conditionReasonReconcileSuccess, ObservedGeneration: syncedGen})
	default:
		out = append(out, conditionDTO{Type: conditionTypeSynced, Status: conditionStatusUnknown, Reason: conditionReasonReconciling, ObservedGeneration: syncedGen})
	}

	// Ready axis. A stored False is honored only while phase is Degraded
	// or Failed; a non-False stored Ready (True/Unknown the provider
	// reported) always wins over the synthesized value.
	readyDemoted := phase == phaseDegraded || phase == phaseFailed
	switch {
	case storedReady != nil && (storedReady.Status != conditionStatusFalse || readyDemoted):
		out = append(out, *storedReady)
	case !synced:
		out = append(out, conditionDTO{Type: conditionTypeReady, Status: conditionStatusUnknown, Reason: conditionReasonPending, ObservedGeneration: syncedGen})
	case healthOK:
		out = append(out, conditionDTO{Type: conditionTypeReady, Status: conditionStatusTrue, Reason: conditionReasonAvailable, ObservedGeneration: syncedGen})
	default:
		out = append(out, conditionDTO{Type: conditionTypeReady, Status: conditionStatusFalse, Reason: conditionReasonUnavailable, ObservedGeneration: syncedGen})
	}

	return append(out, custom...)
}

// storedConditionsToDTO converts the sqlc condition rows for one resource
// into the API DTO shape.
func storedConditionsToDTO(rows []store.ListResourceConditionsRow) []conditionDTO {
	if len(rows) == 0 {
		return nil
	}
	out := make([]conditionDTO, len(rows))
	for i, r := range rows {
		out[i] = conditionDTO{
			Type:               r.Type,
			Status:             r.Status,
			Reason:             r.Reason,
			Message:            r.Message,
			ObservedGeneration: r.ObservedGeneration,
			LastTransitionAt:   r.LastTransitionAt,
		}
	}
	return out
}
