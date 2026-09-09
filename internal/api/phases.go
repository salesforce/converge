package api

// Resource phase values. The SOURCE OF TRUTH is the resources.phase GENERATED
// column in db/migrations/00001_schema.sql (a CASE over the scalar axes +
// delete/freeze precedence). These constants let the server-side derivation
// (projection, ops gating) reference the same strings by name instead of
// scattering literals that can silently drift from the schema.
const (
	phaseDegraded    = "Degraded"
	phaseFailed      = "Failed"
	phaseOrphaned    = "Orphaned"
	phaseQuarantined = "Quarantined"
)

// Condition vocabulary — the K8s-style status axes the conditions projection
// synthesizes + honors (projection_conditions.go). Two axes (Synced/Ready), the
// tri-state status, and the reason phrases, named so the projection references them
// by symbol instead of scattering the same strings across its switch arms.
const (
	conditionTypeSynced = "Synced"
	conditionTypeReady  = "Ready"

	conditionStatusTrue    = "True"
	conditionStatusFalse   = "False"
	conditionStatusUnknown = "Unknown"

	conditionReasonReconcileSuccess = "ReconcileSuccess" // Synced=True
	conditionReasonReconciling      = "Reconciling"      // Synced=Unknown
	conditionReasonPending          = "Pending"          // Ready=Unknown (not yet synced)
	conditionReasonAvailable        = "Available"        // Ready=True
	conditionReasonUnavailable      = "Unavailable"      // Ready=False
)
