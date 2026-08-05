package model

import "context"

// ─────────────────────────────────────────────────────────────────────────
// StageDispatcher — the dispatch seam between the reconcile engine and wherever
// a reaction's handler actually RUNS.
//
// The core's reconcile engine (internal/runtime) builds a ReactionRequest from a
// claimed DB row and calls DispatchStage; the IMPLEMENTATION decides WHERE the
// handler runs and returns its Outcome:
//   - remote (the broker's fanout, the ONLY production impl): the dispatcher does
//     NOT execute — it ships the stage to a dumb worker over Connect and awaits the
//     worker's result. Routing/leases/heartbeats are the broker's job; running the
//     handler is the worker's.
//   - in-process (test only, test/internal/inproc): encode the stage to a proto
//     StageTask and run it directly against the registered providers — the
//     zero-broker path the integration suite drives.
//
// Either way the ENGINE does all the DB reads/writes AROUND the stage (the gate
// reads, ApplyComposeResult, AppendOutbox) and applies the returned Outcome; the
// dispatcher only gets the handler run and hands the Outcome back. The single
// manifest-driven call means the engine never threads typed per-kind funcs.
//
// It takes a ReactionDecl (the manifest-declared reaction being dispatched) but
// speaks the author-facing ReactionRequest/Outcome — a worker author never
// implements or calls it; only the broker/engine/test executors do.
//
// Returns the Outcome, the failing handler index (for the error label), and the
// error (nil on success). A stage whose handler is not present is an error.
type StageDispatcher interface {
	// kindVersion is the resource's web-API version (work_queue.kindVersion, carried from the
	// claim) and is ALWAYS explicit (>= 1) — there is no implicit v1 default. A
	// dispatcher that routes to versioned handlers (in-process registry, or a
	// remote worker serving several kindVersions) uses it to select the
	// (kind, kindVersion)'s handler; a 0/negative value has no handler and is a hard
	// lookup miss (a real failure, never silently v1).
	DispatchStage(ctx context.Context, kind Kind, kindVersion int, rx ReactionDecl, req ReactionRequest) (Outcome, int, error)
}
