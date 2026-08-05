package converge

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/salesforce/converge/sdk-go/workerpb"
)

// ExecuteTask runs ONE proto StageTask against the given providers, in-process, and
// returns the proto StageComplete — the SAME decode→dispatch→encode path the WorkStream
// runner uses per task (requestFromTask → the provider's Work → completeFromOutcome), with
// NO broker, stream, or transport.
//
// It exists for the in-process test harness: the core's engine drives reconciliation
// without a broker, and its test executor speaks the SDK's REAL wire codec by encoding a
// stage to a StageTask, calling this, and decoding the StageComplete — so an in-process
// test exercises the exact proto contract a shipped worker does, instead of a hand-written
// type bridge. A shipped worker never calls this (it uses Serve); it is the SDK's
// in-process execution seam, kept here so the codec + dispatch have ONE home shared with
// runStage — both route the task through computeComplete.
//
// The providers are indexed by (kind, kindVersion); a task whose reaction has no handler,
// whose stage/payload is malformed, or whose kind is unregistered comes back as a
// terminal-failure StageComplete (never a panic), matching runStage.
func ExecuteTask(ctx context.Context, providers []Provider, t *workerpb.StageTask) *workerpb.StageComplete {
	reactions := newReactionRegistry()
	for _, p := range providers {
		kv := p.Kind()
		kindRuntime{Kind: kv.Kind, KindVersion: kv.Version, Handlers: map[string]ReactionHandler{
			reactionWildcard: providerHandler{p: p},
		}}.registerInto(reactions)
	}
	// slog.Default so a handler that logs during an in-process test doesn't panic; the
	// bare ctx (no per-task deadline) matches an in-process test's expectations. nil
	// progress hook: Env.Heartbeat is an optional no-op hint, and the in-process path has no
	// broker to attest liveness to.
	return computeComplete(ctx, reactions, t, slog.Default(), nil)
}

// computeComplete is the ONE decode→lookup→run→encode body shared by the WorkStream
// runner (runStage, per streamed task) and the in-process ExecuteTask seam. It is PURE —
// it computes and RETURNS a StageComplete, never sends it and never touches a stream — so
// each caller owns its own transport (runStage enqueues onto sendCh; ExecuteTask returns
// directly to the test executor). The caller supplies the reaction registry, the reaction
// ctx (runStage narrows it to task_deadline_ms; ExecuteTask passes it through), the logger
// to stamp on req.Env (runStage's rich per-task logger; ExecuteTask's default), and an
// OPTIONAL progress hook wired onto req.Env.Heartbeat — a no-op progress hint (Env.Heartbeat
// is not a liveness gate; a running handler is attested for as long as it runs). Both
// shipped callers pass nil today; the param stays so a future caller can pass one.
//
// A task whose reaction name is empty, whose payload is missing / stage unknown, whose
// (kind, kindVersion, reaction) has no handler, or whose Outcome fails to encode comes back
// as a terminal-failure StageComplete (never a panic) — the reaction-error path stamps
// FailedStage from the failing handler index.
func computeComplete(ctx context.Context, reactions *reactionRegistry, t *workerpb.StageTask, log *slog.Logger, progress func()) *workerpb.StageComplete {
	kind := Kind(t.GetKind())
	kindVersion := int(t.GetKindVersion())
	reaction := t.GetReaction()
	if reaction == "" {
		return failComplete(t, fmt.Sprintf("converge: StageTask for kind %q stage %s carries no reaction name", kind, t.GetStage()), true)
	}
	req, ok := requestFromTask(t)
	if !ok {
		return failComplete(t, fmt.Sprintf("converge: StageTask for stage %s is missing its request payload or has an unknown stage", t.GetStage()), true)
	}
	// requestFromTask always sets req.Env (with the per-task config/bundle override); the
	// caller's logger + progress hook go on it so a handler can log with the right scope and
	// attest liveness for long-running work.
	req.Env.Logger = log
	req.Env.Heartbeat = progress

	handlers, ok := reactions.lookup(kind, kindVersion, reaction)
	if !ok {
		return failComplete(t, fmt.Sprintf("converge: no handler for %s/v%d/%s", kind, kindVersion, reaction), true)
	}
	out, failed, err := runReactionStages(ctx, handlers, req)
	if err != nil {
		sc := failComplete(t, err.Error(), IsTerminal(err))
		sc.FailedStage = int32(failed)
		return sc
	}
	sc, err := completeFromOutcome(t, out)
	if err != nil {
		return failComplete(t, fmt.Sprintf("converge: %v", err), true)
	}
	return sc
}
