package runtime

import (
	"context"
	"fmt"

	"github.com/salesforce/converge/internal/model"
)

// noHandlerExecutor is the fail-closed default model.StageDispatcher a
// Dispatcher/Loop/ReactorDispatcher holds when none was wired. It is a safety
// net, NOT an execution path: production ALWAYS installs the broker's remote
// fanout executor via SetDispatcher before Run, and the in-process test harness
// injects its own executor. If a task somehow reaches an un-wired dispatcher, it
// fails TRANSIENTLY (the delivery stays claimed, the reaper re-arms it —
// at-least-once), never a silent drop or a panic on a zero-value Loop (LSP: a
// zero-value Loop must be safe to call).
type noHandlerExecutor struct{}

var _ model.StageDispatcher = noHandlerExecutor{}

// DispatchStage always fails: there is no handler because no executor was wired.
// The error is NON-terminal so the task re-arms rather than dead-letters — an
// un-wired executor is an operational misconfiguration to recover from, not a
// resource-level permanent failure.
func (noHandlerExecutor) DispatchStage(_ context.Context, kind model.Kind, kindVersion int, rx model.ReactionDecl, _ model.ReactionRequest) (model.Outcome, int, error) {
	return model.Outcome{}, 0, fmt.Errorf("runtime: no reaction executor wired for %s/v%d/%s (SetDispatcher was not called)", kind, kindVersion, rx.Name)
}
