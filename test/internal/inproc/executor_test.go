package inproc_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/sdk-go/converge"
	"github.com/salesforce/converge/test/internal/inproc"
)

// fakeProvider is a converge.Provider whose Work records the reaction it was asked to run
// and echoes the reaction name back — enough to prove DispatchStage encoded the right
// stage, routed to this provider, and threaded the Outcome back. An operate reaction emits
// its result on OperationOutput (the operate stage's payload axis); every other reaction
// emits on Status — so the fake fills the axis the reaction's stage actually carries,
// exercising the real per-stage encode.
type fakeProvider struct {
	kv  converge.KindVersion
	ran *string
}

func (p fakeProvider) Kind() converge.KindVersion     { return p.kv }
func (fakeProvider) OnConfig(converge.ProviderConfig) {}
func (fakeProvider) Ready() bool                      { return true }

func (p fakeProvider) Work(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	*p.ran = req.Reaction
	echo := json.RawMessage(`{"` + req.Reaction + `":1}`)
	if req.Trigger == converge.TriggerOperation {
		return converge.Outcome{OperationOutput: echo}, nil
	}
	return converge.Outcome{Status: echo}, nil
}

// TestExecutorRoutes proves the in-process test executor encodes each reaction's stage
// into a proto StageTask, runs it against the matching (kind, version) provider, and
// threads the provider's Outcome back — regardless of the reaction's Trigger/Emits. A
// task for a kind with no registered provider is a terminal error (no silent no-op).
func TestExecutorRoutes(t *testing.T) {
	mask := func(bits ...model.OutcomeBit) model.OutcomeMask { return bits }

	const kind = model.Kind("k")
	var ran string
	exec := inproc.New([]converge.Provider{
		fakeProvider{kv: converge.KindVersion{Kind: converge.Kind(kind), Version: 1}, ran: &ran},
	})

	cases := []struct {
		name string
		rx   model.ReactionDecl
		req  model.ReactionRequest
	}{
		{"compose", model.ReactionDecl{Name: "compose", Trigger: model.TriggerSpecChange, Emits: mask(model.OutcomeChildren, model.OutcomeStatus)}, model.ReactionRequest{}},
		{"work", model.ReactionDecl{Name: "work", Trigger: model.TriggerSpecChange, Emits: mask(model.OutcomeStatus)}, model.ReactionRequest{}},
		{"rollup", model.ReactionDecl{Name: "rollup", Trigger: model.TriggerChildrenSettled, Emits: mask(model.OutcomeStatus)}, model.ReactionRequest{}},
		{"delete", model.ReactionDecl{Name: "delete", Trigger: model.TriggerDeleteRequested, Emits: mask(model.OutcomeFinalizer), Finalizer: "f"}, model.ReactionRequest{}},
		{"operate", model.ReactionDecl{Name: "operate", Trigger: model.TriggerOperation, Verb: "v", Emits: mask(model.OutcomeOperationOutput)}, model.ReactionRequest{Operation: &model.Operation{Verb: "v"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ran = ""
			out, failed, err := exec.DispatchStage(context.Background(), kind, 1, tc.rx, tc.req)
			if err != nil {
				t.Fatalf("err: %v (failed stage %d)", err, failed)
			}
			if ran != tc.name {
				t.Fatalf("provider ran reaction %q, want %q", ran, tc.name)
			}
			// compose/delete return their own (children/finalizer) outcome shape, not a
			// threaded status; the status-bearing stages echo the reaction name back.
			switch tc.name {
			case "compose", "delete":
				// no status assertion — those stages don't thread status back here
			case "operate":
				if want := `{"` + tc.name + `":1}`; string(out.OperationOutput) != want {
					t.Fatalf("operation output = %s, want %s", out.OperationOutput, want)
				}
			default:
				if want := `{"` + tc.name + `":1}`; string(out.Status) != want {
					t.Fatalf("status = %s, want %s", out.Status, want)
				}
			}
		})
	}

	// A task for a kind with no registered provider is a terminal error (no silent
	// no-op): the executor must report the missing (kind, version).
	t.Run("missing provider errors", func(t *testing.T) {
		_, _, err := exec.DispatchStage(context.Background(), model.Kind("absent"), 1,
			model.ReactionDecl{Name: "work", Trigger: model.TriggerSpecChange, Emits: mask(model.OutcomeStatus)},
			model.ReactionRequest{})
		if err == nil {
			t.Fatal("DispatchStage for an unregistered kind must error")
		}
	})
}
