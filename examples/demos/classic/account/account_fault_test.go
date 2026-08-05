package account

import (
	"encoding/json"
	"testing"

	"github.com/salesforce/converge/examples/demos/classic/fault"
	"github.com/salesforce/converge/sdk-go/converge"
)

// TestNoFaultReconciles proves the default (no injector) path is unchanged: the
// account worker writes its status and returns no error — so `just demo` (rate 0)
// converges exactly as before the feature.
func TestNoFaultReconciles(t *testing.T) {
	p := Provider{} // zero injector — never faults
	out, err := p.Work(t.Context(), converge.ReactionRequest{
		Resource: converge.Resource{Kind: Kind, Name: "acc-team-x", Spec: json.RawMessage(`{"team_name":"team-x"}`)},
	})
	if err != nil {
		t.Fatalf("no-fault React errored: %v", err)
	}
	var st AccountStatus
	if err := json.Unmarshal(out.Status, &st); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if st.AccountID != "acc-stub-team-x" {
		t.Fatalf("account_id = %q, want acc-stub-team-x", st.AccountID)
	}
}

// TestFaultIsTransientAndEmitsNothing proves an injected fault (rate 1) returns a
// TRANSIENT error and NO status — so the engine retries the resource (it can
// heal) and never records a partial/garbage status from a failed attempt.
func TestFaultIsTransientAndEmitsNothing(t *testing.T) {
	p := Provider{faults: fault.Injector{Rate: 1}} // always faults
	out, err := p.Work(t.Context(), converge.ReactionRequest{
		Resource: converge.Resource{Kind: Kind, Name: "acc-team-x", Spec: json.RawMessage(`{"team_name":"team-x"}`)},
	})
	if err == nil {
		t.Fatal("expected an injected fault error")
	}
	if converge.IsTerminal(err) {
		t.Fatalf("injected fault must be TRANSIENT, got terminal: %v", err)
	}
	if out.Status != nil {
		t.Fatalf("a faulted reconcile must emit no status, got %s", out.Status)
	}
}
