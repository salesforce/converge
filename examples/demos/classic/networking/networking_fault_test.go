package networking

import (
	"encoding/json"
	"testing"

	"github.com/salesforce/converge/examples/demos/classic/fault"
	"github.com/salesforce/converge/sdk-go/converge"
)

// netWorkers builds one of each networking worker with the given injector, plus a
// valid spec for it — so a single table drives vpc/tgw/route fault behavior.
func netWorkers(in fault.Injector) []struct {
	name string
	rt   converge.ReactionHandler
	spec json.RawMessage
} {
	return []struct {
		name string
		rt   converge.ReactionHandler
		spec json.RawMessage
	}{
		{"vpc", vpcWorker{faults: in}, json.RawMessage(`{"account_id":"123456789012","cidr":"10.0.0.0/16"}`)},
		{"tgw", tgwWorker{faults: in}, json.RawMessage(`{"name":"tgw-x"}`)},
		{"route", routeWorker{faults: in}, json.RawMessage(`{"vpc_id":"vpc-1","tgw_id":"tgw-1"}`)},
	}
}

// TestNetNoFaultReconciles proves the default (no injector) path writes a status
// and returns no error for all three networking kinds — `just demo` unaffected.
func TestNetNoFaultReconciles(t *testing.T) {
	for _, w := range netWorkers(fault.Injector{}) {
		t.Run(w.name, func(t *testing.T) {
			out, err := w.rt.React(t.Context(), converge.ReactionRequest{
				Resource: converge.Resource{Name: w.name + "-x", Spec: w.spec},
			})
			if err != nil {
				t.Fatalf("%s no-fault React errored: %v", w.name, err)
			}
			if len(out.Status) == 0 {
				t.Fatalf("%s no-fault React emitted no status", w.name)
			}
		})
	}
}

// TestNetFaultIsTransientAndEmitsNothing proves an injected fault (rate 1) on each
// networking kind returns a TRANSIENT error and NO status — so the engine retries
// and the resource can heal, not stick Failed.
func TestNetFaultIsTransientAndEmitsNothing(t *testing.T) {
	for _, w := range netWorkers(fault.Injector{Rate: 1}) {
		t.Run(w.name, func(t *testing.T) {
			out, err := w.rt.React(t.Context(), converge.ReactionRequest{
				Resource: converge.Resource{Name: w.name + "-x", Spec: w.spec},
			})
			if err == nil {
				t.Fatalf("%s: expected an injected fault error", w.name)
			}
			if converge.IsTerminal(err) {
				t.Fatalf("%s: injected fault must be TRANSIENT, got terminal: %v", w.name, err)
			}
			if out.Status != nil {
				t.Fatalf("%s: a faulted reconcile must emit no status, got %s", w.name, out.Status)
			}
		})
	}
}
