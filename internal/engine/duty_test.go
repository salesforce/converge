package engine

import (
	"testing"
)

// TestDutyNames pins the duty identifiers used in logs and the cluster view.
// There is no "dispatch" duty: all provider execution is remote (the claim duty
// is the server-side surface; dumb workers run the stages).
func TestDutyNames(t *testing.T) {
	if got := ControlDuty(&ControlPlaneConfig{}).Name(); got != "control" {
		t.Errorf("control duty name = %q, want %q", got, "control")
	}
	if got := ClaimDuty(&BrokerConfig{}).Name(); got != "claim" {
		t.Errorf("claim duty name = %q, want %q", got, "claim")
	}
}

// dutyNames returns each duty's Name() in order — the shape the resolver tests
// assert against.
func dutyNames(ds []Duty) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = d.Name()
	}
	return out
}

// TestDutiesFromConfig covers the config→duties resolver: role derivation
// (control bundle first, claim last). The claim duty is the server-side dispatch
// surface; there is no in-process "dispatch" duty (removed with native-SQL
// workers), and the duty set is derived purely from ROLE (RunControl/RunBroker).
func TestDutiesFromConfig(t *testing.T) {
	tests := []struct {
		name    string
		cfg     EngineConfig
		want    []string // expected duty names in order
		wantErr bool
	}{
		{
			name: "role=all (broker) -> control then claim",
			cfg:  EngineConfig{RunControl: true, RunBroker: true},
			want: []string{"control", "claim"},
		},
		{
			name: "role=control -> control only, no claim",
			cfg:  EngineConfig{RunControl: true},
			want: []string{"control"},
		},
		{
			// THE prod ROLE=broker reality: RunBroker alone builds the claim duty.
			// The broker claims every manifested kind (no compiled-in kind list).
			name: "role=broker -> claim only, no control",
			cfg:  EngineConfig{RunBroker: true},
			want: []string{"claim"},
		},
		{
			name: "no control, no broker -> empty duty list (legal)",
			cfg:  EngineConfig{},
			want: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DutiesFromConfig(tc.cfg)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got duties %v", dutyNames(got))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			names := dutyNames(got)
			if len(names) != len(tc.want) {
				t.Fatalf("duties = %v, want %v", names, tc.want)
			}
			for i := range names {
				if names[i] != tc.want[i] {
					t.Fatalf("duties = %v, want %v", names, tc.want)
				}
			}
		})
	}
}

// TestClaimDutyIsDispatchSurface guards the SILENT interface-satisfaction break that
// stranded live config/bundle pushes: Engine.Start caches the pod's provider-executing
// duty via a `d.(dispatchSurface)` type assertion, so if *claimDuty ever stops
// satisfying dispatchSurface (a method added to the interface but not the duty), the
// assertion fails QUIETLY — Engine.dispatch stays nil, and BroadcastConfig/BroadcastBundle
// (plus AddKindLive/InFlight/ConnectedWorkers) all no-op. A worker then never receives a
// live default-config/bundle push and only self-heals on the 5-min GetProviderConfig
// poll — long after a config/bundle-bootstrapped composer has exhausted its transient
// retries and terminal-failed "no bundle yet". This asserts the same thing Start does, so
// the break is a test (and, via the var _ dispatchSurface guard in duties.go, a compile)
// failure rather than a runtime strand.
func TestClaimDutyIsDispatchSurface(t *testing.T) {
	d := ClaimDuty(&BrokerConfig{})
	if _, ok := d.(dispatchSurface); !ok {
		t.Fatal("*claimDuty must satisfy dispatchSurface (Engine.Start caches it via that assertion; " +
			"a miss leaves Engine.dispatch nil → BroadcastConfig/BroadcastBundle no-op → workers never get live pushes)")
	}
}
