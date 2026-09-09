package fault

import (
	"testing"

	"github.com/salesforce/converge/sdk-go/converge"
)

// TestRateZeroNeverFails proves the zero value (and rate 0) is a clean
// pass-through — a worker built without the env wired never injects, so `just
// demo` is unaffected by the feature.
func TestRateZeroNeverFails(t *testing.T) {
	for _, in := range []Injector{{}, {Rate: 0}, {Rate: -1}} {
		for range 10_000 {
			if in.Roll() {
				t.Fatalf("Injector%+v rolled a fault at rate<=0", in)
			}
		}
	}
}

// TestRateOneAlwaysFails proves rate>=1 always injects (the deterministic upper
// bound), with no random draw.
func TestRateOneAlwaysFails(t *testing.T) {
	for _, in := range []Injector{{Rate: 1}, {Rate: 1.5}} {
		for range 1_000 {
			if !in.Roll() {
				t.Fatalf("Injector%+v failed to roll a fault at rate>=1", in)
			}
		}
	}
}

// TestRateIsApproximate proves a mid rate injects roughly that fraction over many
// draws — the statistical contract the demo relies on (≈rate of resources flap).
// Wide tolerance keeps it non-flaky while still catching a wired-wrong rate.
func TestRateIsApproximate(t *testing.T) {
	const n = 100_000
	const rate = 0.3
	in := Injector{Rate: rate}
	hits := 0
	for range n {
		if in.Roll() {
			hits++
		}
	}
	got := float64(hits) / n
	if got < rate-0.03 || got > rate+0.03 {
		t.Fatalf("observed fault rate %.3f, want ≈%.2f (±0.03)", got, rate)
	}
}

// TestFailIsTransient proves the injected error is TRANSIENT (retryable), not
// terminal — so the engine re-queues the resource and it can heal. A terminal
// fault would stick Failed forever, defeating the retry demo.
func TestFailIsTransient(t *testing.T) {
	err := Injector{Rate: 1}.Fail("vpc", "vpc-team-x")
	if err == nil {
		t.Fatal("Fail returned nil")
	}
	if converge.IsTerminal(err) {
		t.Fatalf("injected fault must be TRANSIENT (retryable), got terminal: %v", err)
	}
	// The message names the resource so the worker log shows what's flapping.
	if want := "vpc/vpc-team-x"; !contains(err.Error(), want) {
		t.Fatalf("error %q should name the resource %q", err.Error(), want)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
