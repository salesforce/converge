package runtime

import (
	"testing"
	"time"

	"github.com/salesforce/converge/internal/model"
)

// TestResyncerSetKinds covers the live-refresh surface the control plane uses to
// retune a RUNNING Resyncer from kind_config with no restart: SetKinds replaces
// the map atomically, drops disabled (Interval==0) entries, and kindsSnapshot
// hands the sweeper a stable copy.
func TestResyncerSetKinds(t *testing.T) {
	r := &Resyncer{kinds: map[model.Kind]ResyncKind{
		"a": {Interval: time.Minute},
	}}

	// Replace: a retuned, b added, the old "a" interval overwritten.
	r.SetKinds(map[model.Kind]ResyncKind{
		"a": {Interval: 2 * time.Minute, Recompose: true},
		"b": {Interval: 30 * time.Second},
		"c": {Interval: 0}, // disabled → must be dropped
	})

	got := r.kindsSnapshot()
	if len(got) != 2 {
		t.Fatalf("snapshot size = %d, want 2 (c is disabled)", len(got))
	}
	if got["a"].Interval != 2*time.Minute || !got["a"].Recompose {
		t.Errorf("kind a = %+v, want {2m, recompose}", got["a"])
	}
	if got["b"].Interval != 30*time.Second {
		t.Errorf("kind b = %+v, want {30s}", got["b"])
	}
	if _, ok := got["c"]; ok {
		t.Error("kind c (Interval=0) must be dropped, not registered")
	}

	// The snapshot is a COPY: mutating it must not affect the Resyncer.
	got["a"] = ResyncKind{Interval: time.Hour}
	if r.kindsSnapshot()["a"].Interval != 2*time.Minute {
		t.Error("kindsSnapshot must return a copy, not the live map")
	}

	// Clearing to empty stops all resync (a kind removed from kind_config).
	r.SetKinds(map[model.Kind]ResyncKind{})
	if len(r.kindsSnapshot()) != 0 {
		t.Error("SetKinds(empty) should clear the map")
	}
}
