package runtime

import "testing"

// TestResharderDefaults confirms NewResharder seeds the shard-space cardinality; the
// LivenessWindow is set by the caller (the TopologyWatcher's shared window), so it is
// intentionally zero until then. The reactive loop that debounces cluster_changed
// bursts lives in the TopologyWatcher now (see topologywatcher_test.go), driven by
// the shared runtime.Debounce primitive.
func TestResharderDefaults(t *testing.T) {
	r := NewResharder(nil, "m", NewShardSet(nil))
	if r.Total != NumShards {
		t.Fatalf("Total = %d; want NumShards=%d", r.Total, NumShards)
	}
	if r.LivenessWindow != 0 {
		t.Fatalf("LivenessWindow = %v; want 0 (set by the watcher)", r.LivenessWindow)
	}
}
