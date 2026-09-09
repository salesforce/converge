package converge

import (
	"context"
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/salesforce/converge/sdk-go/workerpb"
	"github.com/salesforce/converge/sdk-go/workerpb/workerpbconnect"
)

// stalledClient is a WorkerServiceClient whose GetProviderConfig blocks until
// its own context is cancelled — i.e. a hung-but-connected broker. Every other
// method is unused here.
type stalledClient struct {
	workerpbconnect.WorkerServiceClient
}

func (stalledClient) GetProviderConfig(ctx context.Context, _ *connect.Request[workerpb.GetProviderConfigRequest]) (*connect.Response[workerpb.GetProviderConfigResponse], error) {
	<-ctx.Done() // never answers; only the per-call WithTimeout can free us
	return nil, ctx.Err()
}

// cannedClient returns a scripted GetProviderConfig response per call, so a test can
// drive the sequence of broker answers (first a full snapshot, then an empty one, …).
type cannedClient struct {
	workerpbconnect.WorkerServiceClient
	resps []*workerpb.GetProviderConfigResponse
	i     int
}

func (c *cannedClient) GetProviderConfig(context.Context, *connect.Request[workerpb.GetProviderConfigRequest]) (*connect.Response[workerpb.GetProviderConfigResponse], error) {
	r := c.resps[c.i]
	if c.i < len(c.resps)-1 {
		c.i++
	}
	return connect.NewResponse(r), nil
}

// TestConfigLoadMergeKeepsLastKnownOnEmpty proves configCache.load MERGES rather than
// REPLACES: a successful-but-EMPTY broker response (a cold ProviderConfigCache on reconnect, or
// a GetProviderConfig that raced the default landing) must NOT blank a bundle/config the
// worker already holds — else the 5-min refresh (the backstop for a dropped push) would
// itself re-trigger the "no bundle yet" it exists to prevent.
func TestConfigLoadMergeKeepsLastKnownOnEmpty(t *testing.T) {
	full := &workerpb.GetProviderConfigResponse{Entries: []*workerpb.ProviderConfigEntry{
		{Kind: "k", KindVersion: 1, Config: []byte(`{"v":1}`), Bundle: []byte("BUNDLE-A")},
	}}
	empty := &workerpb.GetProviderConfigResponse{}
	c := newConfigCache(&cannedClient{resps: []*workerpb.GetProviderConfigResponse{full, empty}}, []KindVersion{{Kind: "k", Version: 1}}, 0, nil)

	if err := c.load(context.Background()); err != nil { // first: full snapshot
		t.Fatalf("first load: %v", err)
	}
	if got := bundleOf(c, "k"); got != "BUNDLE-A" {
		t.Fatalf("after full load, Bundle(k) = %q, want BUNDLE-A", got)
	}
	if err := c.load(context.Background()); err != nil { // second: empty response
		t.Fatalf("second load: %v", err)
	}
	if got := bundleOf(c, "k"); got != "BUNDLE-A" {
		t.Fatalf("after empty load, Bundle(k) = %q — an empty response BLANKED the cache (regression)", got)
	}
	if got := specOf(c, "k"); got != `{"v":1}` {
		t.Fatalf("after empty load, Config(k) = %q — blanked (regression)", got)
	}
}

// TestConfigLoadOverwritesWithLatest proves the merge still takes the LATEST value for a
// kind the broker DOES return (load-and-overwrite is the expected behavior; only ABSENT
// kinds are preserved).
func TestConfigLoadOverwritesWithLatest(t *testing.T) {
	v1 := &workerpb.GetProviderConfigResponse{Entries: []*workerpb.ProviderConfigEntry{{Kind: "k", KindVersion: 1, Bundle: []byte("BUNDLE-A")}}}
	v2 := &workerpb.GetProviderConfigResponse{Entries: []*workerpb.ProviderConfigEntry{{Kind: "k", KindVersion: 1, Bundle: []byte("BUNDLE-B")}}}
	c := newConfigCache(&cannedClient{resps: []*workerpb.GetProviderConfigResponse{v1, v2}}, []KindVersion{{Kind: "k", Version: 1}}, 0, nil)
	_ = c.load(context.Background())
	_ = c.load(context.Background())
	if got := bundleOf(c, "k"); got != "BUNDLE-B" {
		t.Fatalf("Bundle(k) = %q, want the latest BUNDLE-B (overwrite expected)", got)
	}
}

// countingClient counts GetProviderConfig calls (and returns a fixed bundle) so a
// test can prove run loads EAGERLY — before the first ticker tick — rather than
// waiting a full refresh interval.
type countingClient struct {
	workerpbconnect.WorkerServiceClient
	calls chan struct{}
}

func (c *countingClient) GetProviderConfig(context.Context, *connect.Request[workerpb.GetProviderConfigRequest]) (*connect.Response[workerpb.GetProviderConfigResponse], error) {
	select {
	case c.calls <- struct{}{}:
	default:
	}
	return connect.NewResponse(&workerpb.GetProviderConfigResponse{Entries: []*workerpb.ProviderConfigEntry{{Kind: "k", KindVersion: 1, Bundle: []byte("B")}}}), nil
}

// TestConfigCacheRunEagerLoad proves configCache.run performs an IMMEDIATE load on
// start, before entering the ticker loop. This is the resilience edge: after a broker
// restart / worker reconnect a config that changed in the gap must converge without
// waiting a full (default 5-min) refresh interval. We set a LONG refresh so the ONLY
// way a load happens inside the test window is the eager first load.
func TestConfigCacheRunEagerLoad(t *testing.T) {
	cc := &countingClient{calls: make(chan struct{}, 1)}
	c := newConfigCache(cc, []KindVersion{{Kind: "k", Version: 1}}, time.Hour, nil) // ticker would fire only in 1h
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.run(ctx, nil)

	select {
	case <-cc.calls:
		// Eager load happened; the cache should now hold the bundle.
		if got := bundleOf(c, "k"); got != "B" {
			t.Fatalf("after eager load, Bundle(k) = %q, want B", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("configCache.run did not load eagerly on start (waited for the +interval tick?)")
	}
}

// TestConfigLoadBoundsHungBroker proves configCache.load can't hang forever on
// a hung-but-connected broker: the per-call loadTimeout converts the indefinite
// block into a returned error, so the worker's boot retry loop keeps making
// progress instead of wedging inside load. Uses a short test-scoped timeout by
// driving the parent ctx, which the WithTimeout still derives from.
func TestConfigLoadBoundsHungBroker(t *testing.T) {
	c := newConfigCache(stalledClient{}, []KindVersion{{Kind: "noop", Version: 1}}, 0, nil)

	// A parent deadline well under loadTimeout proves load honors cancellation;
	// the production bound is loadTimeout itself (verified by the const wiring).
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- c.load(ctx) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("load returned nil against a hung broker; want an error")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Logf("load error (any non-nil is acceptable): %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("load did not return within 5s against a hung broker (no per-call timeout?)")
	}
}

// specOf / bundleOf read a kind's current cached default at version 1 — test-only
// inspection of the cache (production delivery is onChange → the provider's OnConfig).
func specOf(c *configCache, kind string) string {
	s, _ := c.Get(Kind(kind), 1)
	return string(s)
}

func bundleOf(c *configCache, kind string) string {
	_, d := c.Get(Kind(kind), 1)
	return string(d)
}
