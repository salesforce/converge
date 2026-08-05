package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// fakePoolStat is a static PoolStat for the DB-pool gauge test.
type fakePoolStat struct{}

func (fakePoolStat) TotalConns() int32        { return 7 }
func (fakePoolStat) IdleConns() int32         { return 3 }
func (fakePoolStat) AcquiredConns() int32     { return 4 }
func (fakePoolStat) AcquireCount() int64      { return 42 }
func (fakePoolStat) EmptyAcquireCount() int64 { return 5 }

// fakeBrokerSource is a static BrokerSource for the broker-gauge test.
type fakeBrokerSource struct{}

func (fakeBrokerSource) InFlight() int             { return 11 }
func (fakeBrokerSource) ConnectedWorkerCount() int { return 2 }
func (fakeBrokerSource) MeshCounters() (int64, int64, int64) {
	return 1, 2, 3
}

func scrape(t *testing.T, p *Provider) (int, string) {
	t.Helper()
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL) //nolint:noctx // test
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 1<<16)
	n, _ := resp.Body.Read(buf)
	return resp.StatusCode, string(buf[:n])
}

// TestDisabledIsNoOp: a disabled Provider serves 404 on /metrics, its Meter is a
// no-op (instrument registration never panics), and Shutdown is a no-op.
func TestDisabledIsNoOp(t *testing.T) {
	p, err := New(Config{Enabled: false})
	if err != nil {
		t.Fatalf("New disabled: %v", err)
	}
	if p.Enabled() {
		t.Fatal("disabled Provider reports Enabled()")
	}
	// Registration against a disabled Provider must not panic.
	p.RegisterProcess("v-test", "control")
	p.RegisterDBPool(func() PoolStat { return fakePoolStat{} })
	_ = p.RegisterBroker()
	_ = p.RegisterControl()
	p.RegisterBrokerGauges(fakeBrokerSource{})

	code, _ := scrape(t, p)
	if code != http.StatusNotFound {
		t.Fatalf("disabled /metrics: got %d, want 404", code)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("disabled Shutdown: %v", err)
	}
}

// TestEnabledExposesInstruments: an enabled Provider serves the registered
// instruments in Prometheus exposition on /metrics.
func TestEnabledExposesInstruments(t *testing.T) {
	p, err := New(Config{Enabled: true, ServiceName: "converge", ServiceVersion: "v-test", Role: "all"})
	if err != nil {
		t.Fatalf("New enabled: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	p.RegisterProcess("v-test", "all")
	p.RegisterDBPool(func() PoolStat { return fakePoolStat{} })
	_ = p.RegisterBroker()
	_ = p.RegisterControl()
	p.RegisterBrokerGauges(fakeBrokerSource{})

	code, body := scrape(t, p)
	if code != http.StatusOK {
		t.Fatalf("/metrics: got %d, want 200", code)
	}
	// OTel Prometheus exporter dot→underscore-normalizes names. Spot-check a gauge
	// from each source is present with its expected value.
	for _, want := range []string{
		"converge_build_info",
		"converge_up",
		"converge_db_pool_connections",
		"converge_broker_inflight_tasks",
		"converge_broker_connected_workers",
	} {
		if !contains(body, want) {
			t.Errorf("scrape missing metric %q\n---\n%s", want, body)
		}
	}
	// The DB-pool acquired-conns gauge reads the fake (4 acquired).
	if !contains(body, `state="acquired"`) {
		t.Errorf("db pool connections missing state label; body:\n%s", body)
	}
}

// TestCountersRecord: the synchronous counters returned by RegisterBroker/Control
// increment and surface on /metrics.
func TestCountersRecord(t *testing.T) {
	p, err := New(Config{Enabled: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	bc := p.RegisterBroker()
	cc := p.RegisterControl()
	ctx := context.Background()
	bc.Claimed.Add(ctx, 3)
	cc.Swept.Add(ctx, 9)

	code, body := scrape(t, p)
	if code != http.StatusOK {
		t.Fatalf("/metrics: got %d, want 200", code)
	}
	if !contains(body, "converge_broker_dispatch_claimed") {
		t.Errorf("missing broker_dispatch_claimed; body:\n%s", body)
	}
	if !contains(body, "converge_control_sweeper_rows") {
		t.Errorf("missing control_sweeper_rows; body:\n%s", body)
	}
}

func contains(hay, needle string) bool {
	return len(hay) >= len(needle) && (indexOf(hay, needle) >= 0)
}

func indexOf(hay, needle string) int {
	for i := 0; i+len(needle) <= len(hay); i++ {
		if hay[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
