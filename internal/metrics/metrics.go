// Package metrics wires OpenTelemetry metrics for the converge control-plane and
// broker tiers. It exports via a Prometheus /metrics endpoint (pull-based) mounted
// on the plaintext health listener (:8081) — so scraping never crosses the (m)TLS
// API or broker Connect ports, matching the Helm chart's metrics/ServiceMonitor
// scaffold. Off by default; enabled via Config.Enabled (OTEL_METRICS_ENABLED).
//
// Design notes:
//   - One process-wide Provider owns a Prometheus registry + an OTel MeterProvider
//     backed by the OTel Prometheus exporter. Handler() serves the registry.
//   - Instruments are OBSERVABLE where the value already lives in an existing
//     counter/gauge (broker fanoutMetrics, pgxpool.Stat, connected workers): a
//     callback reads the live value at scrape time — zero hot-path cost, no
//     double-bookkeeping. Synchronous counters are used only where a hot path
//     genuinely increments (dispatch claimed/completed).
//   - Disabled Provider is a safe no-op: Meter() returns a no-op meter, Handler()
//     returns 404, Shutdown() is a no-op. Callers never need nil checks.
package metrics

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
)

// meterName is the instrumentation scope for every converge instrument.
const meterName = "github.com/salesforce/converge"

// Config controls metrics wiring. Zero value = disabled (a no-op Provider).
type Config struct {
	// Enabled turns metrics on. When false, New returns a no-op Provider.
	Enabled bool
	// ServiceName labels the resource (OTEL_SERVICE_NAME). Defaults to "converge".
	ServiceName string
	// ServiceVersion is the build version, stamped on the resource.
	ServiceVersion string
	// Role ("control" | "broker" | "all") → the `converge.role` resource attribute,
	// so a scrape can tell which tier a pod ran.
	Role string
	// InstanceID is this pod's unique id (its memberID: hostname-pid-uuid8) →
	// service.instance.id, which the OTel semconv requires to be UNIQUE per instance.
	// Empty falls back to ServiceName (not unique — fine for a single-process run).
	InstanceID string
}

// Provider owns the metrics pipeline: a Prometheus registry, the OTel exporter that
// feeds it, and the MeterProvider instruments register against. A disabled Provider
// is fully inert.
type Provider struct {
	enabled  bool
	registry *prometheus.Registry
	mp       *sdkmetric.MeterProvider
	meter    metric.Meter
	stopOnce sync.Once // makes Shutdown idempotent (a second call is a no-op)
}

// New builds a Provider. When cfg.Enabled is false it returns a no-op Provider
// (Meter() → no-op meter, Handler() → 404, Shutdown() → nil) so callers stay
// branch-free. On a real exporter/provider error it returns the error (fail fast at
// boot rather than silently drop metrics).
func New(cfg Config) (*Provider, error) {
	if !cfg.Enabled {
		return &Provider{meter: noop.NewMeterProvider().Meter(meterName)}, nil
	}
	svc := cfg.ServiceName
	if svc == "" {
		svc = "converge"
	}
	reg := prometheus.NewRegistry()
	// The OTel Prometheus exporter registers itself as a Collector on reg; the OTel
	// MeterProvider then feeds it. WithoutScopeInfo/WithoutTargetInfo would trim the
	// otel_scope_* / target_info series, but we keep them (standard OTel-Prom output).
	exp, err := promexporter.New(promexporter.WithRegisterer(reg))
	if err != nil {
		return nil, fmt.Errorf("prometheus exporter: %w", err)
	}
	instanceID := cfg.InstanceID
	if instanceID == "" {
		instanceID = svc // not unique, but valid for a single-process/dev run
	}
	attrs := resource.NewSchemaless(
		semconv.ServiceName(svc),
		semconv.ServiceVersion(cfg.ServiceVersion),
		// service.instance.id MUST be unique per instance (semconv) → the pod's memberID.
		semconv.ServiceInstanceID(instanceID),
		// which tier the pod ran, so a dashboard can split control vs broker.
		attribute.String("converge.role", cfg.Role),
	)
	res, err := resource.Merge(resource.Default(), attrs)
	if err != nil {
		// A schema-URL mismatch from Merge is non-fatal for our schemaless attrs;
		// keep the merged base where possible, else fall back to the custom attrs so
		// metrics still export (losing only the sdk/process/host defaults).
		res = attrs
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exp),
		sdkmetric.WithResource(res),
	)
	return &Provider{
		enabled:  true,
		registry: reg,
		mp:       mp,
		meter:    mp.Meter(meterName),
	}, nil
}

// Enabled reports whether metrics are live (a real registry + exporter).
func (p *Provider) Enabled() bool { return p != nil && p.enabled }

// Meter returns the process meter instruments register against. Safe on a disabled
// Provider (returns a no-op meter), so instrumentation code needs no nil/enabled check.
func (p *Provider) Meter() metric.Meter {
	if p == nil || p.meter == nil {
		return noop.NewMeterProvider().Meter(meterName)
	}
	return p.meter
}

// Handler serves the Prometheus exposition for the registry. On a disabled Provider
// it 404s (so mounting it unconditionally is harmless). Mount at /metrics on the
// plaintext health listener.
func (p *Provider) Handler() http.Handler {
	if !p.Enabled() {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "metrics disabled", http.StatusNotFound)
		})
	}
	return promhttp.HandlerFor(p.registry, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
	})
}

// Shutdown flushes + stops the MeterProvider. Safe/no-op on a disabled Provider.
// Call late in the shutdown sequence (after the duties that emit metrics stop).
func (p *Provider) Shutdown(ctx context.Context) error {
	if !p.Enabled() || p.mp == nil {
		return nil
	}
	// Idempotent: the SDK's second Shutdown returns ErrReaderShutdown, so gate on
	// stopOnce (a second call is a clean no-op).
	var err error
	p.stopOnce.Do(func() { err = p.mp.Shutdown(ctx) })
	return err
}
