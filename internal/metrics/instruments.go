package metrics

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// This file defines the converge instrument set. Instruments are registered from
// cmd/converge against live value SOURCES passed as small closures/interfaces, so
// this package depends on no broker/runtime/pgx type (no import cycle, stays
// generic). OBSERVABLE instruments (gauges/observable-counters) read their source
// at scrape time — zero hot-path cost, no double-bookkeeping against the existing
// atomic counters / pgxpool.Stat / connected-worker snapshot. All are no-ops on a
// disabled Provider (Meter() is a no-op meter).

// PoolStat is the subset of pgxpool.Stat the DB-pool gauges read (kept as an
// interface so the metrics package doesn't import pgx).
type PoolStat interface {
	TotalConns() int32
	IdleConns() int32
	AcquiredConns() int32
	AcquireCount() int64
	EmptyAcquireCount() int64
}

// RegisterProcess registers converge_build_info (a constant 1 labelled with version
// + role, the standard build-info pattern) and converge_up (constant 1 while the
// process serves). Call once at startup.
func (p *Provider) RegisterProcess(version, role string) {
	m := p.Meter()
	buildInfo, err := m.Int64ObservableGauge("converge.build.info",
		metric.WithDescription("Build/version info; constant 1, labelled version and role."),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(1, metric.WithAttributes(
				attribute.String("version", version),
				attribute.String("role", role),
			))
			return nil
		}))
	if err != nil {
		slog.Warn("metrics: register build_info", "error", err)
	}
	_ = buildInfo
	up, err := m.Int64ObservableGauge("converge.up",
		metric.WithDescription("1 while the process is serving."),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(1)
			return nil
		}))
	if err != nil {
		slog.Warn("metrics: register up", "error", err)
	}
	_ = up
}

// RegisterDBPool registers gauges for the pgx connection pool, sampled from stat()
// at scrape time. stat returns the current PoolStat (call pool.Stat() inside it).
func (p *Provider) RegisterDBPool(stat func() PoolStat) {
	m := p.Meter()
	// A single observable gauge with a "state" attribute (total/idle/acquired) keeps
	// the series count small and mirrors how pool dashboards split conns.
	conns, err := m.Int64ObservableGauge("converge.db.pool.connections",
		metric.WithDescription("pgx pool connections by state (total/idle/acquired)."),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			s := stat()
			if s == nil {
				return nil
			}
			o.Observe(int64(s.TotalConns()), metric.WithAttributes(attribute.String("state", "total")))
			o.Observe(int64(s.IdleConns()), metric.WithAttributes(attribute.String("state", "idle")))
			o.Observe(int64(s.AcquiredConns()), metric.WithAttributes(attribute.String("state", "acquired")))
			return nil
		}))
	if err != nil {
		slog.Warn("metrics: register db pool connections", "error", err)
	}
	_ = conns
	// Cumulative acquire counters (monotonic) — empty acquires are the backpressure
	// signal (an acquire that had to wait for / open a conn).
	acquires, err := m.Int64ObservableCounter("converge.db.pool.acquires",
		metric.WithDescription("Cumulative pgx pool acquires."),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			if s := stat(); s != nil {
				o.Observe(s.AcquireCount())
			}
			return nil
		}))
	if err != nil {
		slog.Warn("metrics: register db pool acquires", "error", err)
	}
	_ = acquires
	emptyAcq, err := m.Int64ObservableCounter("converge.db.pool.empty_acquires",
		metric.WithDescription("Cumulative pgx pool acquires that found no idle conn (backpressure)."),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			if s := stat(); s != nil {
				o.Observe(s.EmptyAcquireCount())
			}
			return nil
		}))
	if err != nil {
		slog.Warn("metrics: register db pool empty_acquires", "error", err)
	}
	_ = emptyAcq
}

// BrokerSource supplies the broker's live counters at scrape time (implemented by
// *broker.Server — an interface here to avoid importing internal/broker).
type BrokerSource interface {
	// InFlight is the pod's total in-flight (dispatched, awaiting Complete) task count.
	InFlight() int
	// ConnectedWorkerCount is the number of live worker streams connected to this broker.
	ConnectedWorkerCount() int
	// MeshCounters returns the cumulative mesh fast-recovery counts.
	MeshCounters() (peerGoneWakes, foreignDropSignals, syntheticDelivered int64)
}

// BrokerCounters are the synchronous hot-path counters the broker increments
// directly (returned to the caller to hold + Add on). RegisterBroker always returns
// NON-nil instruments — a disabled Provider's no-op meter still hands back a non-nil
// no-op struct — so Add is safe there. But the ZERO VALUE (a nil metric.Int64Counter,
// e.g. a directly-constructed config that never called RegisterBroker) is NOT safe:
// calling .Add on a nil interface PANICS. Hence the call sites nil-guard before Add.
type BrokerCounters struct {
	// Claimed counts stage tasks dispatched to a worker.
	Claimed metric.Int64Counter
	// Completed counts stage results resolved (by outcome: success|transient|terminal).
	Completed metric.Int64Counter
}

// RegisterBroker returns the synchronous dispatch counters (claimed/completed) so
// they can be wired into the broker duty AT CONSTRUCTION (before Start). Safe on a
// disabled Provider (nil-ish no-op counters). Pair with RegisterBrokerGauges after
// the engine is live. On a non-broker pod the counters simply never increment.
func (p *Provider) RegisterBroker() BrokerCounters {
	m := p.Meter()
	claimed, err := m.Int64Counter("converge.broker.dispatch_claimed",
		metric.WithDescription("Stage tasks claimed + dispatched to a worker."))
	if err != nil {
		slog.Warn("metrics: register broker dispatch_claimed", "error", err)
	}
	completed, err := m.Int64Counter("converge.broker.dispatch_completed",
		metric.WithDescription("Stage results resolved, by outcome."))
	if err != nil {
		slog.Warn("metrics: register broker dispatch_completed", "error", err)
	}
	return BrokerCounters{Claimed: claimed, Completed: completed}
}

// RegisterBrokerGauges wires the broker observable gauges (read from src at scrape
// time). Call once on a broker pod AFTER the engine is live. No-op on a disabled
// Provider (no-op meter).
func (p *Provider) RegisterBrokerGauges(src BrokerSource) {
	m := p.Meter()
	if _, err := m.Int64ObservableGauge("converge.broker.inflight_tasks",
		metric.WithDescription("Stage tasks dispatched and awaiting completion on this broker."),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(src.InFlight()))
			return nil
		})); err != nil {
		slog.Warn("metrics: register broker inflight", "error", err)
	}
	if _, err := m.Int64ObservableGauge("converge.broker.connected_workers",
		metric.WithDescription("Dumb worker streams connected to this broker."),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(src.ConnectedWorkerCount()))
			return nil
		})); err != nil {
		slog.Warn("metrics: register broker connected_workers", "error", err)
	}
	if _, err := m.Int64ObservableCounter("converge.broker.mesh_events",
		metric.WithDescription("Cumulative broker-mesh fast-recovery events by kind."),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			wakes, drops, delivered := src.MeshCounters()
			o.Observe(wakes, metric.WithAttributes(attribute.String("event", "peer_gone_wakes")))
			o.Observe(drops, metric.WithAttributes(attribute.String("event", "foreign_drop_signals")))
			o.Observe(delivered, metric.WithAttributes(attribute.String("event", "synthetic_delivered")))
			return nil
		})); err != nil {
		slog.Warn("metrics: register broker mesh_events", "error", err)
	}
}

// ControlCounters are the control-plane sweeper counters (synchronous, incremented
// after each duty tick with the row count it acted on). One counter with a "sweep"
// attribute keeps the series compact.
type ControlCounters struct {
	// Swept adds rows a sweeper acted on, labelled by sweep (reaper_stale,
	// reaper_rescheduled, reaper_unclaimed_deleted, orphans_swept, resources_removed,
	// specgc_reclaimed, members_reclaimed, resync_repended).
	Swept metric.Int64Counter
}

// RegisterControl wires the control-plane sweeper counter. Call once on a control pod.
func (p *Provider) RegisterControl() ControlCounters {
	m := p.Meter()
	swept, err := m.Int64Counter("converge.control.sweeper_rows",
		metric.WithDescription("Rows a control-plane sweeper acted on, by sweep."))
	if err != nil {
		slog.Warn("metrics: register control sweeper_rows", "error", err)
	}
	return ControlCounters{Swept: swept}
}
