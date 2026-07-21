package engine

import (
	"context"
)

// controlDuty runs every schema-owning, sweeper-running responsibility (the
// ControlPlane bundle: drain + reap + specgc + resync + membergc, as a unit on a
// control pod). It does NOT execute provider code — the sweepers operate on
// work_queue / work_outbox / resources rows directly and read each kind's resync
// policy from the kind_config DB table, never a Setup'd client — so a pod that
// runs only this duty dials nothing.
type controlDuty struct {
	cfg     *ControlPlaneConfig
	control *ControlPlane
}

var _ Duty = (*controlDuty)(nil)

// ControlDuty builds the control sweepers from cfg. cfg.Pool is required; the
// rest default as in NewControlPlane. It takes NO registry — the control plane
// reads resync policy from the kind_config DB table, never a constructed
// provider.
func ControlDuty(cfg *ControlPlaneConfig) Duty { return &controlDuty{cfg: cfg} }

func (d *controlDuty) Name() string { return "control" }

func (d *controlDuty) Start(ctx context.Context, deps Deps) error {
	// Fold the shared Deps onto the config; explicit cfg fields win when already
	// set.
	if d.cfg.Pool == nil {
		d.cfg.Pool = deps.Pool
	}
	if d.cfg.Shards == nil {
		d.cfg.Shards = deps.Shards
	}
	cp, err := NewControlPlane(ctx, d.cfg)
	if err != nil {
		return err
	}
	d.control = cp
	return cp.Start(ctx)
}

// Stop joins the control sweepers. The Engine calls it as part of its uniform
// reverse-order duty-stop loop.
func (d *controlDuty) Stop(ctx context.Context) error {
	if d.control == nil {
		return nil
	}
	return d.control.Stop(ctx)
}
