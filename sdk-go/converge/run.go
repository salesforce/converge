package converge

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// bootLoadCeiling caps the boot config-load backoff. A worker retries the initial
// GetProviderConfig with jittered backoff (the broker may not be up yet — k8s rollout
// ordering, a broker restart) rather than failing fast, since a worker that died here
// would never come back and its kinds would stall (the broker won't claim a kind with
// no ready subscriber). The ceiling bounds the retry interval, not the total wait.
const bootLoadCeiling = 30 * time.Second

// run connects to the broker and serves the worker's reactions until ctx is
// cancelled/deadlined, then drains in-flight work and returns. It BLOCKS and owns the
// WHOLE lifecycle — dialing, the boot config-load (retried with jittered backoff until
// the broker answers), the background config-refresh, the readiness poller, and
// reconnect-with-backoff across stream drops. Serve calls it; the caller owns ONLY the
// context: cancel it (or let its deadline fire) to shut down; run returns nil on that
// clean shutdown.
//
// A dropped stream is reconnected internally (never surfaced); run requires at least one
// registered provider (a worker that can execute nothing would poll forever) and returns
// an error only for a fatal setup problem (no kinds, a bad TLS config, an un-buildable
// runner).
func (w *worker) run(ctx context.Context) error {
	// Dial the broker (once) unless a test injected a Transport. Construction can't fail,
	// so the dial + its TLS-reload watcher start here, bound to ctx.
	if w.client == nil {
		client, err := w.dial(ctx)
		if err != nil {
			return err
		}
		w.client = client
	}

	runtimes, pairs := w.runtimeSnapshot()
	if len(runtimes) == 0 {
		return fmt.Errorf("converge: no providers registered")
	}

	// Prime the default-config cache at the FINAL registered pairs + load them (with
	// jittered backoff until the broker answers), so the per-task PULL sees live config
	// from the first task. The deferred-provider setup above already primed at the
	// version-blind boot pairs; this re-tracks the REAL versions the registered runtimes
	// carry (a v2 worker now pulls v2's default) — and covers workers that used only Handle.
	if err := w.primeAndLoad(ctx, pairs); err != nil {
		return err // ctx cancelled during the boot retry
	}

	rnr, err := newRunner(runnerConfig{
		Client:      w.client,
		Kinds:       runtimes,
		MaxInflight: w.cfg.maxInflight,
		Logger:      w.cfg.logger,
		DrainGrace:  w.cfg.drainGrace,
		// A broker providerconfig push (the whole monolith, spec + data) is applied to the
		// cache, which fires the author's OnConfig (via the cache's onChange, wired in
		// primeAndLoad) with the full {Spec, Data} — their hook to redial/recompile.
		OnConfigUpdate: func(kind Kind, kindVersion int, spec json.RawMessage, data []byte) {
			w.cache.apply(kind, kindVersion, spec, data)
		},
		// Re-pull the current defaults on every (re)connect so a reconnecting worker
		// converges to live config at once instead of waiting out the periodic refresh.
		OnConnect: func(cctx context.Context) {
			if err := w.cache.load(cctx); err != nil {
				w.cfg.logger.Warn("converge: config re-pull on connect failed (keeping last-known)", "err", err)
			}
		},
		// Initial readiness rides the Subscribe burst: any kind whose Ready probe is
		// currently false is advertised RS- BEFORE the broker can dispatch it, closing
		// the subscribe→first-poll window (a kind that boots degraded gets zero tasks).
		// Re-evaluated on every (re)connect (the runner calls it per Subscribe).
		InitialUnready: w.initialUnready,
	})
	if err != nil {
		return fmt.Errorf("converge: build runner: %w", err)
	}
	w.runnerMu.Lock()
	w.runner = rnr
	w.runnerMu.Unlock()

	// Background config-refresh (the failsafe behind the broker's live push) + the
	// health poller (emits RS+/RS- on probe edges). Both bound to ctx (stop on
	// shutdown) and span the whole reconnect loop below — the poller re-establishes
	// readiness on each reconnect via the Runner's InitialUnready, and only emits edges
	// itself. Started once, before the loop.
	go w.cache.run(ctx, func(err error) { w.cfg.logger.Warn("converge: config refresh failed (keeping last-known)", "err", err) })
	go runHealthPoller(ctx, rnr, w.healthSnapshot(), w.cfg.healthPoll)

	// Reconnect-with-backoff until ctx ends — the SDK owns this so the dev doesn't. Each
	// runner.Run is ONE stream session: it returns when the stream drops (reconnect) or,
	// on ctx cancel, after draining in-flight work (then the loop exits).
	reconnect := time.Second
	for ctx.Err() == nil {
		start := time.Now()
		serveErr := rnr.Run(ctx)
		if ctx.Err() != nil {
			break // clean shutdown: the last Run drained; done
		}
		// Reset the backoff after a session that stayed up longer than the current
		// backoff — a genuinely healthy stream, not a fast-failing flap. Without this,
		// one early blip would pin the backoff at the ceiling for the process lifetime.
		if time.Since(start) > reconnect {
			reconnect = time.Second
		}
		w.cfg.logger.Warn("converge: stream ended; reconnecting", "err", serveErr, "backoff", reconnect)
		// ±25% jitter: a broker crash drops every worker's stream at once, so an
		// un-jittered backoff reconnects them in lockstep — a thundering herd on the
		// surviving broker(s). Jitter decorrelates; the escalation uses the raw value.
		select {
		case <-time.After(jitter(reconnect)):
		case <-ctx.Done():
		}
		if reconnect < w.cfg.reconnectCeiling {
			reconnect *= 2
		}
	}
	return nil
}

// primeAndLoad ensures the config cache exists tracking `pairs`, then LOADS them from
// the broker — retrying with jittered exponential backoff until it succeeds or ctx is
// cancelled. The cache is built once (first call, wiring onChange → the provider's
// OnConfig) and re-tracked on later calls (setPairs), so a version-blind boot load and
// run's real-version load both route through here. Blocking-until-loaded matters: the load
// fires each provider's OnConfig with its primed default BEFORE the first task, so a
// config-bootstrapped provider has its config in hand as it comes up.
//
// Anti-thundering-herd: a fleet-wide broker restart fails every worker's
// GetProviderConfig together, so an un-jittered backoff would re-storm the recovered
// listener in lockstep; the escalation runs on the raw value, the sleep is jittered
// (the SDK-local jitter helper).
func (w *worker) primeAndLoad(ctx context.Context, pairs []KindVersion) error {
	if w.cache == nil {
		// onChange fires the provider's OnConfig on ANY default change — the boot prime,
		// a refresh that moved, or a broker push — so a provider always holds its current
		// default (delivered at startup, not only on a later edit). The full {Spec, Data}
		// monolith is passed whole; an empty pair (nil, nil) signals the default was deleted.
		onChange := func(kind Kind, kindVersion int, spec json.RawMessage, data []byte) {
			if fn := w.configCallback(kind, kindVersion); fn != nil {
				fn(ProviderConfig{Spec: spec, Data: data})
			}
		}
		w.cache = newConfigCache(w.client, pairs, w.cfg.configRefresh, onChange)
	} else {
		w.cache.setPairs(pairs)
	}
	delay := time.Second
	for {
		if err := w.cache.load(ctx); err == nil {
			return nil
		} else if ctx.Err() != nil {
			return ctx.Err()
		} else {
			w.cfg.logger.Warn("converge: broker not ready, retrying config load", "err", err, "backoff", delay)
			select {
			case <-time.After(jitter(delay)):
			case <-ctx.Done():
				return ctx.Err()
			}
			if delay < bootLoadCeiling {
				delay *= 2
			}
		}
	}
}
