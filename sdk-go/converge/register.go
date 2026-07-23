package converge

import "maps"

// register wires one Provider's (kind, version) into the worker's three maps: the wildcard
// handler (every reaction the core dispatches for this pair routes to Work), the
// providerconfig-push callback (OnConfig, the whole {Spec, Data} monolith), and the
// readiness probe (Ready). kindVersion is explicit (>= 1) — a 0-version kind is a
// registration bug the runner rejects. Called by Serve, before run.
func (w *worker) register(p Provider) {
	kv := p.Kind()
	w.mu.Lock()
	defer w.mu.Unlock()
	byReaction := w.handlers[kv]
	if byReaction == nil {
		byReaction = map[string]ReactionHandler{}
		w.handlers[kv] = byReaction
	}
	byReaction[reactionWildcard] = providerHandler{p: p}
	w.onConfig[kv] = p.OnConfig
	w.health[kv] = p.Ready
}

// runtimeSnapshot materializes the registered handlers into the []kindRuntime the runner
// needs + the config-cache pairs, under one lock. Called once at the top of run. A pair
// with a 0/negative kindVersion is a registration bug — the runner's newRunner rejects it
// (there is no implicit v1), so it surfaces there as a hard error rather than silently
// advertising the wrong version.
func (w *worker) runtimeSnapshot() ([]kindRuntime, []KindVersion) {
	w.mu.Lock()
	defer w.mu.Unlock()
	runtimes := make([]kindRuntime, 0, len(w.handlers))
	pairs := make([]KindVersion, 0, len(w.handlers))
	for kv, byReaction := range w.handlers {
		handlers := make(map[string]ReactionHandler, len(byReaction))
		maps.Copy(handlers, byReaction)
		runtimes = append(runtimes, kindRuntime{Kind: kv.Kind, KindVersion: kv.Version, Handlers: handlers})
		pairs = append(pairs, kv)
	}
	return runtimes, pairs
}

// healthSnapshot copies the registered readiness probes for the poller (read-only during
// run). Empty → the poller idles (every kind always ready).
func (w *worker) healthSnapshot() map[KindVersion]func() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make(map[KindVersion]func() bool, len(w.health))
	maps.Copy(out, w.health)
	return out
}

// configCallback returns the registered OnConfig callback for a pushed (kind,ver), or nil.
// Read on the providerconfig-push path.
func (w *worker) configCallback(kind Kind, kindVersion int) func(ProviderConfig) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.onConfig[KindVersion{Kind: kind, Version: kindVersion}]
}
