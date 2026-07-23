// Package runtime is the per-pod runtime that drives providers. It is the
// analogue of K8s controller-runtime's manager + worker-queue machinery:
// kind-agnostic plumbing that runs on every pod and turns the persistent state
// in `resources` / `work_queue` / `work_outbox` / `lifecycle_outbox` into actual
// provider invocations. It is manifest-driven: it dispatches off a kind's
// declared reactions (read from the KindManifestCache), never off a kind name.
//
// The drivers here are independent goroutine loops; a pod runs the subset its
// duties (see internal/engine) select. Grouped by who owns them:
//
//	Dispatcher (claim loop — the broker's, via internal/broker) — loop.go
//	  Round-robins the per-(kind,task_type) work_queue claim (FOR UPDATE SKIP
//	  LOCKED, equality probe + partition pruning + per-kind global cap), keeps
//	  each pair filled to its cap, runs ONE heartbeat over all in-flight tasks,
//	  and feeds each claimed task to a StageDispatcher. The reconcile task runs
//	  a kind's compose?/work?/rollup? reactions in one invocation; operate runs
//	  the operation reaction; the shared reaction-application body is reaction_engine.go.
//
//	Drainer (control) — drainer.go
//	  Pops batches from work_outbox and applies the deltas to resources +
//	  work_queue in one tx (synced_gen advance + health_ok — which flip the
//	  generated is_ready — status write, resource_conditions upsert, reactive dep
//	  cascade). Compose results bypass it (children commit inline).
//
//	Reaper (control) — reaper.go
//	  Time-driven sweeper. Re-arms rows the trigger machinery missed (deps just
//	  ready, failed past retry, stale claims) and sweeps expired orphans. Calls
//	  the plpgsql reap_stale_work / requeue_failed_and_pending / sweep_expired_orphans.
//
//	Resyncer (control, only for kinds that opted in) — resyncer.go
//	  Drift-detection sweeper: re-pends settled resources WITHOUT bumping
//	  generation so the provider re-observes health. Calls requeue_for_resync.
//
//	ReactorDispatcher (broker) — reactor.go
//	  Drains lifecycle_outbox and ships each matched transition's reaction
//	  (STAGE_REACT) through the same StageDispatcher as the claim loop, acking
//	  only after a successful delivery (at-least-once).
//
//	KindManifestCache — kind_manifest_cache.go
//	  Lock-free snapshot of kind_manifest (the CRDs), refreshed via a
//	  NotifyRefresher on kind_manifest_changed; the claim/dispatch reads a kind's
//	  reactions + policy from it (the API validates schemas off a separate view).
//
//	ProviderConfigCache — provider_config_cache.go
//	  Boot + live per-kind default providerconfig, refreshed via a NotifyRefresher
//	  on providerconfig_changed; the broker serves it to dumb workers.
//
//	NotifyRefresher — notify_refresher.go
//	  The shared "keep an in-memory view convergent with a DB table" driver: a
//	  LISTEN on a NOTIFY channel (prompt) + a failsafe reload tick (a dropped
//	  NOTIFY still converges). KindManifestCache + ProviderConfigCache build on it, as does
//	  the API pod's kind-schema surface (cmd/converge).
//
//	Resharder — resharder.go
//	  Recomputes this pod's owned shard range on cluster-membership change and
//	  swaps the shared ShardSet the other drivers read each tick.
//
//	ClusterMemberReporter — the running-fleet heartbeat (one per process).
//
// All are idempotent and shard-aware. If a pod dies mid-call the Reaper re-pends
// and the next claim re-runs; if Postgres dies the reaper's full-table sweep
// recovers the queue from `resources` (so work_queue / work_outbox can be UNLOGGED).
//
// What this package is NOT
//
//   - Not the model. The reaction model (ReactionDecl, StageDispatcher,
//     Outcome) lives in internal/model/, and provider handler types in
//     sdk-go/converge/. This package consumes them; it does not define them.
//   - Not the providers. Concrete kinds live in their own provider packages
//     (see examples/demos/ for reference providers). A provider author never
//     needs to read this package.
//   - Not boot/wiring. internal/engine/ assembles a pod's duty list from these
//     drivers; main.go calls into engine, never directly into runtime.
package runtime
