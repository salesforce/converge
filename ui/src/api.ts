// API client for Converge.
//
// Field shape mirrors the Go server's resourceInfo / resourceFull.
// Kubernetes identity model: a resource is addressed by its PUBLIC
// (kind, name) pair — that's what every route, filter, and link keys on.
// Canonical names: kind, name, owner_kind, owner_name, generation,
// synced_gen, is_ready, health_ok, phase, conditions, finalizers,
// deletion_requested_at, spec, status, labels, annotations,
// created_at, updated_at. The internal uuid (`uid`) is carried ONLY on
// the full detail fetch, as an opaque incarnation handle — it is NEVER
// used for addressing, routing, or as a navigation key.
//
// Readiness state is the server-computed `phase` scalar — the SINGLE
// source of truth, derived in the DB from two orthogonal axes plus a
// failure marker. The UI renders `r.phase` verbatim and NEVER re-derives
// it (re-deriving readiness client-side is what lets the rendered list
// disagree with a server-side filter):
//   - Synced:  synced_gen vs generation (spec reconciled).
//   - Ready:   health_ok (observed healthy right now — can be false even
//              when fully synced, e.g. a drift probe found it degraded).
//   - Failed:  failure_gen = generation (own reconcile hard-failed).
// `is_ready` is Synced AND Ready (plus not-deleting). The detail fetch
// also carries a synthesized `conditions` array (Synced/Ready/custom) for
// the "why"; classification keys off `phase`, never the conditions.
//
// The server also synthesizes owner_kind / owner_name onto every owned
// resource's labels map for filter-by-owner UI use.
//
// Every entity is a resource: owner_kind/owner_name absent → root, set →
// owned by another resource. The API is uniformly
// /api/resources/{kind}/{name}/*.

// retryReadAfterWrite is a React Query `retry` option that retries
// only on transient 404s. The API may be served from a read replica
// with async streaming lag; immediately after a POST/PUT, a GET that
// hits the replica can briefly return 404 until the new row replicates.
//
// It retries at most 3 times. A NON-404 (4xx/5xx) is never retried — it's
// surfaced immediately. A 404 is retried, but always paired with the short
// fixed retryDelayReadAfterWrite below: a genuinely-missing resource (a
// stale link, a deleted row, a typo'd id) then surfaces its "not found"
// error in ~1.5 s instead of sitting on the loading skeleton for the ~30 s
// that React Query's default exponential backoff (1·2·4·8·16 s) would take
// — which read as "the card just shows a grey box and never says 404".
//
// Use as: `useQuery({ ..., retry: retryReadAfterWrite,
//                     retryDelay: retryDelayReadAfterWrite })`.
export const retryReadAfterWrite = (failureCount: number, error: Error): boolean => {
  if (failureCount >= 3) return false;
  return error.message.startsWith('404');
};

// retryDelayReadAfterWrite is the fixed 500 ms between read-after-write
// retries (vs the default growing backoff), so 3 attempts span ~1.5 s —
// long enough to ride out replica lag, short enough that a real 404 is
// shown promptly.
export const retryDelayReadAfterWrite = (): number => 500;

// Kind is just a string — the real set of kinds is whatever manifests (CRDs)
// have been applied (GET /api/kinds). The UI never hard-codes kind names;
// dropdowns/chips are populated dynamically.
export type Kind = string;

// Phase is the resource's readiness state — the server's `phase` scalar,
// rendered verbatim. Six values, fixed precedence (Deleting > Failed >
// Orphaned > Degraded > Ready > Reconciling), computed in the DB so the list
// filter, counts, and badge can never disagree.
//   - Ready        synced + healthy (== is_ready)
//   - Reconciling  spec change in flight / never reconciled
//   - Degraded     synced but unhealthy / children not ready (Ready=False)
//   - Orphaned     the composer dropped this child; in the grace window before
//                  teardown — a re-emit re-adopts it, else the reaper sweeps it
//   - Failed       own reconcile hard-failed for the live spec
//   - Deleting      soft-delete in progress
export type Phase = 'Ready' | 'Reconciling' | 'Degraded' | 'Failed' | 'Deleting' | 'Orphaned' | 'Quarantined';

// Outcome of an Apply (manifest), kubectl-style:
//   - created     a new (kind,name) root was inserted
//   - configured  an existing root's spec/labels changed (generation bumped)
//   - unchanged   identical re-apply — no generation bump, nothing scheduled
export type ApplyResult = 'created' | 'configured' | 'unchanged';

export const PHASE_VALUES: Phase[] = ['Ready', 'Reconciling', 'Degraded', 'Failed', 'Deleting', 'Orphaned', 'Quarantined'];

// Condition mirrors metav1.Condition. The backend exposes two standard
// axes — Synced (spec reconciled; synced_gen vs generation) and Ready
// (observed healthy; health_ok) — plus any custom axes a provider
// reports. The Synced condition and happy-path Ready=True are
// synthesized server-side from the scalar fields; non-default Ready and
// custom conditions come from the resource_conditions table.
export interface Condition {
  type: string;
  status: 'True' | 'False' | 'Unknown';
  reason?: string;
  message?: string;
  last_transition_at?: string;
  observed_generation?: number;
}

// ResourceInfo is the parent/owner envelope shape on owner-scoped
// list responses (children, topology/leaves, summary). Carries
// both K8s readiness axes: synced_gen vs generation is Synced, health_ok
// is Ready/health, and is_ready is their AND (plus not-deleting).
export interface ResourceInfo {
  kind: Kind;
  // kind_version is the resource's pinned web-API version (v1, v2, …). The detail view
  // shows it; a resource routes to a worker serving its (kind, kind_version).
  kind_version: number;
  name: string;
  owner_kind?: string;
  owner_name?: string;
  generation: number;
  synced_gen: number;
  is_ready: boolean;
  health_ok: boolean;
  phase: Phase;
  conditions?: Condition[] | null;
  finalizers?: string[] | null;
  deletion_requested_at?: string | null;
  labels?: Record<string, string>;
  created_at: string;
  updated_at: string;
}

// ResourceListItem is the slim row shape returned by every list/page
// endpoint (resources, children, topology/leaves, subgraph
// nodes). It carries `phase` — the one classification signal — so list
// views render the badge directly and never re-derive it. Spec, status,
// finalizers, annotations, health_ok and conditions are NOT carried; the
// detail panel re-fetches via GET /api/resources/{kind}/{name} on click
// (which includes the full conditions array for the "why").
export interface ResourceListItem {
  kind: Kind;
  // kind_version is the row's pinned web-API version (v1, v2, …). Present on the
  // resources list rows (for the version filter + a "kind/vN" row tag);
  // omitted (→ treated as v1) on the leaner children/subgraph/topology rows.
  kind_version?: number;
  name: string;
  owner_kind?: string;
  owner_name?: string;
  generation: number;
  synced_gen: number;
  is_ready: boolean;
  phase: Phase;
  deletion_requested_at?: string | null;
  labels?: Record<string, string>;
  created_at: string;
  updated_at: string;
}

// WorkInfo is the live work_queue claim for a resource — who is
// reconciling it and for how long. Present on the detail fetch only while
// a task is queued/in-flight; absent once the resource is settled. Lets a
// long-running reconcile show more than a flat "Reconciling".
export interface WorkInfo {
  task_type: string;
  broker_id?: string;   // CLAIMING process / lease holder (the broker in a fanned-out deploy)
  worker_id?: string;   // the WORKER that actually runs the stage → "running on <worker>"
  claimed: boolean;     // false = queued but not yet picked up
  attempts: number;     // >1 = retried (failing+requeued), not just slow once
  claimed_at: string;
  elapsed: string;      // human, server-rendered, e.g. "1h47m"
  heartbeat_at: string;
  heartbeat_age: string; // fresh = alive; stale = stuck/about to be reaped
  generation: number;
}

// ProviderConfigRef is the resource→config link the detail panel renders:
// a resource's attached CUSTOM provider config (name is the global handle,
// kind == the resource's kind). Null when the resource uses its kind default.
export interface ProviderConfigRef {
  name: string;
  kind: Kind;
}

// FullResource carries spec + status in addition to the info header.
// Returned only by GET /api/resources/{kind}/{name} (detail panel) and
// embedded in subgraph nodes (one extra column: min_depth).
export interface FullResource extends ResourceInfo {
  // uid is the internal uuid, an OPAQUE incarnation handle — it may be
  // DISPLAYED (e.g. a "UID" detail field) but is NEVER used for addressing,
  // routing, or as a React navigation key. Address by (kind, name) instead.
  uid: string;
  // root_kind/root_name identify the resource at the top of this resource's
  // ownership chain (absent on a root itself).
  root_kind?: string;
  root_name?: string;
  spec: unknown;
  status?: unknown | null;
  provider_config?: ProviderConfigRef | null;
  work?: WorkInfo | null;
  // manifest_drift is true when this resource is pinned to an older kind
  // manifest than the kind's current applied version — the kind's CRD was
  // re-applied after this resource last scheduled. It re-pins on its next
  // reconcile. Detail-read only (absent on list items).
  manifest_drift: boolean;
  // Failure detail (detail-read only). failure_terminal: the current-generation
  // failure won't auto-retry — either the provider declared it terminal or a
  // persistently-transient failure was escalated at the cap; a Failed row with
  // this set is DEAD-LETTERED (recover by editing the spec or forcing a reconcile).
  // failure_attempts: durable count of consecutive transient reconcile failures for
  // this generation, climbing toward max_transient_attempts (the kind's cap; absent
  // when the kind has no config or the cap is 0 = unbounded). Rendered as
  // "attempt N of CAP" and a dead-letter badge on the detail panel.
  failure_terminal?: boolean;
  failure_attempts?: number;
  max_transient_attempts?: number;
}

// ResourceManifest is the self-describing apply/download document:
// identity (kind + name), labels, and the per-kind spec bundled into
// one object. It's the exact shape POST /api/resources accepts and the
// shape GET /api/v1/raw/resources/{kind}/{name}/manifest returns, so a
// downloaded manifest re-applies verbatim. Mirrors model.ResourceManifest in Go.
export interface ResourceManifest {
  kind: Kind;
  // kind_version is the web-API version this apply targets (v1, v2, …). REQUIRED and
  // explicit (>= 1): there is no implicit v1 default — the API rejects a missing/0
  // kind_version (422). On an existing resource, a kind_version DIFFERENT from its
  // pinned one is a breaking FLIP (spec rewritten to the new kind_version's shape, in
  // place). A download emits the resource's current kind_version so a re-apply keeps it.
  kind_version: number;
  name: string;
  labels?: Record<string, string>;
  spec: unknown;
  // provider_config_ref attaches a CUSTOM provider config by name: its
  // document overrides the kind's default per field, cloned into the work
  // queue at schedule (like spec) so edits apply on the next reconcile.
  // Omit to use only the kind default; an empty string clears an existing
  // attachment. The manifest download emits it ONLY when one is attached,
  // so a download→reapply preserves the attachment without polluting the
  // common no-config manifest. Mirrors model.ResourceManifest's apply body.
  provider_config_ref?: string;
}

// Resource is what list/page/subgraph callers see embedded in their
// responses — the slim shape. Detail handlers use FullResource.
export type Resource = ResourceListItem;

// SpecRevision is one entry in a ROOT's spec history (bodies elided — fetch
// a body via /api/v1/raw/resources/{kind}/{name}/spec/{generation}). A revision is
// identified by its authored generation. source labels the writer
// (apply/rollback); is_current flags the checked-out revision.
export interface SpecRevision {
  generation: number;
  source: string;
  size_bytes: number;
  is_current: boolean;
  created_at: string;
}

// PHASE_BG: tailwind background colors for each phase. Orphaned = purple,
// distinct from Deleting's slate — conveys "left the composition, waiting".
export const PHASE_BG: Record<Phase, string> = {
  Ready: 'bg-emerald-500',
  Reconciling: 'bg-amber-500',
  Degraded: 'bg-orange-500',
  Failed: 'bg-rose-500',
  Deleting: 'bg-slate-400',
  Orphaned: 'bg-purple-500',
  Quarantined: 'bg-yellow-600',
}

// PHASE_FILL: hex colors (for inline SVG/style) keyed by phase.
export const PHASE_FILL: Record<Phase, string> = {
  Ready: '#10b981',
  Reconciling: '#f59e0b',
  Degraded: '#f97316',
  Failed: '#f43f5e',
  Deleting: '#94a3b8',
  Orphaned: '#a855f7',
  Quarantined: '#ca8a04',
}

// PHASE_LABELS: human-readable labels for each phase.
export const PHASE_LABELS: Record<Phase, string> = {
  Ready: 'Ready',
  Reconciling: 'Reconciling',
  Degraded: 'Degraded',
  Failed: 'Failed',
  Deleting: 'Deleting',
  Orphaned: 'Orphaned',
  Quarantined: 'Quarantined',
}

// ReadinessKindCount mirrors the server's readinessKindCount: per-kind
// breakdown of the six phase buckets. The UI summary card renders one
// stacked bar per kind from this.
export interface ReadinessKindCount {
  kind: Kind;
  ready: number;
  reconciling: number;
  degraded: number;
  failed: number;
  deleting: number;
  orphaned: number;
  quarantined: number;
}

// ReadinessTotals: "all kinds together" rollup used as the top
// counters above the per-kind table.
export interface ReadinessTotals {
  ready: number;
  reconciling: number;
  degraded: number;
  failed: number;
  deleting: number;
  orphaned: number;
  quarantined: number;
}

// countForPhase reads the per-phase count off a ReadinessKindCount /
// ReadinessTotals by Phase value — so callers can iterate PHASE_VALUES
// without a switch.
export function countForPhase(c: ReadinessKindCount | ReadinessTotals, p: Phase): number {
  switch (p) {
    case 'Ready': return c.ready;
    case 'Reconciling': return c.reconciling;
    case 'Degraded': return c.degraded;
    case 'Failed': return c.failed;
    case 'Deleting': return c.deleting;
    case 'Orphaned': return c.orphaned;
    case 'Quarantined': return c.quarantined;
  }
}

export interface OwnerSummary {
  owner: ResourceInfo;
  totals: ReadinessTotals;
  by_kind: ReadinessKindCount[];
}

export interface ScopedSummary {
  owners: ResourceInfo[];
  totals: ReadinessTotals;
  by_kind: ReadinessKindCount[];
}

// One token-paginated page of the resources list. There is NO total: the
// list pages with Next/Prev cursors (AWS-console style), so the server never
// runs a COUNT(*) over the filtered set — exact + cheap at the 10M-row scale
// target. next_cursor is a single opaque token; '' means this is the last page.
export interface OwnerResourcesPage {
  owner: ResourceInfo;
  rows: Resource[];
  next_cursor: string;
}

export interface MultiOwnerResourcesPage {
  owners: ResourceInfo[];
  rows: Resource[];
  next_cursor: string;
}

// RootPageItem is a row on /api/resources/roots, i.e. a root
// resource plus a count of owned resources under it.
export interface RootPageItem extends ResourceInfo {
  resource_count: number;
}

export interface RootsPage {
  rows: RootPageItem[];
  total: number;
  next_cursor: string;
}

export interface SubgraphNode extends Resource {
  min_depth: number;
}

export interface SubgraphData {
  nodes: SubgraphNode[];
  edges: GraphEdge[];
}

// GraphEdge joins two subgraph nodes by their "kind/name" ref strings
// (matching SubgraphNode.ref) — from = dependency, to = dependent.
export interface GraphEdge {
  from: string;
  to: string;
}

// TopologyBucket: server returns by_readiness keyed by the `phase`
// scalar (Ready/Reconciling/Degraded/Failed/Deleting) mirroring the SQL
// GROUP BY phase.
export interface TopologyBucket {
  bucket: string;
  by_readiness: Partial<Record<Phase, number>>;
  total: number;
}

export interface TopologyResponse {
  owner: ResourceInfo;
  group_by: string[];
  // Server marshals nil slice as null when no path drill is active.
  path: string[] | null;
  next_key: string;
  buckets: TopologyBucket[];
  total: number;
  next_cursor: string;
}

export interface TopologyLeavesResponse {
  owner: ResourceInfo;
  resources: Resource[];
  next_cursor: string;
}

// ValueMapping is one composer-emitted "fill spec field X from
// upstream status field Y" entry. arch-v2 dropped the `substituted`
// bootstrap latch: the framework re-applies every mapping on every
// upstream status change, so every declared mapping is simply the wired
// data path.
export interface ValueMapping {
  dependent_field: string;
  source_field: string;
}

// UpstreamDep is one upstream resource the dependent depends on,
// joined with every declared value mapping on the (this dependent,
// that upstream) edge. upstream_ready is the upstream's is_ready rollup;
// upstream_health_ok is its Ready/health axis, so the UI can tell
// "upstream not synced yet" apart from "upstream synced but unhealthy".
export interface UpstreamDep {
  kind: Kind;
  name: string;
  upstream_ready: boolean;
  upstream_health_ok: boolean;
  upstream_updated_at?: string | null;
  value_mappings: ValueMapping[] | null;
}

export interface ResourceEvent {
  type: string;
  actor?: string;
  reason?: string;
  message?: string;
  detail: Record<string, unknown>;
  created_at: string;
}

export interface ResourceEventsPage {
  events: ResourceEvent[];
  next_cursor: string;
}

// MemberStatus is the server-computed soft-liveness verdict for a running
// cluster member, rendered VERBATIM by the cluster view — never recomputed
// client-side from last_heartbeat (same discipline as resource `phase`). The
// server flips NotReady once a heartbeat is older than ~90s; the row stays
// listed (greyed) until the GC reclaims it at a longer TTL.
export type MemberStatus = 'Ready' | 'NotReady';

// ClusterMember mirrors the Go server's clusterMemberInfo — one row of the
// cluster_members registry for the cluster view: one running instance of the
// app, identified by its role.
export interface ClusterMember {
  member_id: string; // stable process id "host-pid-uuid8"
  role: string; // "all" | "control" | "broker"
  shards: [number, number] | null; // owned [lo, hi] inclusive span, or null
  config: unknown; // small booted runtime-config blob
  in_flight: number; // live claimed-task count — a BROKER concept (0/irrelevant for control/react)
  workers?: ConnectedWorker[]; // workers connected to this broker (BROKER rows only; omitted otherwise)
  version: string;
  hostname: string;
  pid: number;
  status: MemberStatus; // server-computed; rendered verbatim
  ready: boolean; // status === 'Ready', for badge color
  age_seconds: number; // heartbeat age at fetch time
  uptime: string; // server-rendered (e.g. "1h47m")
  started_at: string; // RFC3339
  last_heartbeat: string; // RFC3339
}

// ConnectedWorker mirrors the Go server's connectedWorkerInfo — one worker
// connected to a broker (the analogue of a pod on a node). A worker can
// serve several kinds. Surfaced under a broker member's `workers`.
export interface ConnectedWorker {
  worker_id: string; // identity the broker OBSERVED: SPIFFE ID / mesh header / peer IP (never self-reported)
  id_source?: string; // "spiffe" | "mesh-header" | "peer-ip" | "" — how worker_id was derived
  id_verified?: boolean; // true only for a cryptographically-verified source (cert / mesh header)
  kinds: string[]; // resource kinds this worker can execute
  // kind_versions is PARALLEL to kinds: kinds[i] is served at kind_version kind_versions[i]
  // (v1, v2, …). Absent/short → the missing entries are kind_version 1. Render each
  // served kind as "kind/vN".
  kind_versions?: number[];
  inflight: number; // tasks it currently holds
  max_inflight: number; // its advertised concurrency ceiling
}

// Status badge colors — Ready green, NotReady grey (kept until GC removes it).
export const MEMBER_STATUS_BG: Record<MemberStatus, string> = {
  Ready: 'bg-emerald-500',
  NotReady: 'bg-slate-400',
};

// ProviderConfig mirrors the Go server's providerConfigBody — one row of the
// runtime-editable providerconfigs table (Crossplane's ProviderConfig). A
// config parameterises one consumer `kind`. is_default marks the kind's single
// live-reconfigurable default; otherwise it's a CUSTOM override a resource
// attaches via provider_config_ref. owner_* identify the composer root that
// emitted it (empty for user/API-created configs).
export interface ProviderConfig {
  name: string;
  kind: Kind;
  // kind_version is the web-API version (v1, v2, …) this config parameterises;
  // default-ness is per-(kind, kind_version), and a custom config attaches only to a
  // resource of the same (kind, kind_version). Omitted/0 = v1.
  kind_version?: number;
  is_default: boolean;
  spec: unknown;
  // data is the opaque provider BUNDLE (bytea) base64-encoded — a zip of
  // Starlark .star files etc. Present on read only when the config has a bundle;
  // omitted for the spec-only common case.
  data?: string;
  owner_kind?: string;
  owner_name?: string;
  created_at: string;
  updated_at: string;
}

// ProviderConfigsPage is one paginated page of the provider-configs list.
export interface ProviderConfigsPage {
  rows: ProviderConfig[];
  total: number;
  limit: number;
  offset: number;
}

// ListProviderConfigsQuery is the filter set for the provider-configs page.
// All optional and AND-combined: kind (exact), name (case-insensitive
// substring), is_default (tri-state — undefined = any), kind_version (exact web-API
// version; undefined/0 = any).
export interface ListProviderConfigsQuery {
  kind?: string;
  name?: string;
  is_default?: boolean;
  kind_version?: number;
  limit?: number;
  offset?: number;
}

// ProviderConfigManifest is the self-describing apply document for a provider
// config — identity (kind + name), the is_default role flag (defaults false),
// and the per-kind spec. It's the exact shape POST /api/providerconfigs accepts,
// so a config viewed in the UI re-applies verbatim. Mirrors the Go server's
// applyProviderConfigInput.Body.
export interface ProviderConfigManifest {
  kind: Kind;
  name: string;
  // kind_version is the web-API version (v1, v2, …) this config targets; the server
  // validates the spec against that (kind, kind_version)'s config schema and enforces
  // one default per (kind, kind_version). REQUIRED and explicit (>= 1): no implicit v1
  // default — the API rejects a missing/0 kind_version (422).
  kind_version: number;
  is_default?: boolean;
  spec: unknown;
  // data is the optional opaque provider BUNDLE (a zip of .star files etc.),
  // base64-encoded. The server decodes it into providerconfigs.data (max 10 MiB
  // decoded). Omit for a spec-only config.
  data?: string;
}

// ─── reactor bindings (reactor subscriptions) ───────────────────────────

// LifecycleTransition is the resource lifecycle edge a subscription fires on —
// mirrors the reactor_bindings.transition CHECK constraint.
export type LifecycleTransition = 'created' | 'synced' | 'degraded' | 'failed' | 'deleted';

// ReactorBinding is one row of the runtime-editable reactor_bindings table — a
// SUBSCRIPTION "when a <watch_kind> resource crosses <transition>, run the
// <reactor> kind's reaction". Mirrors the Go server's reactorBindingBody. It is
// the SOLE wiring surface — there is no derived / manual split; every binding is
// an explicit, editable subscription. A binding is NOT a resource: it
// parameterises the ReactorDispatcher, read fresh on every claim, so an edit
// takes effect live with no restart. The reactor's WHERE-to-deliver config comes
// from the reactor kind's DEFAULT providerconfig (pulled by the worker).
export interface ReactorBinding {
  name: string;
  watch_kind: Kind;
  // watch_kind_version optionally scopes the subscription to ONE web-API version
  // of the WATCHED kind; 0/omitted = all versions. Distinct from reactor_version
  // (which versions the reactor that RUNS).
  watch_kind_version?: number;
  transition: LifecycleTransition;
  // label_match: {} (or omitted) matches all resources of watch_kind; else the
  // resource's labels must contain these (JSONB @>).
  label_match?: unknown;
  // reactor: the reactor kind whose reaction runs (the broker ships STAGE_REACT
  // to a worker advertising this kind). Always distinct from watch_kind.
  reactor: string;
  // reactor_version optionally PINS which reactor kind_version this binding invokes;
  // 0/omitted = unpinned (resolve the reactor's highest published version at claim
  // time, so a reactor upgrade takes effect automatically). A value makes the
  // binding immune to a later reactor publish.
  reactor_version?: number;
  enabled: boolean;
}

// ─── kind manifests (DOT CRD) ──────────────────────────────────────────
// A ReactionDecl is one declared reaction: (trigger, emits) is the DATA that
// used to be a typed provider stage. The core dispatches off these masks. A
// `reactor` reaction carries no transition/kind/label — those live on a binding.
export interface ReactionDecl {
  name: string;
  trigger: 'specChange' | 'childrenSettled' | 'deleteRequested' | 'operation' | 'reactor' | 'resync';
  emits: Array<'children' | 'edges' | 'configs' | 'status' | 'conditions' | 'finalizer' | 'operationOutput' | 'sideEffect'>;
  verb?: string;
  finalizer?: string;
}

// KindManifest is a kind's full DOT definition: JSON Schemas + declared
// reactions + finalizer. Applied by an operator (PUT /kinds/{kind}/manifest);
// the reaction engine + ManifestCache read it.
export interface KindManifest {
  kind: Kind;
  description?: string;
  spec_schema?: unknown;
  status_schema?: unknown;
  config_schema?: unknown;
  reactions: ReactionDecl[];
  finalizer_name?: string;
}

// ─── kinds & per-kind operational config ───────────────────────────────

// KindOperational is a kind's runtime-editable OPERATIONAL config (the
// kind_config row): the global concurrency cap, the per-task deadline, the
// drift-resync policy, the orphan-grace window, and the transient-failure
// dead-letter cap. max_inflight 0 = uncapped; task_deadline_seconds 0 = no
// deadline; resync_interval_seconds 0 = no drift resync; orphan_grace_seconds 0 = a
// composer-dropped child of this kind is pruned immediately (no grace);
// max_transient_attempts 0 = unbounded (transient reconcile failures retry
// forever), >0 = dead-letter (mark terminal) after that many consecutive transient
// failures so a persistently-broken handler stops retrying. Distinct from a kind's
// config DOCUMENT (provider configs).
export interface KindOperational {
  max_inflight: number;
  task_deadline_seconds: number;
  resync_interval_seconds: number;
  resync_recomposes: boolean;
  orphan_grace_seconds: number;
  max_transient_attempts: number;
  // retired SUNSETS this web-API version: new creates/flips onto it are frozen
  // (422) while existing resources keep reconciling so they can drain/migrate.
  // Runtime-editable via applyKindConfig.
  retired?: boolean;
}

// KindSummary is a declared kind's static shape flags + its operational config
// (absent when the kind has no kind_config row). Backs the Kinds list.
// is_reactor marks a REACTOR kind — a side-effect sink bound to other kinds'
// transitions, not a creatable resource: listed read-only with no operational
// config and no Edit.
export interface KindSummary {
  kind: string;
  description?: string;
  // kind_versions are the declared web-API versions (v1, v2, …), ascending; always
  // at least [1]. A kind with >1 kind_version serves each concurrently — a resource /
  // provider config pins one, and the schema refs are per-kind_version. (Matches the
  // API's `kind_versions` JSON key.)
  kind_versions?: number[];
  has_spec: boolean;
  has_status: boolean;
  has_config: boolean;
  is_reactor?: boolean;
  operational?: KindOperational;
}

// KindSchema is one kind's full detail: the summary + its shapes and the live
// default config document (providerconfigs is_default). Backs the Kinds
// detail/expand view.
//
// Each shape comes as both the raw JSON Schema (*_schema, for programmatic
// clients) and its /docs component name (*_schema_ref) — the UI links to the
// concrete shape at /docs#/schemas/{ref} rather than rendering JSON as text.
//
// The schema read is per-kind_version (GET /kinds/{kind}/schema?kind_version=N): the *_schema_ref
// are the refs for the requested kind_version — v1 unsuffixed ("VpcSpec"), v2+ suffixed
// ("VpcSpecV2") — so a caller re-fetches with a kind_version to point the /docs links at
// that version's shape. `kind_versions` (inherited) lists the versions available.
export interface KindSchema extends KindSummary {
  spec_schema?: unknown;
  status_schema?: unknown;
  config_schema?: unknown;
  spec_schema_ref?: string;
  status_schema_ref?: string;
  config_schema_ref?: string;
  default_config?: unknown;
}

// Versioned API base. Every request goes through /api/v1; a future breaking
// change ships a /api/v2 surface and this bumps (or the client negotiates via
// GET /api/v1/version → api_versions). The raw-bytes download URLs built
// directly in components (manifest/status/spec) must use this same prefix.
const BASE = '/api/v1';

async function json<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(BASE + path, init);
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
  return res.json();
}

// resourcePath builds the K8s-identity path (relative to BASE) for a single
// resource, addressed by its public (kind, name). Every per-resource endpoint
// hangs its suffix (/children, /graph, /events, /reconcile, …) off this. Use
// it with json() (which prepends BASE) or prefix BASE for a raw fetch(). The
// internal uuid is NEVER used for addressing.
function resourcePath(kind: string, name: string): string {
  return `/resources/${encodeURIComponent(kind)}/${encodeURIComponent(name)}`;
}

// ─── query-string builders ─────────────────────────────────────────────

export interface ListResourcesPageQuery {
  kinds?: Kind[];
  // kv is the (kind, version) PAIR filter — each entry "kind/N" (e.g. "vpc/2"),
  // sent as a repeatable ?kv=. Each is an exact (kind, kind_version) match; the
  // server AND-matches the set, so vpc/v1 + account/v2 filters to both. This is
  // the Resources-page kind/version chips. Omitted = no pair filter.
  kv?: string[];
  // kind_version filters to a single web-API version (v1, v2, …). Only sent /
  // meaningful when a kind filter is also present (the server prunes by kind
  // first). Omitted = any version. DEPRECATED — prefer kv pairs.
  kind_version?: number;
  // The server filters by a SET of phases (the generated scalar): it
  // returns rows whose phase is ANY of these. The UI sends every selected
  // phase chip as a repeated ?phase= param and the server unions them, so
  // the page (and X-Total) is the exact union — paginated server-side, not
  // narrowed client-side over a sliver of the first page. Empty/omitted =
  // all phases.
  phases?: Phase[];
  name?: string;
  labels?: Record<string, string>;
  limit?: number;
  cursor?: string;
}

function appendListParams(params: URLSearchParams, q: ListResourcesPageQuery): void {
  if (q.kinds) for (const k of q.kinds) params.append('kind', k);
  if (q.kv) for (const pair of q.kv) params.append('kv', pair);
  if (q.kind_version && q.kind_version > 0) params.set('kind_version', String(q.kind_version));
  if (q.phases) for (const p of q.phases) params.append('phase', p);
  // Trim at send time: the input keeps internal spaces in state, but a
  // leading/trailing-padded query would defeat the server's ILIKE.
  if (q.name?.trim()) params.set('name', q.name.trim());
  if (q.labels) {
    for (const [k, v] of Object.entries(q.labels)) {
      params.append('label', `${k}:${v}`);
    }
  }
  params.set('limit', String(q.limit ?? 100));
  if (q.cursor) params.set('cursor', q.cursor);
}

async function fetchOwnerResourcesPage(ownerKind: string, ownerName: string, q: ListResourcesPageQuery): Promise<OwnerResourcesPage> {
  const params = new URLSearchParams();
  appendListParams(params, q);
  const res = await fetch(`${BASE}${resourcePath(ownerKind, ownerName)}/children?${params.toString()}`);
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
  const body = (await res.json()) as { owner: ResourceInfo; resources: Resource[] | null };
  return {
    owner: body.owner,
    rows: body.resources ?? [],
    next_cursor: res.headers.get('X-Next-Cursor') ?? '',
  };
}

// ownerRefs are "kind/name" strings appended as repeated ?owner= params so the
// server unions the scope across N owners (0 = the whole cluster).
async function fetchMultiOwnerResourcesPage(ownerRefs: string[], q: ListResourcesPageQuery): Promise<MultiOwnerResourcesPage> {
  const params = new URLSearchParams();
  for (const ref of ownerRefs) params.append('owner', ref);
  appendListParams(params, q);
  const res = await fetch(`${BASE}/resources?${params.toString()}`);
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
  const body = (await res.json()) as { owners: ResourceInfo[] | null; resources: Resource[] | null };
  return {
    owners: body.owners ?? [],
    rows: body.resources ?? [],
    next_cursor: res.headers.get('X-Next-Cursor') ?? '',
  };
}

export interface ListRootsPageQuery {
  kinds?: Kind[];
  // kind_version filters roots to a single web-API version; meaningful only with
  // a kind filter (server prunes by kind first). Omitted = any version.
  kind_version?: number;
  name?: string;
  created_after?: string;
  created_before?: string;
  updated_after?: string;
  updated_before?: string;
  gen_min?: number;
  gen_max?: number;
  count_min?: number;
  count_max?: number;
  phase?: Phase;
  limit?: number;
  cursor?: string;
}

async function fetchRootsPage(q: ListRootsPageQuery): Promise<RootsPage> {
  const params = new URLSearchParams();
  if (q.kinds) for (const k of q.kinds) params.append('kind', k);
  if (q.kind_version && q.kind_version > 0) params.set('kind_version', String(q.kind_version));
  if (q.name?.trim()) params.set('name', q.name.trim());
  if (q.created_after) params.set('created_after', q.created_after);
  if (q.created_before) params.set('created_before', q.created_before);
  if (q.updated_after) params.set('updated_after', q.updated_after);
  if (q.updated_before) params.set('updated_before', q.updated_before);
  if (q.gen_min) params.set('gen_min', String(q.gen_min));
  if (q.gen_max) params.set('gen_max', String(q.gen_max));
  if (q.count_min) params.set('count_min', String(q.count_min));
  if (q.count_max) params.set('count_max', String(q.count_max));
  if (q.phase) params.set('phase', q.phase);
  params.set('limit', String(q.limit ?? 50));
  if (q.cursor) params.set('cursor', q.cursor);
  const res = await fetch(`${BASE}/resources/roots?${params.toString()}`);
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
  const body = (await res.json()) as { roots: RootPageItem[] | null };
  return {
    rows: body.roots ?? [],
    total: parseInt(res.headers.get('X-Total') ?? '0', 10),
    next_cursor: res.headers.get('X-Next-Cursor') ?? '',
  };
}

// ─── api object ────────────────────────────────────────────────────────

export const api = {
  // The running-fleet registry for the cluster view: every member (a running
  // app instance, control + worker) that wrote a heartbeat, with its role,
  // owned shards, booted config, live in-flight count, and server-computed
  // Ready/NotReady status.
  listClusterMembers: async (): Promise<ClusterMember[]> => {
    const body = await json<{ members: ClusterMember[] | null }>('/cluster-members');
    return body.members ?? [];
  },
  getResourceFull: (kind: string, name: string) =>
    json<FullResource>(`/resources/${encodeURIComponent(kind)}/${encodeURIComponent(name)}`),
  // applyManifest is the single create-or-update ("Apply"). The body is
  // a ResourceManifest ({kind, name, labels, spec}) — kind/name/labels
  // travel with the spec, so a downloaded manifest re-applies as-is. The
  // root is keyed by (kind, name): a new pair creates, an existing one
  // overwrites its spec + labels. Generation bumps only on a real spec
  // change. The upsert always sets labels, so the manifest must carry the
  // FULL intended label set — an absent/empty labels object clears them.
  // Returns the resource plus the kubectl-style outcome from the
  // X-Apply-Result header: "created", "configured", or "unchanged".
  applyManifest: async (
    manifest: ResourceManifest,
  ): Promise<{ resource: FullResource; result: ApplyResult }> => {
    const res = await fetch(`${BASE}/resources`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(manifest),
    });
    if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
    const result = (res.headers.get('X-Apply-Result') as ApplyResult) || 'configured';
    return { resource: (await res.json()) as FullResource, result };
  },
  reconcileResource: (kind: string, name: string) =>
    json<{ status: string }>(`${resourcePath(kind, name)}/reconcile`, { method: 'POST' }),

  // Quarantine sets a failed/stuck resource aside (frozen from all schedulers,
  // excluded from its root's rollup, NOT deleted). Unquarantine clears it and
  // re-arms the resource.
  quarantineResource: (kind: string, name: string) =>
    json<{ quarantined: boolean }>(`${resourcePath(kind, name)}/quarantine`, { method: 'POST' }),
  unquarantineResource: (kind: string, name: string) =>
    json<{ quarantined: boolean }>(`${resourcePath(kind, name)}/unquarantine`, { method: 'POST' }),

  // Spec revision history for a ROOT (newest-first, bodies elided).
  // Children keep only their live body, so this is empty/single for them.
  listSpecHistory: async (kind: string, name: string, limit = 50): Promise<SpecRevision[]> => {
    const body = await json<{ revisions: SpecRevision[] | null }>(
      `${resourcePath(kind, name)}/spec-history?limit=${limit}`,
    );
    return body.revisions ?? [];
  },
  // Check out a historic revision (by authored generation) as the live spec.
  // Returns the resource's generation after checkout. Re-runs the pipeline.
  rollbackResource: (kind: string, name: string, generation: number) =>
    json<{ generation: number }>(`${resourcePath(kind, name)}/rollback`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ generation }),
    }),

  // Upstream dependencies of a resource — drives the "blocked on"
  // panel. An upstream whose upstream_ready=false (not synced, or synced
  // but unhealthy per upstream_health_ok) is something the resource is
  // waiting on. Value mappings are always returned so the UI can render
  // the wired data path.
  listResourceDependencies: async (kind: string, name: string): Promise<UpstreamDep[]> => {
    const body = await json<{ dependencies: UpstreamDep[] | null }>(
      `${resourcePath(kind, name)}/dependencies`,
    );
    return body.dependencies ?? [];
  },

  // Children: the paginated owner-scoped collection (the former unpaginated
  // /children endpoint was folded into it — listOwnerResourcesPage hits /children).
  listOwnerResourcesPage: fetchOwnerResourcesPage,
  listMultiOwnerResourcesPage: fetchMultiOwnerResourcesPage,
  listRootsPage: fetchRootsPage,

  // Single resource: detail-panel shape (spec + status). Lists/pages
  // return ResourceListItem (slim); the detail panel re-fetches on
  // click to avoid shipping multi-MB spec/status payloads in lists.
  getResource: (kind: string, name: string) =>
    json<FullResource>(resourcePath(kind, name)),

  // Summaries
  getOwnerSummary: async (ownerKind: string, ownerName: string): Promise<OwnerSummary> => {
    const body = await json<{
      owner: ResourceInfo;
      totals: ReadinessTotals;
      by_kind: ReadinessKindCount[] | null;
    }>(`${resourcePath(ownerKind, ownerName)}/summary`);
    return {
      owner: body.owner,
      totals: body.totals,
      by_kind: body.by_kind ?? [],
    };
  },
  // ownerRefs are "kind/name" strings appended as repeated ?owner= params
  // (empty = the whole cluster).
  getScopedSummary: async (ownerRefs: string[]): Promise<ScopedSummary> => {
    const params = new URLSearchParams();
    for (const ref of ownerRefs) params.append('owner', ref);
    const qs = params.toString();
    const body = await json<{
      owners: ResourceInfo[] | null;
      totals: ReadinessTotals;
      by_kind: ReadinessKindCount[] | null;
    }>(`/resources/summary${qs ? `?${qs}` : ''}`);
    return {
      owners: body.owners ?? [],
      totals: body.totals,
      by_kind: body.by_kind ?? [],
    };
  },

  // Events (resource audit log). The single opaque cursor pages backward
  // through the append-only log; '' returns the newest page.
  listEvents: async (
    kind: string,
    name: string,
    cursor = '',
    limit = 100,
  ): Promise<ResourceEventsPage> => {
    const params = new URLSearchParams();
    if (cursor) params.set('cursor', cursor);
    params.set('limit', String(limit));
    const res = await fetch(`${BASE}${resourcePath(kind, name)}/events?${params.toString()}`);
    if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
    const body = (await res.json()) as { events: ResourceEvent[] | null };
    return {
      events: body.events ?? [],
      next_cursor: res.headers.get('X-Next-Cursor') ?? '',
    };
  },

  // Subgraph
  getSubgraph: async (
    kind: string,
    name: string,
    ancestorDepth = 3,
    descendantDepth = 1,
    limit = 200,
  ): Promise<SubgraphData> => {
    const params = new URLSearchParams();
    params.set('ancestor_depth', String(ancestorDepth));
    params.set('descendant_depth', String(descendantDepth));
    params.set('limit', String(limit));
    const body = await json<{ nodes: SubgraphNode[] | null; edges: GraphEdge[] | null }>(
      `${resourcePath(kind, name)}/subgraph?${params.toString()}`,
    );
    return {
      nodes: body.nodes ?? [],
      edges: body.edges ?? [],
    };
  },

  // Topology
  getTopology: async (
    ownerKind: string,
    ownerName: string,
    groupBy: string[],
    path: string[],
    cursor = '',
    limit = 200,
  ): Promise<TopologyResponse> => {
    const params = new URLSearchParams();
    for (const k of groupBy) params.append('group_by', k);
    for (const p of path) params.append('path', p);
    if (cursor) params.set('cursor', cursor);
    params.set('limit', String(limit));
    const res = await fetch(`${BASE}${resourcePath(ownerKind, ownerName)}/topology?${params.toString()}`);
    if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
    const body = (await res.json()) as Omit<TopologyResponse, 'total' | 'next_cursor'>;
    return {
      ...body,
      buckets: body.buckets ?? [],
      total: parseInt(res.headers.get('X-Total') ?? '0', 10),
      next_cursor: res.headers.get('X-Next-Cursor') ?? '',
    };
  },
  getTopologyLeaves: async (
    ownerKind: string,
    ownerName: string,
    path: string[],
    cursor = '',
    limit = 200,
  ): Promise<TopologyLeavesResponse> => {
    const params = new URLSearchParams();
    for (const p of path) params.append('path', p);
    if (cursor) params.set('cursor', cursor);
    params.set('limit', String(limit));
    const res = await fetch(`${BASE}${resourcePath(ownerKind, ownerName)}/topology/leaves?${params.toString()}`);
    if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
    const body = (await res.json()) as { owner: ResourceInfo; resources: Resource[] | null };
    return {
      owner: body.owner,
      resources: body.resources ?? [],
      next_cursor: res.headers.get('X-Next-Cursor') ?? '',
    };
  },
  getTopologyKeys: (ownerKind: string, ownerName: string, limit = 100) =>
    json<{ keys: string[] | null }>(`${resourcePath(ownerKind, ownerName)}/topology/keys?limit=${limit}`).then(
      (b) => ({ keys: b.keys ?? [] }),
    ),

  // ── Provider configs (the runtime-editable per-kind config store) ──────
  // PAGINATED list with optional kind / name / is_default filters. Mirrors
  // GET /api/providerconfigs; the body carries total/limit/offset for paging.
  listProviderConfigs: async (q: ListProviderConfigsQuery = {}): Promise<ProviderConfigsPage> => {
    const params = new URLSearchParams();
    if (q.kind) params.set('kind', q.kind);
    if (q.name?.trim()) params.set('name', q.name.trim());
    if (q.is_default !== undefined) params.set('default', String(q.is_default));
    if (q.kind_version) params.set('kind_version', String(q.kind_version));
    params.set('limit', String(q.limit ?? 100));
    params.set('offset', String(q.offset ?? 0));
    const body = await json<{
      provider_configs: ProviderConfig[] | null;
      total: number;
      limit: number;
      offset: number;
    }>(`/providerconfigs?${params.toString()}`);
    return {
      rows: body.provider_configs ?? [],
      total: body.total ?? 0,
      limit: body.limit ?? (q.limit ?? 100),
      offset: body.offset ?? (q.offset ?? 0),
    };
  },
  // One config by name (the detail fetch the resource panel link lands on).
  getProviderConfig: (name: string) =>
    json<ProviderConfig>(`/providerconfigs/${encodeURIComponent(name)}`),
  // applyProviderConfig is the create-or-update ("Apply") for a provider config.
  // The body is a ProviderConfigManifest ({kind, name, kind_version, is_default, spec}) —
  // the same shape the detail view renders, so a config re-applies as-is. Upsert
  // is keyed by name: a new name creates, an existing one overwrites
  // kind/kind_version/role/spec. kind_version is REQUIRED and explicit (>= 1) — the
  // server rejects a missing/0 kind_version (422), no implicit v1 default; is_default
  // omitted ⇒ false (a CUSTOM override). The server validates the spec against that
  // (kind, kind_version)'s config schema and enforces one default per (kind,
  // kind_version) (409 on a second default). Returns the stored config plus the
  // kubectl-style outcome from X-Apply-Result ("created" or "configured").
  applyProviderConfig: async (
    manifest: ProviderConfigManifest,
  ): Promise<{ config: ProviderConfig; result: ApplyResult }> => {
    const res = await fetch(`${BASE}/providerconfigs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        kind: manifest.kind,
        name: manifest.name,
        // kind_version: forward the manifest's explicit version verbatim (REQUIRED,
        // >= 1). No ?? 0 fallback — a missing version must fail loudly at the server
        // (422), never silently become 0/v1.
        kind_version: manifest.kind_version,
        is_default: manifest.is_default ?? false,
        spec: manifest.spec,
        // data: carry the optional provider bundle through so a re-apply of a
        // bundle-backed config doesn't silently drop the bundle.
        ...(manifest.data !== undefined ? { data: manifest.data } : {}),
      }),
    });
    if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
    const result = (res.headers.get('X-Apply-Result') as ApplyResult) || 'configured';
    return { config: (await res.json()) as ProviderConfig, result };
  },
  // Delete (soft-delete) a resource by its (kind, name). K8s-style: the server
  // stamps deletion_requested_at + a finalizer and queues a `delete` teardown
  // task, so the resource enters phase=Deleting and its row is removed only once
  // the finalizer is stripped (a finalizer-less kind is hard-deleted immediately).
  // An in-flight reconcile is NOT cancelled — it lands harmlessly. 404
  // (deleted=false path → thrown) when no such resource existed.
  deleteResource: (kind: string, name: string): Promise<{ deleted: boolean }> =>
    json<{ deleted: boolean }>(resourcePath(kind, name), {
      method: 'DELETE',
    }),

  // Delete a config by name. The server's ON DELETE SET NULL on
  // resources.provider_config_id demotes any consumers back to the kind
  // default; 404 (deleted=false path → thrown) when no such config existed.
  deleteProviderConfig: (name: string): Promise<{ deleted: boolean }> =>
    json<{ deleted: boolean }>(`/providerconfigs/${encodeURIComponent(name)}`, {
      method: 'DELETE',
    }),

  // ── Reactor bindings (reactor subscriptions) ───────────────────────────
  // The runtime-editable "what reacts to what" subscription table. Bindings are
  // read fresh by the dispatcher on every claim, so create/edit/delete is live.
  listReactorBindings: async (): Promise<ReactorBinding[]> => {
    const body = await json<{ bindings: ReactorBinding[] | null }>('/reactor-bindings');
    return body.bindings ?? [];
  },
  // Create-or-update a subscription (upsert by name). The server validates
  // watch_kind is known, reactor is a registered reactor kind distinct from
  // watch_kind, and transition is one of the lifecycle edges. Returns the stored
  // binding.
  applyReactorBinding: (b: ReactorBinding): Promise<ReactorBinding> =>
    json<ReactorBinding>('/reactor-bindings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(b),
    }),
  deleteReactorBinding: (name: string): Promise<{ deleted: boolean }> =>
    json<{ deleted: boolean }>(`/reactor-bindings/${encodeURIComponent(name)}`, {
      method: 'DELETE',
    }),

  // ── Kinds & per-kind operational config ────────────────────────────────
  // Every DECLARED kind (registered or pending) with its shape flags + cap/resync
  // in ONE request — backs the Kinds page table.
  listKindSchemas: async (): Promise<KindSummary[]> => {
    const body = await json<{ kinds: KindSummary[] | null }>('/kinds');
    return body.kinds ?? [];
  },
  // One kind's full detail: schemas + operational config + live default config doc.
  // kind_version selects which web-API version's schema refs to return (v1 = unsuffixed
  // refs); omitted or ≤1 hits the default (v1). The ?kind_version= is appended only for
  // kind_version>1 so the common single-version read stays on the unsuffixed URL.
  getKindSchema: (kind: string, kind_version?: number) =>
    json<KindSchema>(
      `/kinds/${encodeURIComponent(kind)}/schema${kind_version && kind_version > 1 ? `?kind_version=${kind_version}` : ''}`,
    ),
  // Set a (kind, kind_version)'s operational config (cap + resync + retired). The
  // cap change is LIVE — the next work claim reads kind_config directly; resync is
  // picked up on the control plane's next refresh. All fields are effective values
  // (0/false = off), so send the current value to preserve it (notably `retired`).
  // kind_version selects which version's config to edit. It is REQUIRED and explicit
  // (>= 1): the server rejects a missing/0 kind_version (422) — there is no implicit
  // v1 default — so ?kind_version= is ALWAYS appended.
  applyKindConfig: (
    kind: string,
    cfg: KindOperational,
    kind_version: number,
  ): Promise<KindOperational> =>
    json<{ kind: string } & KindOperational>(
      `/kinds/${encodeURIComponent(kind)}/versions/${kind_version}/config`,
      {
        method: 'PUT', // idempotent full-replace of the (kind, version) config row
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(cfg),
      },
    ),

  // ── Kind manifests (DOT CRD: schemas + reactions + policy) ─────────────────
  // The Terraform-style "define what this kind IS" surface. The reaction engine
  // dispatches off the declared reactions; a DB trigger derives kind_config. A
  // reactor kind's CRD only REGISTERS its `reactor` reaction — the "when X
  // transitions, run this reactor" wiring is a separate, editable reactor_bindings
  // subscription (see the Reactor bindings page / applyReactorBinding).
  listKindManifests: async (): Promise<KindManifest[]> => {
    const body = await json<{ manifests: KindManifest[] | null }>('/kinds/manifests');
    return body.manifests ?? [];
  },
  // kind_version is REQUIRED and explicit (>= 1): the GET manifest endpoint no
  // longer defaults to v1, so name the version whose manifest to fetch.
  getKindManifest: (kind: string, kind_version: number): Promise<KindManifest> =>
    json<KindManifest>(
      `/kinds/${encodeURIComponent(kind)}/versions/${kind_version}/manifest`,
    ),
  // Apply (upsert) a kind manifest. An ADDITIVE (backward-compatible) schema
  // change to an existing (kind, kind_version) is auto-allowed; a BREAKING change
  // is rejected 409 ALWAYS — there is no in-place override, the operator must
  // publish it as a NEW kind_version so live resources keep validating against the
  // schema they were applied under. Returns the stored manifest + its content-hash
  // version.
  putKindManifest: (
    kind: string,
    m: KindManifest,
  ): Promise<KindManifest & { manifest_version: number }> =>
    json<KindManifest & { manifest_version: number }>(`/kinds/${encodeURIComponent(kind)}/manifest`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(m),
    }),
  // Delete one (kind, kind_version) CRD. Refused 409 if a resource is pinned to
  // that version or a reactor binding pins it exactly (an unpinned binding does
  // NOT block); the version's provider configs are cascade-deleted. 404 if the
  // version was already gone. kind_version is REQUIRED (>= 1).
  deleteKindManifest: (
    kind: string,
    kind_version: number,
  ): Promise<{ kind: string; kind_version: number; deleted_configs: number }> =>
    json<{ kind: string; kind_version: number; deleted_configs: number }>(
      `/kinds/${encodeURIComponent(kind)}/versions/${kind_version}/manifest`,
      { method: 'DELETE' },
    ),
};
