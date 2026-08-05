// The worker SDK's ergonomic types — the shapes a provider author reads and returns,
// wrapping the generated proto (gen/…/worker_pb). The broker speaks proto; convert.ts
// translates proto ⇄ these types at the SDK edge. Kept name-for-name aligned with
// sdk-go/converge/types.go (see docs/spec/sdk-spec.md).

// ─────────────────────────────────────────────────────────────────────────
// Kind identity.
// ─────────────────────────────────────────────────────────────────────────

// KindVersion is a (kind, web-API version) pair — the addressable unit of everything
// versioned. version is EXPLICIT (>= 1); there is no implicit v1.
export interface KindVersion {
  kind: string;
  version: number;
}

// ResourceRef identifies a sibling resource by (kind, name) — the natural key under a
// single owner. Used in DepEdge and ChildSpec.
export interface ResourceRef {
  kind: string;
  name: string;
}

// ─────────────────────────────────────────────────────────────────────────
// Trigger / Transition — the two author-facing lifecycle enums a reaction reads off its
// ReactionRequest.
// ─────────────────────────────────────────────────────────────────────────

// Trigger is why a reaction fired — a string-union mirroring the proto Stage/Transition intent.
export type Trigger =
  | "specChange"
  | "childrenSettled"
  | "deleteRequested"
  | "operation"
  | "reactor"
  | "resync";

// Transition is the resource lifecycle edge a reactor delivery fired on (delivered to a
// reactor handler as data on ReactionRequest.transition).
export type Transition = "created" | "synced" | "degraded" | "failed" | "deleted";

// ─────────────────────────────────────────────────────────────────────────
// Conditions — K8s/Crossplane multi-axis status.
// ─────────────────────────────────────────────────────────────────────────

// Condition is one health/status axis a reaction reports. Only type "Ready" feeds the
// health_ok scalar that gates is_ready; a reaction may emit custom axes too.
export interface Condition {
  type: string; // "Ready" | "Synced" | custom
  status: "True" | "False" | "Unknown";
  reason?: string;
  message?: string;
}

// ─────────────────────────────────────────────────────────────────────────
// Resource / Operation — the provider-side view of a resources / resource_operations row.
// ─────────────────────────────────────────────────────────────────────────

// Resource is what a reaction handler receives — the SDK's ergonomic view of the proto
// Resource. Opaque JSON fields (spec/status) are raw bytes the handler parses. State is
// observed via isReady (synced AND healthy AND not deleting).
export interface Resource {
  id: Uint8Array;
  kind: string;
  name: string;
  spec: Uint8Array; // opaque JSON
  status: Uint8Array; // opaque JSON
  generation: bigint;
  isReady: boolean;
  labels: Record<string, string>;
}

// Operation is the provider-side view of a resource_operations row, passed to an operate
// reaction. resourceId identifies what the verb runs against.
export interface Operation {
  resourceId: Uint8Array;
  verb: string;
  input: Uint8Array; // opaque JSON
  attempts: number;
  requestedBy: string;
}

// ─────────────────────────────────────────────────────────────────────────
// ChildSpec / DepEdge / ValueFlow / ProviderConfigSpec — what a composer emits.
// ─────────────────────────────────────────────────────────────────────────

// ChildSpec describes one child a composer wants to exist under a parent. spec is the
// opaque JSON the core stores at the DB boundary. kindVersion is REQUIRED and explicit
// (>= 1) — a composer emits each child at the version it wants.
export interface ChildSpec {
  kind: string;
  kindVersion: number;
  name: string;
  spec: Uint8Array; // opaque JSON
  labels: Record<string, string>;
}

// ProviderConfigSpec is one provider config a composer emits alongside its children.
// kindVersion is the web-API version of the consumer kind (REQUIRED, >= 1).
export interface ProviderConfigSpec {
  name: string;
  kind: string;
  kindVersion: number;
  isDefault: boolean;
  spec: Uint8Array; // opaque JSON
}

// ValueFlow declares one field-level flow on a DepEdge: the dependent's
// spec[dependentField] is filled from the upstream's status[sourceField] (both RFC 6901
// JSON pointers) when the upstream's status becomes available.
export interface ValueFlow {
  dependentField: string;
  sourceField: string;
}

// DepEdge: dependent (from) depends on dependency (to), optionally carrying value flows
// that fill the dependent's spec from the upstream's status when the upstream is ready.
export interface DepEdge {
  from: ResourceRef;
  to: ResourceRef;
  values: ValueFlow[];
}

// ─────────────────────────────────────────────────────────────────────────
// ProviderConfig — the kind DEFAULT delivered to onConfig.
// ─────────────────────────────────────────────────────────────────────────

// ProviderConfig is the kind's DEFAULT config delivered to onConfig at startup + on every
// edit. The per-resource override rides the task (req.env); the handler merges them via
// effectiveConfig/effectiveBundle. Either axis may be empty; both empty = the default was
// deleted.
export interface ProviderConfig {
  spec: Uint8Array; // opaque JSON config document
  data: Uint8Array; // opaque bundle bytes
}

// ─────────────────────────────────────────────────────────────────────────
// ReactionRequest / Outcome — the request/response a handler speaks.
// ─────────────────────────────────────────────────────────────────────────

// ReactionRequest carries everything a reaction may need — the SDK's ergonomic view of a
// decoded StageTask. The core fills only the fields a given trigger needs; reaction +
// trigger tell the worker which handler body to run and why.
export interface ReactionRequest {
  reaction: string; // the reaction name selected by the core
  kindVersion: number;
  trigger: Trigger;
  resource: Resource;
  status: Uint8Array; // accumulated status; empty for the first stage
  observed: Resource[]; // composer: current owned children + statuses
  descendants: Resource[]; // rollup: the whole settled subtree
  operation?: Operation; // operate: the resource_operations row
  transition?: Transition; // reactor: the lifecycle edge that fired
  dedupToken: string; // reactor: at-least-once dedup key
  generation: bigint;
  // env — the per-call environment. providerConfig/providerBundle are the per-resource
  // CUSTOM overrides (empty when the resource carries none); the kind DEFAULT reaches the
  // provider through onConfig. Resolve the EFFECTIVE config with effectiveConfig/Bundle.
  env: {
    providerConfig: Uint8Array; // per-resource CUSTOM config override (opaque)
    providerBundle: Uint8Array; // per-resource CUSTOM bundle override (opaque)
  };
}

// Outcome is what a reaction returns — the SDK's ergonomic view; convert.ts encodes it
// back into a proto StageComplete per stage. Fill only the parts the reaction produces;
// status is last-writer-wins, conditions accumulate.
export interface Outcome {
  status?: Uint8Array; // work/rollup/react status (opaque JSON)
  conditions?: Condition[];
  children?: ChildSpec[]; // composer: desired children
  edges?: DepEdge[]; // composer: dep edges (+ value flows)
  configs?: ProviderConfigSpec[]; // composer: provider configs it owns
  operationOutput?: Uint8Array; // operate: verb output (opaque JSON)
  sideEffectDone?: boolean; // lifecycle: the side effect completed (informational)
}

// ─────────────────────────────────────────────────────────────────────────
// effectiveConfig / effectiveBundle — merge the kind default with a per-resource override
//.
// ─────────────────────────────────────────────────────────────────────────

const decoder = new TextDecoder();
const encoder = new TextEncoder();

// effectiveConfig computes a task's effective config document: it overlays the per-resource
// override on the kind default via a deep JSON merge (override wins per key, nested objects
// deep-merged) and returns the parsed object typed as T. defaultSpec is the provider's last
// onConfig spec; override is req.env.providerConfig. An absent/empty override yields the
// default; an empty default yields the override; a malformed override falls back to the
// default (never wipes config).
export function effectiveConfig<T = unknown>(defaultSpec: Uint8Array, override: Uint8Array): T {
  const merged = mergeConfigDoc(defaultSpec, override);
  if (merged.length === 0) {
    return {} as T;
  }
  try {
    return JSON.parse(decoder.decode(merged)) as T;
  } catch {
    return {} as T;
  }
}

// effectiveBundle resolves a task's effective opaque bundle: the per-resource override
// (req.env.providerBundle) when non-empty, else the kind default. A whole-artifact REPLACE
// (opaque bytes can't deep-merge). Returns one of the inputs verbatim; treat as read-only.
export function effectiveBundle(defaultData: Uint8Array, override: Uint8Array): Uint8Array {
  return override.length > 0 ? override : defaultData;
}

// mergeConfigDoc overlays a per-resource override on the kind default config document. The
// default is the base; the override wins per top-level key and nested objects are
// deep-merged. An empty override returns the default verbatim (the hot path); a malformed
// override or non-object either side falls back so a bad override never wipes the config.
function mergeConfigDoc(base: Uint8Array, override: Uint8Array): Uint8Array {
  if (override.length === 0) return base;
  if (base.length === 0) return override;
  let baseVal: unknown;
  let overVal: unknown;
  try {
    baseVal = JSON.parse(decoder.decode(base));
  } catch {
    return override;
  }
  try {
    overVal = JSON.parse(decoder.decode(override));
  } catch {
    return base;
  }
  if (!isObject(baseVal)) return override;
  if (!isObject(overVal)) return base;
  return encoder.encode(JSON.stringify(deepMerge(baseVal, overVal)));
}

// deepMerge recursively overlays src onto dst (mutating a copy of dst): a plain-object
// value merges key-by-key, anything else (scalar, array, null) REPLACES. Mirrors mergo's
// WithOverride semantics used by the Go SDK.
function deepMerge(dst: Record<string, unknown>, src: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = { ...dst };
  for (const [k, sv] of Object.entries(src)) {
    const dv = out[k];
    if (isObject(dv) && isObject(sv)) {
      out[k] = deepMerge(dv, sv);
    } else {
      out[k] = sv;
    }
  }
  return out;
}

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

// ─────────────────────────────────────────────────────────────────────────
// Provider / terminal — the one author contract + the non-retryable error marker.
// ─────────────────────────────────────────────────────────────────────────

// Provider is the ONE SDK contract — identical in shape to the Go converge.Provider:
// kind() / work() / onConfig() / ready(). ONE provider serves ONE (kind, version); a
// worker serving several lists one provider per pair in the slice passed to serve, so
// work/onConfig/ready take no kind/version argument (the pair is fixed by kind()). A
// provider dials its OWN downstreams on its own schedule and reports readiness through
// ready() — that IS the bring-up contract (the SDK never runs a setup step).
export interface Provider {
  kind(): KindVersion;
  work(req: ReactionRequest): Promise<Outcome>;
  onConfig(cfg: ProviderConfig): void;
  ready(): boolean;
}

// TerminalError marks a reaction failure as TERMINAL (non-retryable): the scheduler stops
// re-queuing it without a spec change. A plain thrown error is TRANSIENT (re-dispatched).
export class TerminalError extends Error {
  readonly terminal = true;
}

// terminal wraps a message (or error) as a non-retryable failure. The run loop maps it to
// StageComplete.terminal=true.
export function terminal(msg: string): TerminalError {
  return new TerminalError(msg);
}

// isTerminal reports whether err was marked terminal.
export function isTerminal(err: unknown): boolean {
  return err instanceof TerminalError;
}
