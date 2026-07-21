// @converge/worker-sdk — the TypeScript worker SDK.
//
// Two-tier public surface: the HIGH tier is serve() (env config, dials the broker itself,
// the 99% path); the LOW tier is runWorker() (a client-with-options for a caller who already
// holds a transport). Both funnel through ONE run loop. The author contract is the Provider
// interface + the value types work() speaks + the terminal() error marker + the
// effectiveConfig/effectiveBundle merge helpers. Everything else (the runner loop, transport,
// config cache, converters, backoff) is internal and not exported.
//
// The SDK wraps only the generated worker.proto stubs (gen/); see docs/spec/sdk-spec.md.

// High-level entrypoint.
export { serve } from "./serve.js";

// Low-level tier (advanced): inject your own broker client + options.
export { runWorker, type Transport, type RunOptions } from "./runworker.js";

// The author contract + the value types work() reads and returns.
export {
  type Provider,
  type ReactionRequest,
  type Outcome,
  type Resource,
  type Operation,
  type Condition,
  type ChildSpec,
  type DepEdge,
  type ValueFlow,
  type ProviderConfig,
  type ProviderConfigSpec,
  type ResourceRef,
  type KindVersion,
  type Trigger,
  type Transition,
  effectiveConfig,
  effectiveBundle,
  terminal,
  isTerminal,
  TerminalError,
} from "./types.js";
