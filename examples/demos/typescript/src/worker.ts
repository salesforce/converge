// The TypeScript demo worker: THREE providers on @converge/worker-sdk, proving the TS SDK
// does the whole engine flow a Go worker does — leaf work AND composition — over the SAME
// broker + worker.proto. A worker's language is invisible to the control plane.
//
//   tsproject/demo (composer root)
//     ├─ account/<team>   (leaf: derives a stub account id)   ×N teams
//     └─ bucket/demo      (leaf: derives a bucket name)
//          edge: bucket.account_id ← account.account_id  (value flow, filled when the
//                first account is Ready — the composer wires it, the engine flows it)
//   rollup: the root goes Ready only when every child is Ready.
//
// A provider is the whole author experience: implement Provider (kind/work/onConfig/ready)
// and hand the slice to serve(). One worker serves all three pairs — the SDK fans a claimed
// task to the provider whose kind() matches. No proto, no Connect, no loop: the SDK reads
// BROKER_ADDR / TLS_* / WORKER_* from the environment (the dev-local launcher sets them) and
// owns dial/reconnect/config/drain.
//
// Run via `just demo` in this directory (builds the SDK + this worker, brings up a cluster,
// applies the CRDs + the tsproject root). See README.md.
import {
  effectiveConfig,
  serve,
  terminal,
  type ChildSpec,
  type DepEdge,
  type Outcome,
  type Provider,
  type ProviderConfig,
  type ReactionRequest,
} from "@converge/worker-sdk";
// Types GENERATED from the kind manifests (schema-first: `npm run gen` runs
// cmd/gen-types over testfixtures/*.kind.json). The manifest's JSON Schema is the single
// source of truth; these interfaces are derived from it, so the shapes this worker reads
// and writes can never drift from what the core validates against. DO NOT hand-edit src/gen.
import type { AccountConfig, AccountSpec } from "./gen/account.js";
import type { BucketSpec, BucketStatus } from "./gen/bucket.js";
import type { TsprojectSpec } from "./gen/tsproject.js";

const enc = new TextEncoder();
const dec = new TextDecoder();

// json encodes an object as the opaque JSON bytes the core stores (spec/status are opaque
// to the engine — a provider owns their shape).
const json = (v: unknown): Uint8Array => enc.encode(JSON.stringify(v));
// parse decodes an opaque JSON field; {} for an empty field (first stage carries no status).
const parse = <T = Record<string, unknown>>(b: Uint8Array): T => JSON.parse(dec.decode(b) || "{}") as T;

// ─────────────────────────────────────────────────────────────────────────
// account — a leaf whose readiness is GATED ON ITS PROVIDERCONFIG. It can't mint an account
// id without a name pattern (its "downstream" is the config), so:
//   - onConfig stores the kind DEFAULT (the SDK primes it at startup + re-fires on edits).
//   - ready() is FALSE until a non-empty id_pattern is set → the broker advertises RS- and
//     parks this kind's work; once the config lands, ready() flips TRUE → RS+ → work drains.
//   - work() resolves the EFFECTIVE config (default ⊕ any per-resource override) and formats
//     the id from the pattern.
// This is the real bring-up contract: a provider reports readiness, the SDK polls it, and the
// broker steers work accordingly — no separate setup step.
// ─────────────────────────────────────────────────────────────────────────
class AccountProvider implements Provider {
  // The kind DEFAULT providerconfig spec, as delivered by onConfig. Empty until the first
  // (primed) delivery; empty again if the default is deleted. Guards readiness.
  private defaultConfig: Uint8Array = new Uint8Array();

  kind() {
    return { kind: "account", version: 1 };
  }

  async work(req: ReactionRequest): Promise<Outcome> {
    // Parse into Partial<AccountSpec>: the generated type describes a VALID spec, but these
    // are raw bytes off the wire — validate presence ourselves before trusting them.
    let spec: Partial<AccountSpec>;
    try {
      spec = parse<Partial<AccountSpec>>(req.resource.spec);
    } catch (e) {
      // A malformed spec won't parse on retry either — fail terminally so the scheduler
      // stops re-queuing it (the engine surfaces it as a terminal failure, not a flap).
      throw terminal(`decode account spec: ${e}`);
    }
    if (!spec.team_name) {
      throw terminal("account spec missing required field: team_name");
    }
    // Effective config = the kind default overlaid with this resource's per-resource override
    // (which rides the task in req.env). The merge is deep, override-wins; an absent override
    // yields the default. Readiness guarantees a pattern is present by the time work runs.
    const cfg = effectiveConfig<Partial<AccountConfig>>(this.defaultConfig, req.env.providerConfig);
    const pattern = cfg.id_pattern ?? "acc-{team_name}";
    const accountId = pattern.replace(/\{team_name\}/g, spec.team_name);
    // Stub "provision": derive a deterministic id from the pattern. Swap this body for a real
    // cloud call and the kind becomes a real AWS Organizations account — the engine, the
    // broker, and the wire contract are unchanged.
    return { status: json({ account_id: accountId }) };
  }

  // onConfig delivers the kind DEFAULT — at startup (the SDK primes it) and on every edit.
  // An empty spec means the default was deleted; store it either way so ready() reflects it.
  onConfig(cfg: ProviderConfig): void {
    this.defaultConfig = cfg.spec;
  }

  // ready() gates this kind's work on having a usable config. Empty config → false (RS-, work
  // parks); a config carrying a non-empty id_pattern → true (RS+, work flows).
  ready(): boolean {
    if (this.defaultConfig.length === 0) return false;
    try {
      const cfg = parse<Partial<AccountConfig>>(this.defaultConfig);
      return typeof cfg.id_pattern === "string" && cfg.id_pattern.length > 0;
    } catch {
      return false; // malformed config → not ready
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────
// bucket — a leaf that depends on an account: its account_id is FILLED BY THE ENGINE from
// the upstream account's status (the composer declares the value-flow edge). So work() just
// reads the already-flowed account_id off its own spec and derives a bucket name.
// ─────────────────────────────────────────────────────────────────────────
class BucketProvider implements Provider {
  kind() {
    return { kind: "bucket", version: 1 };
  }

  async work(req: ReactionRequest): Promise<Outcome> {
    let spec: BucketSpec;
    try {
      spec = parse<BucketSpec>(req.resource.spec);
    } catch (e) {
      throw terminal(`decode bucket spec: ${e}`);
    }
    // account_id arrives via the value flow once the upstream account is Ready. Until then
    // the engine holds this task (the edge gates it), so by the time work() runs it's set —
    // guard anyway so a misconfigured edge fails loudly instead of producing a bad name.
    if (!spec.account_id) {
      throw terminal("bucket spec missing account_id (the account→bucket value flow did not fill it)");
    }
    const status: BucketStatus = { bucket: `bkt-${spec.account_id}`, account_id: spec.account_id };
    return { status: json(status) };
  }

  onConfig(): void {
    /* pure leaf */
  }
  ready(): boolean {
    return true;
  }
}

// ─────────────────────────────────────────────────────────────────────────
// tsproject — a COMPOSER. compose() fans the spec (a team list) into account children + one
// bucket, wired by a dep edge that flows the first account's id into the bucket. rollup()
// aggregates the settled children into the root's status + Ready condition.
// ─────────────────────────────────────────────────────────────────────────
class ProjectProvider implements Provider {
  kind() {
    return { kind: "tsproject", version: 1 };
  }

  // work() dispatches by reaction name: the CRD declares `compose` (specChange) and
  // `rollup` (childrenSettled), so the SDK routes each claimed task here with req.reaction
  // set. (A leaf serves one reaction; a composer serves several, switched on req.reaction —
  // register, don't type-assert.)
  async work(req: ReactionRequest): Promise<Outcome> {
    switch (req.reaction) {
      case "compose":
        return this.compose(req);
      case "rollup":
        return this.rollup(req);
      default:
        throw terminal(`tsproject: unknown reaction ${req.reaction}`);
    }
  }

  // compose expands the project spec into the desired child set + dep edges. It composes
  // from the spec alone (req.resource); the engine diffs this against what already exists
  // and creates/updates/prunes to match.
  private compose(req: ReactionRequest): Outcome {
    // Partial<TsprojectSpec>: the generated type describes a VALID spec (teams non-empty),
    // but these are raw bytes — validate before trusting.
    let spec: Partial<TsprojectSpec>;
    try {
      spec = parse<Partial<TsprojectSpec>>(req.resource.spec);
    } catch (e) {
      // A spec that won't parse never composes without an edit → terminal (stop retrying).
      throw terminal(`CompositionFailed: decode tsproject spec: ${e}`);
    }
    const teams = spec.teams ?? [];
    if (teams.length === 0) {
      // Composes zero children → surface as a real terminal failure, not a hollow "ready".
      throw terminal("CompositionFailed: tsproject spec has no teams");
    }
    const project = spec.project ?? req.resource.name;

    const children: ChildSpec[] = [];
    const edges: DepEdge[] = [];
    const labels = (extra: Record<string, string>) => ({ composed_by: "tsproject", project, ...extra });

    // One account per team.
    for (const team of teams) {
      children.push({
        kind: "account",
        kindVersion: 1,
        name: `${project}-${team}`,
        spec: json({ team_name: team, email: `${team}@${project}.example.com` }),
        labels: labels({ team, role: "account" }),
      });
    }

    // One bucket for the project, depending on the FIRST team's account. The value flow
    // fills the bucket's spec.account_id from that account's status.account_id when the
    // account goes Ready — declarative wiring, no imperative "wait for the account" code.
    const firstAccount = `${project}-${teams[0]}`;
    const bucketName = `${project}-bucket`;
    children.push({
      kind: "bucket",
      kindVersion: 1,
      name: bucketName,
      // account_id is intentionally absent here — the engine flows it in along the edge.
      spec: json({}),
      labels: labels({ role: "bucket" }),
    });
    edges.push({
      from: { kind: "bucket", name: bucketName },
      to: { kind: "account", name: firstAccount },
      values: [{ dependentField: "/account_id", sourceField: "/account_id" }],
    });

    return {
      children,
      edges,
      status: json({ project, teams: teams.length, phase: "composed" }),
    };
  }

  // rollup fires on childrenSettled with req.descendants = the whole settled subtree. It
  // aggregates their readiness into the root's status + a Ready condition (only the "Ready"
  // condition feeds the health scalar the composite rolls up).
  private rollup(req: ReactionRequest): Outcome {
    const total = req.descendants.length;
    const ready = req.descendants.filter((d) => d.isReady).length;
    const allReady = total > 0 && ready === total;
    return {
      status: json({ children_total: total, children_ready: ready, phase: allReady ? "ready" : "reconciling" }),
      conditions: [
        {
          type: "Ready",
          status: allReady ? "True" : "False",
          reason: allReady ? "AllChildrenReady" : "ChildrenNotReady",
          message: `${ready}/${total} children Ready`,
        },
      ],
    };
  }

  onConfig(): void {
    /* the composer reads no default providerconfig */
  }
  ready(): boolean {
    return true;
  }
}

// The caller owns the signal + exit code. serve reads BROKER_ADDR / TLS_* / WORKER_* /
// HEALTH_ADDR from the environment, then dials, serves, reconnects, and drains until the
// signal aborts. One worker, three providers — the SDK routes each claimed (kind, version)
// task to the matching provider.
const controller = new AbortController();
process.on("SIGINT", () => controller.abort());
process.on("SIGTERM", () => controller.abort());

serve([new ProjectProvider(), new AccountProvider(), new BucketProvider()], controller.signal).then(
  () => process.exit(0),
  (err: unknown) => {
    console.error("worker exited:", err);
    process.exit(1);
  },
);
