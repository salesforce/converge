// Example: an "account" worker in TypeScript. It implements Provider
// (kind/work/onConfig/ready) and calls serve() — no proto, no Connect, no loop.
//
// Run: BROKER_ADDR=http://localhost:9090 npx tsx example/account-worker.ts
import { serve, terminal, type Outcome, type Provider, type ReactionRequest } from "../src/index.js";

const enc = new TextEncoder();
const dec = new TextDecoder();

class AccountProvider implements Provider {
  kind() {
    return { kind: "account", version: 1 };
  }

  async work(req: ReactionRequest): Promise<Outcome> {
    // Parse the opaque spec, derive a stub account id, write it to status — the same shape
    // as the Go account.reconcile.
    let spec: { team_name?: string };
    try {
      spec = JSON.parse(dec.decode(req.resource.spec) || "{}");
    } catch (e) {
      throw terminal(`decode account spec: ${e}`); // non-retryable
    }
    const status = { account_id: `acc-stub-${spec.team_name ?? ""}` };
    return { status: enc.encode(JSON.stringify(status)) };
  }

  onConfig(): void {
    // A pure leaf — no default providerconfig to react to.
  }

  ready(): boolean {
    return true; // no downstream to dial
  }
}

// The caller owns the signal + exit code. serve reads BROKER_ADDR / TLS_* / WORKER_* from
// the environment itself (set WORKER_ID / WORKER_MAX_PARALLEL / HEALTH_ADDR / etc. there),
// then dials, serves, reconnects, and drains until the signal aborts.
const controller = new AbortController();
process.on("SIGINT", () => controller.abort());
process.on("SIGTERM", () => controller.abort());

await serve([new AccountProvider()], controller.signal);
