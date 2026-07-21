// backoff.ts holds the SDK's reconnect/config-load retry timing. The one rule it encodes is
// the anti-thundering-herd policy: a whole worker fleet loses a broker at once, so an
// UN-jittered backoff reconnects them in lockstep — a synchronized herd that just re-storms
// the recovered listener each tick. jitter decorrelates the herd; the ESCALATION (the
// doubling toward the ceiling) stays on the raw, un-jittered value so the growth is
// deterministic. Used only by the run loop's reconnect + the config cache's boot-load loop.

// jitter returns ms perturbed by ±25% so reconnect/config-load sleeps across many workers
// decorrelate instead of firing in lockstep. ms <= 0 is returned unchanged. Callers
// escalate on the RAW ms (jitter only shapes the sleep), so the sequence still climbs
// deterministically toward its ceiling.
export function jitter(ms: number): number {
  if (ms <= 0) return ms;
  const span = ms / 2; // the full ±25% window is ms/2 wide
  return Math.round(ms - span / 2 + Math.random() * (span + 1));
}

// sleep resolves after ms, or early (rejecting-free) when the signal aborts — the ctx-
// cancellable sleep the reconnect/boot loops use so a shutdown mid-backoff returns at once.
export function sleep(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    const t = setTimeout(done, ms);
    const onAbort = () => done();
    function done() {
      clearTimeout(t);
      signal.removeEventListener("abort", onAbort);
      resolve();
    }
    signal.addEventListener("abort", onAbort, { once: true });
  });
}
