// config.ts reads the worker's environment configuration — the twelve-factor "zero-config"
// path. A deployment sets the worker knobs and calls serve(); serve reads them here. This
// is the sole config path on serve (runWorker takes RunOptions instead).
//
// Each variable is optional with a documented default (the Default* consts below are the
// single source of truth). A SET-but-malformed value (a bad duration, a non-numeric count)
// is a LOUD error thrown from loadConfig, never a silently dropped default.

// Default* — the SDK's config defaults. One source of truth: loadConfig seeds these, then
// overlays only the SET env vars.
export const DefaultMaxInflight = 100;
export const DefaultDrainTimeoutMs = 30_000;
export const DefaultTLSReloadIntervalMs = 30_000;
export const DefaultConfigRefreshMs = 60_000;
export const DefaultHealthPollMs = 2_000;
export const DefaultReconnectCeilingMs = 30_000;

// Config is the resolved worker configuration. Byte-for-byte the fields Go's config struct
// carries; durations are milliseconds (the JS idiom) rather than Go time.Duration.
export interface Config {
  brokerAddr: string;
  tlsCertFile: string;
  tlsKeyFile: string;
  tlsCAFile: string;
  tlsReloadIntervalMs: number;
  maxInflight: number;
  drainGraceMs: number;
  configRefreshMs: number;
  healthPollMs: number;
  reconnectCeilingMs: number;
  workerKinds: Set<string> | null; // WORKER_KINDS allowlist; null = all
  healthAddr: string; // HEALTH_ADDR probe listener; "" = disabled
}

// loadConfig reads process.env and returns the resolved Config, seeded with the Default*
// consts and overlaid with only the SET, well-formed variables. It THROWS on a set-but-
// malformed value (naming the offending var + expected type) — bad config fails loud at
// startup, never silently falls back.
export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  return {
    brokerAddr: env.BROKER_ADDR ?? "",
    tlsCertFile: env.TLS_CERT_FILE ?? "",
    tlsKeyFile: env.TLS_KEY_FILE ?? "",
    tlsCAFile: env.BROKER_CA_FILE ?? "",
    tlsReloadIntervalMs: parseDurationMs(env.TLS_RELOAD_INTERVAL, "TLS_RELOAD_INTERVAL", DefaultTLSReloadIntervalMs),
    maxInflight: parseCount(env.WORKER_MAX_PARALLEL, "WORKER_MAX_PARALLEL", DefaultMaxInflight),
    drainGraceMs: parseDurationMs(env.WORKER_DRAIN_TIMEOUT, "WORKER_DRAIN_TIMEOUT", DefaultDrainTimeoutMs),
    configRefreshMs: DefaultConfigRefreshMs,
    healthPollMs: DefaultHealthPollMs,
    reconnectCeilingMs: DefaultReconnectCeilingMs,
    workerKinds: parseKindFilter(env.WORKER_KINDS),
    healthAddr: (env.HEALTH_ADDR ?? "").trim(),
  };
}

// parseDurationMs parses a Go-style duration string ("30s", "1500ms", "2m") or a bare
// number of milliseconds into ms. An unset value keeps the default; a SET-but-unparseable
// value throws (loud). Mirrors envconfig's time.Duration parsing on the Go side.
function parseDurationMs(v: string | undefined, name: string, def: number): number {
  if (v === undefined || v.trim() === "") return def;
  const ms = durationToMs(v.trim());
  if (ms === null) {
    throw new Error(`converge: invalid worker environment config: ${name}=${JSON.stringify(v)} is not a duration (e.g. "30s", "500ms")`);
  }
  return ms;
}

// durationToMs converts a Go duration literal to milliseconds, or null if unparseable.
// Supports a leading sign, a decimal, and the ns/us/µs/ms/s/m/h unit suffixes Go accepts
// (the ones a worker knob realistically uses). A bare integer is treated as milliseconds.
function durationToMs(s: string): number | null {
  if (/^-?\d+$/.test(s)) return Number(s); // bare integer = ms
  const re = /(-?\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h)/gy;
  const unitMs: Record<string, number> = { ns: 1e-6, us: 1e-3, "µs": 1e-3, ms: 1, s: 1000, m: 60_000, h: 3_600_000 };
  let total = 0;
  let matched = false;
  let idx = 0;
  for (let m = re.exec(s); m !== null; m = re.exec(s)) {
    if (m.index !== idx) return null; // gap → malformed
    total += Number(m[1]) * unitMs[m[2]];
    idx = re.lastIndex;
    matched = true;
  }
  if (!matched || idx !== s.length) return null;
  return Math.round(total);
}

// parseCount parses a non-negative integer worker count; unset keeps the default, a
// SET-but-non-integer throws.
function parseCount(v: string | undefined, name: string, def: number): number {
  if (v === undefined || v.trim() === "") return def;
  if (!/^\d+$/.test(v.trim())) {
    throw new Error(`converge: invalid worker environment config: ${name}=${JSON.stringify(v)} is not a non-negative integer`);
  }
  return Number(v.trim());
}

// parseKindFilter parses the comma-separated WORKER_KINDS env into a Set, or null (serve
// all registered kinds).
function parseKindFilter(csv: string | undefined): Set<string> | null {
  const trimmed = (csv ?? "").trim();
  if (trimmed === "") return null;
  const s = new Set<string>();
  for (const k of trimmed.split(",")) {
    const t = k.trim();
    if (t !== "") s.add(t);
  }
  return s;
}

// ─────────────────────────────────────────────────────────────────────────
// Logging — LOG_LEVEL + LOG_FORMAT. A minimal leveled logger so the SDK's internal warnings
// (reconnect, config refresh) are observable without a third-party log dependency.
// ─────────────────────────────────────────────────────────────────────────

export type LogLevel = "debug" | "info" | "warn" | "error";

export interface Logger {
  debug(msg: string, fields?: Record<string, unknown>): void;
  info(msg: string, fields?: Record<string, unknown>): void;
  warn(msg: string, fields?: Record<string, unknown>): void;
  error(msg: string, fields?: Record<string, unknown>): void;
}

const levelOrder: Record<LogLevel, number> = { debug: 0, info: 1, warn: 2, error: 3 };

// newLogger builds the SDK logger from LOG_LEVEL (a level name) + LOG_FORMAT (text|json).
// Defaults: info / text. Writes to stderr.
export function newLogger(env: NodeJS.ProcessEnv = process.env): Logger {
  const min = parseLevel(env.LOG_LEVEL);
  const json = (env.LOG_FORMAT ?? "").toLowerCase() === "json";
  const emit = (level: LogLevel, msg: string, fields?: Record<string, unknown>) => {
    if (levelOrder[level] < levelOrder[min]) return;
    const line = json
      ? JSON.stringify({ level, msg, ...(fields ?? {}) })
      : `[${level}] ${msg}${fields ? " " + fmtFields(fields) : ""}`;
    process.stderr.write(line + "\n");
  };
  return {
    debug: (m, f) => emit("debug", m, f),
    info: (m, f) => emit("info", m, f),
    warn: (m, f) => emit("warn", m, f),
    error: (m, f) => emit("error", m, f),
  };
}

function parseLevel(v: string | undefined): LogLevel {
  switch ((v ?? "").toLowerCase()) {
    case "debug":
      return "debug";
    case "warn":
      return "warn";
    case "error":
      return "error";
    default:
      return "info";
  }
}

function fmtFields(fields: Record<string, unknown>): string {
  return Object.entries(fields)
    .map(([k, v]) => `${k}=${typeof v === "string" ? v : JSON.stringify(v)}`)
    .join(" ");
}
