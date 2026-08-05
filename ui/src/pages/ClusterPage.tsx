import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api, type ClusterMember, type ConnectedWorker as ConnectedWorkerT, MEMBER_STATUS_BG } from '../api'
import { formatDateTime } from '../utils'

// ClusterPage is the running-fleet view — "like kubectl get nodes", grouped by
// TIER so the execution architecture is legible: the Control plane (sweepers +
// API), the Brokers (own a shard tile, claim work, fan it out over Connect), and
// in-process Workers / Reactors. NOTE: a remote worker (ROLE=worker,
// the SDK client) holds no DB and does NOT report its OWN cluster_members row —
// it connects INTO a broker and pulls work. Each broker publishes its live set
// of connected workers in its heartbeat, so expanding a broker row lists the
// workers on it (id, the kinds each can execute, and its slot usage) — the
// analogue of a node's pods.
//
// Liveness is rendered VERBATIM from the server's `status` (and `ready`) —
// never recomputed client-side from last_heartbeat (same rule as resource
// `phase`). A NotReady row (a member that stopped beating) stays listed,
// greyed, until the ControlPlane's ClusterMemberGC reclaims it past the TTL; a
// cleanly-shut-down member deregisters and disappears at once. The 10s refetch
// surfaces the NotReady flip and the eventual disappearance.
export function ClusterPage() {
  const { data, isPending, isError, error } = useQuery({
    queryKey: ['cluster-members'],
    queryFn: () => api.listClusterMembers(),
    refetchInterval: 10000,
    refetchIntervalInBackground: false,
    placeholderData: (prev) => prev,
  })

  const [expanded, setExpanded] = useState<Set<string>>(new Set())
  const toggle = (id: string) =>
    setExpanded((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })

  if (isError) {
    return (
      <main className="flex-1 p-6">
        <div className="max-w-lg p-4 bg-red-50 border border-red-200 rounded text-sm text-red-800">
          <div className="font-semibold mb-1">Failed to load cluster members</div>
          <div className="text-xs font-mono">{(error as Error)?.message}</div>
        </div>
      </main>
    )
  }

  const members = data ?? []
  const ready = members.filter((c) => c.status === 'Ready').length
  const notReady = members.length - ready

  // Group members into tiers so the execution architecture reads at a glance.
  // Each member goes into the FIRST tier whose match() accepts it (TIERS order
  // is specific → catch-all), so the 'Other' catch-all only ever holds genuinely
  // unclassified roles — never the control/broker rows already placed above.
  const byTier = new Map<string, ClusterMember[]>()
  for (const c of members) {
    const tier = TIERS.find((t) => t.match(c.role)) ?? TIERS[TIERS.length - 1]
    const list = byTier.get(tier.key) ?? []
    list.push(c)
    byTier.set(tier.key, list)
  }
  const tiers = TIERS.map((t) => ({
    ...t,
    members: (byTier.get(t.key) ?? []).sort((a, b) =>
      a.hostname !== b.hostname
        ? a.hostname.localeCompare(b.hostname)
        : a.member_id.localeCompare(b.member_id),
    ),
  })).filter((t) => t.members.length > 0)

  return (
    <main className="flex-1 flex flex-col overflow-hidden">
      <div className="flex-1 overflow-y-auto p-6 space-y-5">
        <section>
          <h2 className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">
            Cluster members
          </h2>
          <div className="text-sm text-slate-600 tabular-nums">
            {isPending ? (
              'Loading…'
            ) : (
              <>
                {members.length} member{members.length === 1 ? '' : 's'} · {ready} Ready ·{' '}
                <span className={notReady > 0 ? 'text-slate-700 font-medium' : ''}>{notReady} NotReady</span>
              </>
            )}
          </div>
          <p className="text-xs text-slate-400 mt-1 max-w-3xl">
            In-flight + Workers count a member's broker duty; members without it show a dash.
            Workers connect to a broker rather than registering — expand a member to list its own.
          </p>
        </section>

        {!isPending && tiers.length === 0 && (
          <div className="py-6 text-center text-slate-400">No members reporting.</div>
        )}

        {tiers.map((t) => (
          <section key={t.key}>
            <div className="flex items-baseline gap-2 mb-1">
              <h3 className="text-sm font-semibold text-slate-800">{t.label}</h3>
              <span className="text-xs text-slate-500 tabular-nums">
                {t.members.length} · {t.members.filter((c) => c.status === 'Ready').length} Ready
              </span>
            </div>
            <p className="text-xs text-slate-400 mb-2 max-w-3xl">{t.blurb}</p>
            <div className="bg-white border border-slate-200 rounded overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-slate-500 bg-slate-50 border-b border-slate-200">
                    <th className="font-normal py-1.5 pl-3 pr-2 w-6"></th>
                    <th className="font-normal py-1.5 px-2">Name</th>
                    <th className="font-normal py-1.5 px-2">Role</th>
                    <th className="font-normal py-1.5 px-2">Status</th>
                    <th className="font-normal py-1.5 px-2">Shards</th>
                    {/* In-flight (tasks parked awaiting a worker) + connected-worker
                        count are BROKER-duty metrics, shown in EVERY tier's table for
                        a uniform fleet view — a real number for a broker-capable
                        member (role all / broker / broker-<kind>), a dash otherwise.
                        A role="all" node shows them just like a dedicated broker;
                        role="control" shows a dash. */}
                    <th className="font-normal py-1.5 px-2 text-right">In-flight</th>
                    <th className="font-normal py-1.5 px-2 text-right">Workers</th>
                    <th className="font-normal py-1.5 px-2">Version</th>
                    <th className="font-normal py-1.5 px-2">Uptime</th>
                    <th className="font-normal py-1.5 px-2">Last heartbeat</th>
                  </tr>
                </thead>
                <tbody>
                  {t.members.map((c) => (
                    <MemberRow
                      key={c.member_id}
                      c={c}
                      isOpen={expanded.has(c.member_id)}
                      dim={c.status === 'NotReady' ? 'text-slate-400' : 'text-slate-700'}
                      onToggle={() => toggle(c.member_id)}
                    />
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        ))}
      </div>
    </main>
  )
}

// TIERS groups members by execution role. A role string is "all" | "control" |
// "worker[-<kind>]" | "broker[-<kind>]" | "react"; a member is placed in the
// first tier whose match() accepts it. "all" (the dev all-in-one) is its own
// tier since it is every duty at once.
const TIERS: {
  key: string
  label: string
  blurb: string
  match: (role: string) => boolean
}[] = [
  {
    key: 'control',
    label: 'Control plane',
    blurb: 'Runs the sweepers and the API. No provider work.',
    match: (r) => r === 'control',
  },
  {
    key: 'broker',
    label: 'Brokers',
    blurb: 'Claim work and dispatch it to connected workers.',
    match: (r) => r === 'broker' || r.startsWith('broker-'),
  },
  {
    key: 'react',
    label: 'Reactors',
    blurb: 'Run lifecycle reactions (side effects on transitions).',
    match: (r) => r === 'react',
  },
  {
    key: 'allinone',
    label: 'All-in-one',
    blurb: 'A single process running every duty (control + claim + react) — the dev/default mode.',
    match: (r) => r === 'all',
  },
  {
    key: 'other',
    label: 'Other',
    blurb: 'Members whose role doesn’t match a known tier.',
    match: () => true, // catch-all; must be last
  },
]

function MemberRow({
  c,
  isOpen,
  dim,
  onToggle,
}: {
  c: ClusterMember
  isOpen: boolean
  dim: string
  onToggle: () => void
}) {
  // Broker-duty metrics are shown for a broker-capable member (all / broker /
  // broker-<kind>) and dashed otherwise — derived per member so the row is correct
  // in every tier (a role="all" node in the All-in-one tier shows real numbers).
  const isBroker = hasBrokerCapability(c.role)
  return (
    <>
      <tr
        className={`border-b border-slate-100 hover:bg-slate-50 cursor-pointer ${dim}`}
        onClick={onToggle}
      >
        <td className="py-1.5 pl-3 pr-2 text-slate-400 select-none">{isOpen ? '▼' : '▶'}</td>
        <td className="py-1.5 px-2">
          <div className="font-medium">{c.hostname}</div>
          <div className="font-mono text-[10px] text-slate-400 truncate max-w-[16rem]" title={c.member_id}>
            {c.member_id}
          </div>
        </td>
        <td className="py-1.5 px-2">
          <span className="inline-block px-1.5 py-0.5 rounded text-[11px] bg-indigo-50 text-indigo-800 border border-indigo-200">
            {c.role}
          </span>
        </td>
        <td className="py-1.5 px-2">
          <span className={`inline-block px-1.5 py-0.5 rounded text-[11px] text-white ${MEMBER_STATUS_BG[c.status]}`}>
            {c.status.toLowerCase()}
          </span>
        </td>
        <td className="py-1.5 px-2 font-mono text-xs">{formatShardRange(c.shards)}</td>
        {/* In-flight + connected-worker count: a real number for a broker-capable
            member, a dash for a non-broker one (control / react) — same columns,
            uniform across every tier. */}
        {isBroker ? (
          <>
            <td className="py-1.5 px-2 text-right tabular-nums">{c.in_flight}</td>
            <td className="py-1.5 px-2 text-right tabular-nums">{c.workers?.length ?? 0}</td>
          </>
        ) : (
          <>
            <td className="py-1.5 px-2 text-right text-slate-300">—</td>
            <td className="py-1.5 px-2 text-right text-slate-300">—</td>
          </>
        )}
        <td className="py-1.5 px-2 font-mono text-xs text-slate-600">{c.version}</td>
        <td className="py-1.5 px-2 text-xs" title={c.started_at}>{c.uptime}</td>
        <td className="py-1.5 px-2 text-xs text-slate-500 tabular-nums" title={c.last_heartbeat}>
          {formatDateTime(c.last_heartbeat)}
        </td>
      </tr>
      {isOpen && (
        <tr className="bg-slate-50 border-b border-slate-200">
          <td></td>
          {/* colSpan spans the 9 uniform columns (toggle is its own leading cell). */}
          <td colSpan={9} className="py-3 px-2">
            <div className="grid grid-cols-2 gap-x-6 gap-y-1 text-xs text-slate-700 mb-3 max-w-2xl">
              <Field label="Member ID" value={c.member_id} mono />
              <Field label="PID" value={String(c.pid)} mono />
              <Field label="Shards" value={formatShardRange(c.shards)} mono />
              <Field label="Started" value={formatDateTime(c.started_at)} />
              <Field label="Age (heartbeat)" value={`${c.age_seconds}s ago`} />
            </div>
            {/* Connected workers — shown for a broker-capable member (incl. a
                role="all" node). Each broker publishes its live PollWork-stream set
                in its heartbeat; a worker serves one or more kinds. Absent/empty on
                a broker with nothing connected right now. */}
            {isBroker && <ConnectedWorkers workers={c.workers} />}
            <div className="text-[11px] font-medium text-slate-500 uppercase tracking-wider mb-1">
              Runtime config
            </div>
            <pre className="p-2 bg-slate-100 border border-slate-200 rounded text-xs text-slate-800 overflow-x-auto whitespace-pre-wrap">
              {JSON.stringify(c.config, null, 2)}
            </pre>
          </td>
        </tr>
      )}
    </>
  )
}

// ConnectedWorkers renders the workers connected to a broker — one row
// per worker with the kinds it can execute (a worker may serve several) and its
// live/ceiling task slots. Shown inside an expanded broker row.
function ConnectedWorkers({ workers }: { workers?: ConnectedWorkerT[] }) {
  const list = workers ?? []
  return (
    <div className="mb-3">
      <div className="text-[11px] font-medium text-slate-500 uppercase tracking-wider mb-1">
        Connected workers <span className="tabular-nums">({list.length})</span>
      </div>
      {list.length === 0 ? (
        <div className="text-xs text-slate-400">No workers connected to this broker.</div>
      ) : (
        <div className="bg-white border border-slate-200 rounded overflow-x-auto max-w-2xl">
          <table className="w-full text-xs">
            <thead>
              <tr className="text-left text-slate-500 bg-slate-50 border-b border-slate-200">
                <th className="font-normal py-1 px-2">Worker</th>
                <th className="font-normal py-1 px-2">Kinds</th>
                <th className="font-normal py-1 px-2 text-right">Slots</th>
              </tr>
            </thead>
            <tbody>
              {[...list]
                .sort((a, b) => a.worker_id.localeCompare(b.worker_id))
                .map((w) => (
                  <tr key={w.worker_id} className="border-b border-slate-100 last:border-0">
                    <td className="py-1 px-2 font-mono text-slate-700">
                      {w.worker_id}
                      {/* Identity provenance: the broker OBSERVED this id (never a
                          self-report). Badge shows the source; an unverified id (a bare
                          peer IP, i.e. no mTLS/mesh identity) is flagged so an operator
                          knows the attribution isn't cryptographically vouched-for. */}
                      {w.id_source && (
                        <span
                          className={`ml-2 rounded px-1 text-[10px] align-middle ${
                            w.id_verified
                              ? 'bg-emerald-100 text-emerald-700'
                              : 'bg-amber-100 text-amber-700'
                          }`}
                          title={
                            w.id_verified
                              ? `verified identity (${w.id_source})`
                              : `unverified identity (${w.id_source}) — no mTLS/mesh client identity`
                          }
                        >
                          {w.id_source}
                        </span>
                      )}
                    </td>
                    <td className="py-1 px-2">
                      <div className="flex flex-wrap gap-1">
                        {w.kinds.length === 0 ? (
                          <span className="text-slate-400">—</span>
                        ) : (
                          // kind_versions is PARALLEL to kinds (kinds[i] served at
                          // kind_versions[i]); a missing/short entry defaults to
                          // kind_version 1, so each badge reads "kind/vN".
                          w.kinds.map((k, i) => {
                            const label = `${k}/v${w.kind_versions?.[i] ?? 1}`
                            return (
                              <span
                                key={label}
                                className="inline-block px-1.5 py-0.5 rounded text-[10px] bg-sky-50 text-sky-800 border border-sky-200"
                              >
                                {label}
                              </span>
                            )
                          })
                        )}
                      </div>
                    </td>
                    <td className="py-1 px-2 text-right tabular-nums text-slate-600">
                      {w.inflight}/{w.max_inflight}
                    </td>
                  </tr>
                ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function Field({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="flex gap-2">
      <span className="text-slate-400 w-32 shrink-0">{label}</span>
      <span className={mono ? 'font-mono break-all' : ''}>{value}</span>
    </div>
  )
}

// formatShardRange renders the owned [lo, hi] inclusive span as "lo-hi"
// (or "N" for a single shard); null/empty (no owned slice — e.g. a single-pod
// dev process sweeping everything) renders as "—".
function formatShardRange(shards: [number, number] | null): string {
  if (!shards) return '—'
  const [lo, hi] = shards
  return lo === hi ? String(lo) : `${lo}-${hi}`
}

// hasBrokerCapability reports whether a member RUNS a broker duty — the one thing
// that gives it a meaningful in-flight count + connected-worker set. It is derived
// PER MEMBER from the role string so the fleet view is uniform across every
// deployment topology: "all" (control + broker in one process) is broker-capable
// just like a dedicated "broker" or a per-kind "broker-<kind>"; a "control"-only or
// "react" member is not. The in-flight/worker columns show a real number for a
// broker-capable member and a dash otherwise — the same table shape for all tiers,
// rather than a hardcoded brokers-only table.
function hasBrokerCapability(role: string): boolean {
  return role === 'all' || role === 'broker' || role.startsWith('broker-')
}
