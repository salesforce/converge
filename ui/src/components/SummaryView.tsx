import { useQuery } from '@tanstack/react-query'
import { api, type Kind, type Phase, PHASE_VALUES, PHASE_BG, PHASE_LABELS, countForPhase, type ReadinessKindCount } from '../api'

interface Props {
  // Owner scope from the filter bar — "kind/name" refs. Empty = cross-cluster.
  ownerRefs: string[]
}

// Per-phase model from the server (totals + by_kind): each row carries the
// phase-bucket counts, read via countForPhase(row, p).
const BUCKETS: Phase[] = PHASE_VALUES

export function SummaryView({ ownerRefs }: Props) {
  const refsKey = ownerRefs.slice().sort().join(',')
  const { data: summary, isPending, isError, error } = useQuery({
    queryKey: ['summary-scoped', refsKey],
    queryFn: () => api.getScopedSummary(ownerRefs),
    refetchInterval: 10000,
    refetchIntervalInBackground: false,
    placeholderData: (prev) => prev,
  })

  if (isError) {
    return (
      <div className="p-6">
        <div className="max-w-lg p-4 bg-red-50 border border-red-200 rounded text-sm text-red-800">
          <div className="font-semibold mb-1">Failed to load summary</div>
          <div className="text-xs font-mono">{(error as Error)?.message}</div>
        </div>
      </div>
    )
  }
  if (isPending || !summary) {
    return <SummarySkeleton />
  }

  const totals = summary.totals ?? { ready: 0, reconciling: 0, degraded: 0, failed: 0, deleting: 0, orphaned: 0, quarantined: 0 }
  const byKind = summary.by_kind ?? []
  const totalCount = PHASE_VALUES.reduce((s, p) => s + countForPhase(totals, p), 0)

  const kinds = Array.from(new Set(byKind.map((c) => c.kind))) as Kind[]
  const matrix: Record<string, ReadinessKindCount> = {}
  for (const c of byKind) {
    matrix[c.kind] = c
  }

  return (
    <div className="flex-1 overflow-y-auto p-6 space-y-6">
      <ScopeBadge ownerRefs={ownerRefs} ownerNames={summary.owners.map((o) => o.name)} />

      <section>
        <h2 className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-2">
          Readiness ({totalCount.toLocaleString()} total)
        </h2>
        <div className="flex gap-3">
          {BUCKETS.map((p) => (
            <div key={p} className={`flex-1 p-4 rounded-lg ${PHASE_BG[p]} text-white`}>
              <div className="text-3xl font-semibold tabular-nums">{countForPhase(totals, p).toLocaleString()}</div>
              <div className="text-xs uppercase tracking-wider opacity-90">{PHASE_LABELS[p]}</div>
            </div>
          ))}
        </div>
      </section>

      <section>
        <h2 className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-2">
          By kind
        </h2>
        <div className="overflow-x-auto bg-white border border-slate-200 rounded">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-slate-500 bg-slate-50">
                <th className="font-normal py-1.5 pl-3 pr-4">Kind</th>
                {BUCKETS.map((p) => (
                  <th key={p} className="font-normal py-1.5 px-2 text-right tabular-nums">{PHASE_LABELS[p]}</th>
                ))}
                <th className="font-normal py-1.5 px-3 text-right tabular-nums">total</th>
              </tr>
            </thead>
            <tbody>
              {kinds.map((k) => {
                const row = matrix[k] ?? { kind: k, ready: 0, reconciling: 0, degraded: 0, failed: 0, deleting: 0, orphaned: 0, quarantined: 0 }
                const sum = PHASE_VALUES.reduce((s, p) => s + countForPhase(row, p), 0)
                return (
                  <tr key={k} className="border-t border-slate-200 text-slate-800">
                    <td className="py-1 pl-3 pr-4 font-mono text-xs">{k}</td>
                    {BUCKETS.map((p) => (
                      <td key={p} className="py-1 px-2 text-right tabular-nums">{countForPhase(row, p).toLocaleString()}</td>
                    ))}
                    <td className="py-1 px-3 text-right tabular-nums font-medium">{sum.toLocaleString()}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  )
}

function ScopeBadge({ ownerRefs, ownerNames }: { ownerRefs: string[]; ownerNames: string[] }) {
  if (ownerRefs.length === 0) {
    return (
      <p className="text-xs text-slate-500">
        Showing summary across <strong>all owners</strong>. Add an Owner filter to scope.
      </p>
    )
  }
  if (ownerRefs.length === 1) {
    // ownerRefs[0] is "kind/name"; the echoed owner name is preferred, falling
    // back to the name portion of the ref while the summary loads.
    const refName = ownerRefs[0].slice(ownerRefs[0].indexOf('/') + 1)
    return (
      <p className="text-xs text-slate-500">
        Owner:{' '}
        <span className="font-mono text-slate-700">
          {ownerNames[0] ?? refName}
        </span>
      </p>
    )
  }
  return (
    <p className="text-xs text-slate-500">
      Showing summary across <strong>{ownerRefs.length} owners</strong>:{' '}
      <span className="font-mono text-slate-700">{ownerNames.join(', ')}</span>
    </p>
  )
}

function SummarySkeleton() {
  return (
    <div className="flex-1 overflow-y-auto p-6 space-y-6 animate-pulse" aria-busy="true" aria-live="polite">
      <div className="h-3 w-24 bg-slate-200 rounded" />
      <div className="grid grid-cols-5 gap-3">
        {Array.from({ length: 5 }).map((_, i) => (
          <div key={i} className="h-20 bg-slate-200 rounded-lg" />
        ))}
      </div>
      <div className="h-32 bg-slate-200 rounded" />
      <div className="h-24 bg-slate-200 rounded" />
    </div>
  )
}
