import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useQueries, useQuery } from '@tanstack/react-query'
import { useVirtualizer } from '@tanstack/react-virtual'
import {
  api,
  type Phase,
  type Resource,
  type RootPageItem,
  type TopologyBucket,
  PHASE_VALUES,
  PHASE_FILL,
  PHASE_LABELS,
} from '../api'
import { GroupByPicker } from './GroupByPicker'
import { useDebounced } from '../hooks'

// TopologyView is the topology surface for very large, deeply-nested
// trees (group_by levels × hundreds of buckets each). A spatial chart
// (node-link graph, proportional icicle) is unusable at this fan-out:
// hundreds of siblings collapse into unreadable slivers and the layout
// dominates the main thread.
//
// Instead we render an EXPAND-IN-PLACE indented tree, flattened into a
// virtualized list. Click a row to reveal its children nested below it;
// click again to collapse. The parent stays in view, so navigation is
// fluid both ways and you never lose your place. Crucially the count
// and the drill agree: each row shows "N children · M resources", and
// expanding shows exactly those N children indented underneath.
//
// Each expanded node lazily fetches its own children via the same
// /api/topology (buckets) and /topology/leaves (bottom) calls; React
// Query caches per-path, so collapse→re-expand is instant and the 30s
// poll only refreshes branches that are open. We never fetch the whole
// tree — only the open branches.

// Per-node child fetch cap (server max is 500). The server orders
// buckets alphabetically; we sort each sibling group failures-first
// CLIENT-SIDE, so a failing bucket whose name sorts past this cap won't
// surface until server-side paging is added. We surface that truncation
// per-node so it's never silent.
const LEVEL_LIMIT = 500

const ROW_H = 38
const INDENT_PX = 18
// X of the first depth-guide line. A row at depth D has paddingLeft
// 8 + D*INDENT_PX and a 16px-wide twisty, so an ancestor at level L has
// its twisty centered at 8 + L*INDENT_PX + 8 = GUIDE_X0 + L*INDENT_PX.
const GUIDE_X0 = 16

// OwnerSel is the Topology tab's selected owner, addressed by its public
// (kind, name). name is display-only; kind+name key every topology call.
interface OwnerSel {
  kind: string
  name: string
}

interface Props {
  // Seed owner from the page's top filter WHEN exactly one owner is selected —
  // a convenience default only. The Topology tab owns its own selection (the
  // in-tab OwnerPicker below), so it never depends on the top filter and is
  // always usable, even with 0 or N owners filtered above. The ref is a
  // "kind/name" string (the owner-filter encoding).
  initialOwnerRef?: string
  onNodeClick: (kind: string, name: string) => void
}

// parseOwnerRef splits a "kind/name" owner ref on its FIRST '/' (a kind never
// contains '/'). Returns null for a malformed/empty ref.
function parseOwnerRef(ref: string | undefined): OwnerSel | null {
  if (!ref) return null
  const i = ref.indexOf('/')
  if (i <= 0) return null
  return { kind: ref.slice(0, i), name: ref.slice(i + 1) }
}

// A node's path is the list of `key:value` segments from the root down
// to it — the same encoding the topology `path` query param uses.
const pathKey = (path: string[]) => path.join('|') || '/'

// ── severity / sort ───────────────────────────────────────────────────

// Severity weight for the failures-first sort. Higher floats higher.
const PHASE_SEVERITY: Record<Phase, number> = {
  Failed: 6,
  Degraded: 5,
  Quarantined: 4,
  Reconciling: 3,
  Orphaned: 2,
  Deleting: 1,
  Ready: 0,
}

function severityOf(byReadiness: Partial<Record<Phase, number>>): {
  severity: number
  count: number
} {
  let severity = 0
  let count = 0
  for (const p of PHASE_VALUES) {
    const n = byReadiness[p] ?? 0
    if (n === 0) continue
    const s = PHASE_SEVERITY[p]
    if (s > severity) {
      severity = s
      count = n
    } else if (s === severity) {
      count += n
    }
  }
  return { severity, count }
}

// ── node model ────────────────────────────────────────────────────────

// A flattened, render-ready tree row. The tree is computed top-down from
// the expanded set + the per-node child query results, then linearized
// (DFS) into this array so the virtualizer can window it.
interface FlatRow {
  // Stable React key + expanded-set membership key (the node's path).
  key: string
  depth: number // indent level (0 = top group level)
  label: string
  // Group-by label key for a bucket, or the resource kind for a leaf.
  keyName: string
  total: number // descendant resource count (bucket) or 1 (leaf)
  byReadiness: Partial<Record<Phase, number>>
  severity: number
  severityCount: number
  isLeaf: boolean
  // State pulled in during flatten.
  isExpanded: boolean
  isLoading: boolean
  // Leaf-only: the resource's public (kind, name) — used to navigate.
  leafKind?: string
  leafName?: string
  phase?: Phase
}

function sortBuckets(rows: { severity: number; severityCount: number; total: number; label: string }[]) {
  rows.sort((a, b) => {
    if (a.severity !== b.severity) return b.severity - a.severity
    if (a.severityCount !== b.severityCount) return b.severityCount - a.severityCount
    if (a.total !== b.total) return b.total - a.total
    return a.label.localeCompare(b.label)
  })
}

export function TopologyView({ initialOwnerRef, onNodeClick }: Props) {
  // The Topology tab owns its OWN owner selection via the in-tab OwnerPicker,
  // so it's always usable regardless of the page's top filter. Seed from
  // initialOwnerRef (the top filter, when it names exactly one owner) purely as
  // a convenience; the user can change it here without touching the top filter.
  const [owner, setOwner] = useState<OwnerSel | null>(() => parseOwnerRef(initialOwnerRef))
  // If the page's single-owner filter changes while this tab is mounted, adopt
  // it as the seed (only when it actually names one) — but never clear an
  // in-tab pick just because the top filter was cleared.
  const [lastSeed, setLastSeed] = useState(initialOwnerRef)
  if (initialOwnerRef !== lastSeed) {
    setLastSeed(initialOwnerRef)
    const seed = parseOwnerRef(initialOwnerRef)
    if (seed && (seed.kind !== owner?.kind || seed.name !== owner?.name)) {
      setOwner(seed)
    }
  }
  const ownerKind = owner?.kind ?? ''
  const ownerName = owner?.name ?? ''
  // Stable identity string for query keys + reset logic.
  const ownerRef = owner ? `${owner.kind}/${owner.name}` : ''

  // group_by starts EMPTY — the core hardcodes NO domain-specific default (any
  // fixed key set would be org-specific). The user picks from the
  // label keys actually present on this owner's children, discovered by
  // GroupByPicker via api.getTopologyKeys (the union of the children's own label
  // keys). Until a key is chosen the view shows the "pick a key" empty state.
  const [groupBy, setGroupBy] = useState<string[]>([])
  // Set of expanded node path keys. Root ('/') is implicitly expanded so
  // the first level always renders. Changing group_by resets to root.
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set([pathKey([])]))

  const handleGroupByChange = useCallback((next: string[]) => {
    setGroupBy(next)
    setExpanded(new Set([pathKey([])]))
  }, [])

  // Switching to a DIFFERENT owner (or clearing) resets the grouping + expansion
  // so we never render a stale tree keyed to the previous owner's label space.
  // A name-only annotation of the SAME owner (seed-name resolution) must NOT
  // reset — that would wipe the user's group-by mid-view — so it only patches
  // the display name.
  const handleOwnerChange = useCallback(
    (next: OwnerSel | null) => {
      setOwner(next)
      // Reset only when the owner IDENTITY (kind/name) changes. The owner
      // picker always supplies the real name, so there's no name-only patch to
      // guard against here. Invoked only from event handlers, so the `owner`
      // closure is fresh.
      const nextRef = next ? `${next.kind}/${next.name}` : ''
      const curRef = owner ? `${owner.kind}/${owner.name}` : ''
      if (nextRef !== curRef) {
        setGroupBy([])
        setExpanded(new Set([pathKey([])]))
      }
    },
    [owner],
  )

  const toggle = useCallback((key: string) => {
    setExpanded((prev) => {
      const next = new Set(prev)
      if (next.has(key)) {
        // Collapse this node and every descendant so re-expanding a
        // parent doesn't resurrect a stale deep-open subtree.
        for (const k of next) if (k === key || k.startsWith(key + '|')) next.delete(k)
      } else {
        next.add(key)
      }
      return next
    })
  }, [])

  // The expanded nodes whose children we must fetch. An expanded node at
  // depth < groupBy.length fetches buckets; at depth === groupBy.length
  // it fetches leaves. We resolve the path of each expanded key.
  const expandedPaths = useMemo(
    () => Array.from(expanded).map((k) => (k === pathKey([]) ? [] : k.split('|'))),
    [expanded],
  )
  // With no grouping there is no tree to fetch — keep both query lists
  // empty so we don't fire a path-less getTopologyLeaves (which would
  // try to page every leaf under the owner) behind the "pick a key"
  // notice. The root path also satisfies `length === groupBy.length`
  // when groupBy is empty, which is what would otherwise leak it in.
  const bucketPaths = groupBy.length === 0 ? [] : expandedPaths.filter((p) => p.length < groupBy.length)
  const leafPaths = groupBy.length === 0 ? [] : expandedPaths.filter((p) => p.length === groupBy.length)

  const bucketQueries = useQueries({
    queries: bucketPaths.map((path) => ({
      queryKey: ['topology', ownerRef, groupBy.join(','), pathKey(path)],
      queryFn: () => api.getTopology(ownerKind, ownerName, groupBy, path, '', LEVEL_LIMIT),
      staleTime: 10_000,
      refetchInterval: 30_000,
    })),
  })
  const leafQueries = useQueries({
    queries: leafPaths.map((path) => ({
      queryKey: ['topology-leaves', ownerRef, pathKey(path)],
      queryFn: () => api.getTopologyLeaves(ownerKind, ownerName, path, '', LEVEL_LIMIT),
      staleTime: 10_000,
      refetchInterval: 30_000,
    })),
  })

  // Stable primitive dep keys so the flatten memo recomputes exactly when
  // results land or the expanded set changes (avoids inline .map in deps).
  const bucketsVersion = bucketQueries.map((q) => q.dataUpdatedAt).join(',')
  const leavesVersion = leafQueries.map((q) => q.dataUpdatedAt).join(',')
  const expandedKey = Array.from(expanded).sort().join(';')

  // The root query is the one whose path is empty ([]). Its index in
  // bucketPaths is whichever slot holds the zero-length path — don't
  // assume index 0 (Set iteration order happens to put root first, but
  // we shouldn't depend on that). Only the ROOT fetch failing should
  // blank the whole view: a deep branch's refetch failing (e.g. a
  // bucket deleted out from under an open subtree) must leave the rest
  // of the tree standing — react-query keeps that branch's last good
  // data, and an empty branch just renders as a closed/empty node.
  const rootIdx = bucketPaths.findIndex((p) => p.length === 0)
  const rootQuery = rootIdx >= 0 ? bucketQueries[rootIdx] : undefined
  const firstError = rootQuery?.error
  const rootLoading = !!rootQuery?.isPending && !rootQuery?.data

  // Flatten the expanded tree (DFS) into the virtualizer's row list.
  const rows = useMemo<FlatRow[]>(() => {
    // Index query results by path for O(1) lookup during the walk.
    const bucketsByPath = new Map<string, { buckets: TopologyBucket[]; total: number; loading: boolean }>()
    bucketPaths.forEach((path, i) => {
      const q = bucketQueries[i]
      bucketsByPath.set(pathKey(path), {
        buckets: q?.data?.buckets ?? [],
        total: q?.data?.total ?? 0,
        loading: !!q?.isPending && !q?.data,
      })
    })
    const leavesByPath = new Map<string, { resources: Resource[]; loading: boolean }>()
    leafPaths.forEach((path, i) => {
      const q = leafQueries[i]
      leavesByPath.set(pathKey(path), {
        resources: q?.data?.resources ?? [],
        loading: !!q?.isPending && !q?.data,
      })
    })

    const out: FlatRow[] = []
    const maxDepth = groupBy.length

    const walk = (parentPath: string[]) => {
      const depth = parentPath.length
      const pk = pathKey(parentPath)

      // Leaf level: parent already consumed all group_by keys.
      if (depth === maxDepth) {
        const leaves = leavesByPath.get(pk)
        if (!leaves) return
        const sorted = leaves.resources.map((r) => {
          const byReadiness = { [r.phase]: 1 }
          const { severity, count } = severityOf(byReadiness)
          return { r, byReadiness, severity, severityCount: count, label: r.name, total: 1 }
        })
        sortBuckets(sorted)
        for (const s of sorted) {
          out.push({
            key: `${s.r.kind}/${s.r.name}`,
            depth,
            label: s.r.name,
            keyName: s.r.kind,
            total: 1,
            byReadiness: s.byReadiness,
            severity: s.severity,
            severityCount: s.severityCount,
            isLeaf: true,
            isExpanded: false,
            isLoading: false,
            leafKind: s.r.kind,
            leafName: s.r.name,
            phase: s.r.phase,
          })
        }
        return
      }

      // Bucket level.
      const bucketsAt = bucketsByPath.get(pk)
      if (!bucketsAt) return
      const nextKey = groupBy[depth]
      const childIsLeafLevel = depth + 1 === maxDepth
      const sorted = bucketsAt.buckets.map((b) => {
        const { severity, count } = severityOf(b.by_readiness)
        return { b, severity, severityCount: count, label: b.bucket || `(no ${nextKey})`, total: b.total }
      })
      sortBuckets(sorted)
      for (const s of sorted) {
        const childPath = [...parentPath, `${nextKey}:${s.b.bucket}`]
        const ck = pathKey(childPath)
        const isExpanded = expanded.has(ck)
        // Child-of-child fetch state, surfaced on the bucket row.
        const childData = isExpanded
          ? childIsLeafLevel
            ? leavesByPath.get(ck)
            : bucketsByPath.get(ck)
          : undefined
        out.push({
          key: ck,
          depth,
          label: s.label,
          keyName: nextKey,
          total: s.b.total,
          byReadiness: s.b.by_readiness,
          severity: s.severity,
          severityCount: s.severityCount,
          isLeaf: false,
          isExpanded,
          isLoading: !!childData?.loading,
        })
        if (isExpanded) walk(childPath)
      }
      return
    }

    walk([])
    return out
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bucketsVersion, leavesVersion, expandedKey, groupBy])

  // Total distinct buckets that exist at the TOP level vs how many we
  // drew, for the "+N more not loaded" header (root truncation is the
  // one most likely to bite; deeper truncation is rarer at this shape).
  const rootData = rootQuery?.data
  const rootShown = rows.filter((r) => r.depth === 0).length
  const rootTotal = rootData?.total ?? rootShown
  const rootTruncated = rootTotal > rootShown
  const failingTop = rows.filter(
    (r) => r.depth === 0 && r.severity >= PHASE_SEVERITY.Degraded,
  ).length

  const parentRef = useRef<HTMLDivElement>(null)
  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => ROW_H,
    overscan: 20,
  })
  const items = virtualizer.getVirtualItems()

  // No owner picked yet → the tab is still fully usable: show a prominent
  // owner picker as the empty state, no dependence on the top filter.
  if (!owner) {
    return (
      <div className="flex-1 flex flex-col overflow-hidden">
        <div className="px-3 py-2 border-b border-slate-200 bg-white flex items-center gap-3">
          <span className="text-xs text-slate-500 uppercase tracking-wider">Owner</span>
          <OwnerPicker value={owner} onChange={handleOwnerChange} />
        </div>
        <CenteredNotice
          title="Pick an owner to render its topology"
          body="Search for a root resource above — the topology tree renders under it, grouped by the label keys you choose."
        />
      </div>
    )
  }

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <div className="px-3 py-2 border-b border-slate-200 bg-white flex items-center gap-4 flex-wrap">
        <div className="flex items-center gap-2">
          <span className="text-xs text-slate-500 uppercase tracking-wider">Owner</span>
          <OwnerPicker value={owner} onChange={handleOwnerChange} />
        </div>
        <span className="h-4 w-px bg-slate-200" aria-hidden="true" />
        <GroupByPicker ownerKind={ownerKind} ownerName={ownerName} value={groupBy} onChange={handleGroupByChange} />
        <div className="ml-auto flex items-center gap-3 text-xs text-slate-500">
          {failingTop > 0 && (
            <span className="text-rose-600 tabular-nums" title="top-level groups containing Failed or Degraded resources">
              {failingTop.toLocaleString()} with failures
            </span>
          )}
          {rootTruncated && (
            <span
              className="px-2 py-0.5 rounded bg-amber-100 text-amber-800 border border-amber-300"
              title={`Showing the alphabetically-first ${rootShown.toLocaleString()} of ${rootTotal.toLocaleString()} ${groupBy[0]}s. Failures-first sort applies to loaded groups only.`}
            >
              ⚠ {(rootTotal - rootShown).toLocaleString()} more {groupBy[0]}s not loaded
            </span>
          )}
        </div>
      </div>

      {groupBy.length === 0 ? (
        <CenteredNotice
          title="No grouping selected"
          body="Pick at least one Group By key above to render the topology."
        />
      ) : firstError ? (
        <div className="p-4">
          <div className="max-w-lg p-3 bg-red-50 border border-red-200 rounded text-xs text-red-800">
            <div className="font-semibold mb-1">Failed to load topology</div>
            <div className="font-mono">{(firstError as Error)?.message}</div>
          </div>
        </div>
      ) : rootLoading ? (
        <div className="flex-1 flex items-center justify-center text-sm text-slate-500" aria-busy="true">
          Loading topology…
        </div>
      ) : rows.length === 0 ? (
        <CenteredNotice title="Nothing here" body="No resources match the current grouping." />
      ) : (
        <div ref={parentRef} className="flex-1 overflow-auto">
          <div style={{ height: virtualizer.getTotalSize(), position: 'relative' }}>
            {items.map((vi) => {
              const row = rows[vi.index]
              return (
                <div
                  key={row.key}
                  style={{
                    position: 'absolute',
                    top: 0,
                    left: 0,
                    right: 0,
                    height: vi.size,
                    transform: `translateY(${vi.start}px)`,
                  }}
                >
                  <TreeRow
                    row={row}
                    onToggle={() => toggle(row.key)}
                    onLeafClick={onNodeClick}
                  />
                </div>
              )
            })}
          </div>
        </div>
      )}

      <Legend />
    </div>
  )
}

// ── row ───────────────────────────────────────────────────────────────

function TreeRow({
  row,
  onToggle,
  onLeafClick,
}: {
  row: FlatRow
  onToggle: () => void
  onLeafClick: (kind: string, name: string) => void
}) {
  const onClick = () => {
    if (row.isLeaf) {
      if (row.leafKind && row.leafName) onLeafClick(row.leafKind, row.leafName)
    } else {
      onToggle()
    }
  }
  const leafFill = row.isLeaf && row.phase ? PHASE_FILL[row.phase] : undefined

  return (
    <button
      type="button"
      onClick={onClick}
      className="relative w-full h-full flex items-center gap-2 pr-4 border-b border-slate-100 hover:bg-slate-50 text-left"
      style={{
        paddingLeft: 8 + row.depth * INDENT_PX,
        ...(leafFill ? { boxShadow: `inset 3px 0 0 0 ${leafFill}` } : null),
      }}
      title={
        row.isLeaf
          ? `${row.keyName} · ${row.label} · ${row.phase}`
          : `${row.label} — ${row.total.toLocaleString()} resources (click to ${row.isExpanded ? 'collapse' : 'expand'})`
      }
    >
      {/* Depth guide lines: one thin vertical rule per ancestor level,
          aligned to where that ancestor's twisty sits, so a deeply-
          nested row visibly connects back up its lineage. Spans the full
          row height; purely decorative (aria-hidden). */}
      {row.depth > 0 &&
        Array.from({ length: row.depth }, (_, lvl) => (
          <span
            key={lvl}
            aria-hidden
            className="absolute top-0 bottom-0 w-px bg-slate-200"
            style={{ left: GUIDE_X0 + lvl * INDENT_PX }}
          />
        ))}

      {/* Twisty (interior/last-group buckets) or a dot (leaves). */}
      <span className="w-4 shrink-0 text-center text-slate-400 text-xs">
        {row.isLeaf ? '•' : row.isLoading ? '…' : row.isExpanded ? '▾' : '▸'}
      </span>

      <div className="min-w-0 flex-1 flex items-baseline gap-2">
        <span className="text-[10px] text-slate-400 uppercase tracking-wider shrink-0">
          {row.keyName}
        </span>
        <span className="text-sm font-mono text-slate-800 truncate">{row.label}</span>
      </div>

      {/* Readiness bar — fixed width so rows align into a scannable column. */}
      {!row.isLeaf && (
        <div className="w-36 shrink-0 hidden sm:block">
          <ReadinessStrip byReadiness={row.byReadiness} total={row.total} />
        </div>
      )}

      {/* Count summary, right-aligned. Buckets show the rollup total; the
          drill below it reveals the children, so the two now agree. */}
      <div className="w-40 shrink-0 text-right tabular-nums text-xs">
        {row.severity >= PHASE_SEVERITY.Degraded ? (
          <span className="text-rose-600">
            {row.severityCount.toLocaleString()} {dominantLabel(row).toLowerCase()}
          </span>
        ) : row.isLeaf ? (
          <span className="text-slate-400">{PHASE_LABELS[row.phase!].toLowerCase()}</span>
        ) : (
          <span className="text-slate-400">{statusSummaryLabel(row)}</span>
        )}
        {!row.isLeaf && <span className="text-slate-400"> · {row.total.toLocaleString()}</span>}
      </div>
    </button>
  )
}

function dominantLabel(row: FlatRow): string {
  let worst: Phase = 'Ready'
  for (const p of PHASE_VALUES) {
    if ((row.byReadiness[p] ?? 0) > 0 && PHASE_SEVERITY[p] > PHASE_SEVERITY[worst]) worst = p
  }
  return PHASE_LABELS[worst]
}

function statusSummaryLabel(row: FlatRow): string {
  const reconciling = row.byReadiness.Reconciling ?? 0
  if (reconciling > 0) return `${reconciling.toLocaleString()} reconciling`
  return 'all ready'
}

// ReadinessStrip renders per-phase counts as a proportional bar. Shared
// idiom with the rest of the readiness surfaces.
function ReadinessStrip({
  byReadiness,
  total,
}: {
  byReadiness: Partial<Record<Phase, number>>
  total: number
}) {
  if (total === 0) return <div className="h-1.5 rounded-sm bg-slate-100" />
  return (
    <div className="flex h-1.5 rounded-sm overflow-hidden">
      {PHASE_VALUES.map((p) => {
        const n = byReadiness[p] ?? 0
        if (n === 0) return null
        const pct = (n / total) * 100
        return (
          <div
            key={p}
            style={{ width: `${pct}%`, background: PHASE_FILL[p] }}
            title={`${PHASE_LABELS[p].toLowerCase()}: ${n.toLocaleString()} (${pct.toFixed(1)}%)`}
          />
        )
      })}
    </div>
  )
}

// ── chrome ────────────────────────────────────────────────────────────

function CenteredNotice({ title, body }: { title: string; body: string }) {
  return (
    <div className="flex-1 flex items-center justify-center">
      <div className="max-w-sm text-center bg-white rounded-lg border border-slate-200 px-4 py-3 shadow-sm">
        <p className="text-sm text-slate-700 mb-0.5">{title}</p>
        <p className="text-xs text-slate-500">{body}</p>
      </div>
    </div>
  )
}

function Legend() {
  return (
    <div className="px-4 py-2 border-t border-slate-200 bg-white flex gap-3 flex-wrap">
      {PHASE_VALUES.map((p) => (
        <div key={p} className="flex items-center gap-1.5">
          <div className="w-3 h-3 rounded-sm" style={{ background: PHASE_FILL[p] }} />
          <span className="text-xs text-slate-600">{PHASE_LABELS[p].toLowerCase()}</span>
        </div>
      ))}
    </div>
  )
}

// ── owner picker ────────────────────────────────────────────────────────

// OwnerPicker is the Topology tab's OWN single-owner selector — an autocomplete
// over /api/resources/roots with server-side name ILIKE filtering, so it
// works at 100k+ roots without fetching them all (same endpoint the top filter
// bar's owner picker uses). It's self-contained here so the tab never depends on
// the page's top filter. The selected owner is addressed by its (kind, name),
// which the roots list supplies directly — no lazy name resolution needed.
function OwnerPicker({
  value,
  onChange,
}: {
  value: OwnerSel | null
  onChange: (next: OwnerSel | null) => void
}) {
  const [open, setOpen] = useState(false)
  const [q, setQ] = useState('')
  const debouncedQ = useDebounced(q, 200)
  const wrapRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLInputElement>(null)

  const { data, isFetching } = useQuery({
    queryKey: ['roots-search', debouncedQ],
    queryFn: () => api.listRootsPage({ name: debouncedQ || undefined, limit: 20 }),
    enabled: open,
    staleTime: 3_000,
    placeholderData: (prev) => prev,
  })

  useEffect(() => {
    if (open) inputRef.current?.focus()
  }, [open])

  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setOpen(false)
    }
    const onClick = (e: MouseEvent) => {
      if (!wrapRef.current?.contains(e.target as Node)) setOpen(false)
    }
    window.addEventListener('keydown', onKey)
    window.addEventListener('mousedown', onClick)
    return () => {
      window.removeEventListener('keydown', onKey)
      window.removeEventListener('mousedown', onClick)
    }
  }, [open])

  const matches: RootPageItem[] = (data?.rows ?? []).filter(
    (r) => !(r.kind === value?.kind && r.name === value?.name),
  )
  const displayName = value ? value.name : ''

  return (
    <div ref={wrapRef} className="flex items-center gap-1 relative">
      {value && (
        <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded border bg-indigo-50 border-indigo-200 text-indigo-800 font-mono text-xs">
          <span className="truncate max-w-[220px]" title={displayName}>{displayName}</span>
          <button
            onClick={() => onChange(null)}
            className="ml-0.5 opacity-60 hover:opacity-100"
            aria-label={`clear owner ${displayName}`}
          >
            ×
          </button>
        </span>
      )}
      <button
        onClick={() => setOpen((v) => !v)}
        className={`px-2 py-0.5 rounded border border-dashed text-xs transition-colors ${
          open
            ? 'border-blue-400 bg-blue-50 text-blue-700'
            : 'border-slate-300 text-slate-500 hover:border-slate-400 hover:text-slate-700 hover:bg-slate-50'
        }`}
      >
        {value ? 'change' : 'pick owner…'}
      </button>

      {open && (
        <div className="absolute top-full left-0 mt-1 z-30 w-[360px] bg-white border border-slate-200 rounded-lg shadow-lg p-2">
          <input
            ref={inputRef}
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="type to search owners…"
            className="w-full bg-white border border-slate-300 px-2 py-1 rounded text-sm text-slate-800 placeholder-slate-400 outline-none focus:ring-1 focus:ring-blue-500"
          />
          <ul className="mt-1 max-h-72 overflow-y-auto" role="listbox">
            {isFetching && matches.length === 0 && (
              <li className="text-xs text-slate-500 italic px-1 py-1">Searching…</li>
            )}
            {!isFetching && matches.length === 0 && (
              <li className="text-xs text-slate-500 italic px-1 py-1">No matches</li>
            )}
            {matches.map((r) => (
              <li key={`${r.kind}/${r.name}`}>
                <button
                  onClick={() => {
                    onChange({ kind: r.kind, name: r.name })
                    setQ('')
                    setOpen(false)
                  }}
                  className="w-full text-left px-2 py-1 text-sm hover:bg-slate-100 rounded flex items-center gap-2"
                >
                  <span className="text-[10px] font-mono px-1 rounded border bg-indigo-50 text-indigo-800 border-indigo-200">
                    {r.kind}
                  </span>
                  <span className="text-slate-800 truncate">{r.name}</span>
                  <span className="ml-auto text-[10px] text-slate-400 tabular-nums">
                    g{r.generation}
                  </span>
                </button>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  )
}
