import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { keepPreviousData, useQuery } from '@tanstack/react-query'
import { useVirtualizer } from '@tanstack/react-virtual'
import { useNavigate, useParams, useSearchParams } from 'react-router-dom'
import {
  api,
  type Phase,
  type Resource,
  type ResourceInfo,
  PHASE_BG,
  PHASE_LABELS,
} from '../api'
import { FilterBar } from '../components/FilterBar'
import { filtersFromURL, filtersToURL, type Filter } from '../components/filters'
import { ResourcePanel } from '../components/ResourcePanel'
import { TopologyView } from '../components/TopologyView'
import { SubgraphView } from '../components/SubgraphView'
import { SummaryView } from '../components/SummaryView'
import { CreateResourceModal } from '../components/CreateResourceModal'
import { formatDateTime } from '../utils'

type View = 'list' | 'summary' | 'topology' | 'subgraph'
const VALID_VIEWS: View[] = ['list', 'summary', 'topology', 'subgraph']

// ResourcesPage is the unified surface — filters at the top, a view-mode
// switch (list / summary / graph / subgraph), and an optional detail
// panel on the right when a resource is open.
//
// URL: /resources/:view?owner=…&kind=…&readiness=…&label=…&name=…
// Resource detail: /resources/:view/r/:kind/:name?<filters>
export function ResourcesPage() {
  const navigate = useNavigate()
  const { view: viewParam, kind, name } = useParams()
  const [searchParams, setSearchParams] = useSearchParams()

  // A resource is open when the route carries both a kind and a name. Its
  // "kind/name" ref is the single opaque token passed through the view
  // callbacks (matching the owner-filter + SubgraphNode.ref encoding). The
  // internal uuid is never used for addressing or routing.
  const resourceRef = kind && name ? `${kind}/${name}` : undefined

  const view: View = (VALID_VIEWS as string[]).includes(viewParam ?? '')
    ? (viewParam as View)
    : 'summary'

  // Filter state lives in URL; derive chips for FilterBar from it. Owner
  // chips carry the owner's "kind/name" ref as their value; the name is
  // right there in the ref, so no lazy id→name resolve is needed.
  const filters: Filter[] = useMemo(
    () => filtersFromURL(searchParams),
    [searchParams],
  )
  const setFilters = (next: Filter[]) => {
    const params = filtersToURL(next)
    setSearchParams(params, { replace: false })
  }

  // Compose effective filter scope for the API. Owner refs are "kind/name"
  // strings appended as repeated ?owner= params by the API layer.
  const ownerRefs = useMemo(
    () => filters.filter((f) => f.dim === 'owner').map((f) => f.value),
    [filters],
  )
  // The kind/version chips are (kind, version) PAIRS ("kind/N"), passed straight
  // through to the API as repeatable ?kv=. Each is an exact (kind, kind_version)
  // match, so filtering to vpc/v1 AND account/v2 works — the single-version model
  // is gone.
  const kvFilter = useMemo(
    () => filters.filter((f) => f.dim === 'kindver').map((f) => f.value),
    [filters],
  )
  const phaseFilter = useMemo(
    () => filters.filter((f) => f.dim === 'phase').map((f) => f.value as Phase),
    [filters],
  )
  const nameFilter = filters.find((f) => f.dim === 'name')?.value
  const labelFilters = useMemo(() => {
    const out: Record<string, string> = {}
    for (const f of filters) {
      if (f.dim !== 'label') continue
      const i = f.value.indexOf('=')
      if (i > 0 && i < f.value.length - 1) out[f.value.slice(0, i)] = f.value.slice(i + 1)
    }
    return out
  }, [filters])
  const ownerKindFilters = useMemo(
    () => filters.filter((f) => f.dim === 'owner_kind').map((f) => f.value),
    [filters],
  )

  // Validate view: subgraph needs a resource; topology needs N=1 owner.
  useEffect(() => {
    if (view === 'subgraph' && !resourceRef) {
      navigate(`/resources/list${qsString(searchParams)}`, { replace: true })
    }
  }, [view, resourceRef, navigate, searchParams])

  // Build the /r/:kind/:name detail tail, encoding each segment.
  const detailTail = (k: string, n: string) =>
    `/r/${encodeURIComponent(k)}/${encodeURIComponent(n)}`

  const goView = (next: View) => {
    const tail = kind && name ? detailTail(kind, name) : ''
    navigate(`/resources/${next}${tail}${qsString(searchParams)}`)
  }
  const openResource = (k: string, n: string) => {
    // Guard against a stray falsy kind/name (never build /r/undefined, which
    // would fetch the SPA's index.html and blow up JSON parsing).
    if (!k || !n) return
    navigate(`/resources/${view}${detailTail(k, n)}${qsString(searchParams)}`)
  }
  const closeResource = () => {
    const next: View = view === 'subgraph' ? 'list' : view
    navigate(`/resources/${next}${qsString(searchParams)}`)
  }
  const showSubgraph = (k: string, n: string) => {
    if (!k || !n) return
    navigate(`/resources/subgraph${detailTail(k, n)}${qsString(searchParams)}`)
  }
  const filterByLabel = (k: string, v: string) => {
    // Synthesized owner_kind labels become a typed owner_kind filter chip;
    // anything else passes through as a raw label chip.
    if (k === 'owner_kind') {
      const exists = filters.find((f) => f.dim === 'owner_kind' && f.value === v)
      if (!exists) setFilters([...filters, { dim: 'owner_kind', value: v }])
      return
    }
    const value = `${k}=${v}`
    const exists = filters.find((f) => f.dim === 'label' && f.value === value)
    if (!exists) setFilters([...filters, { dim: 'label', value }])
  }

  const [creating, setCreating] = useState(false)

  return (
    <main className="flex-1 flex flex-col overflow-hidden">
      <Breadcrumbs filters={filters} kind={kind} name={name} onNew={() => setCreating(true)} />
      <FilterBar filters={filters} onChange={setFilters} />
      <ViewSwitch
        current={view}
        onChange={goView}
        hasResource={!!resourceRef}
      />

      <div className="flex-1 flex overflow-hidden">
        <div className="flex-1 flex flex-col overflow-hidden">
          {view === 'list' && (
            <ResourceList
              ownerRefs={ownerRefs}
              kv={kvFilter}
              phases={phaseFilter}
              name={nameFilter}
              labels={labelFilters}
              ownerKinds={ownerKindFilters}
              onResourceClick={openResource}
              hasFilters={filters.length > 0}
              onNew={() => setCreating(true)}
            />
          )}
          {view === 'summary' && <SummaryView ownerRefs={ownerRefs} />}
          {view === 'topology' && (
            // Topology owns its OWN owner selection (an in-tab picker), so it's
            // always usable regardless of the top filter. Seed it from the top
            // filter only when that names exactly one owner — a convenience.
            <TopologyView
              initialOwnerRef={ownerRefs.length === 1 ? ownerRefs[0] : undefined}
              onNodeClick={openResource}
            />
          )}
          {view === 'subgraph' && kind && name && (
            <SubgraphView kind={kind} name={name} onNodeClick={openResource} />
          )}
        </div>
        {kind && name && (
          <ResizablePanel>
            <ResourcePanel
              kind={kind}
              name={name}
              onClose={closeResource}
              onShowSubgraph={() => showSubgraph(kind, name)}
              onFilterByLabel={filterByLabel}
              onOpenResource={openResource}
            />
          </ResizablePanel>
        )}
      </div>

      <CreateResourceModal
        open={creating}
        onClose={() => setCreating(false)}
        onApplied={(r) => {
          setCreating(false)
          // Drop the user straight into the applied resource's panel.
          navigate(`/resources/list${detailTail(r.kind, r.name)}`)
        }}
      />
    </main>
  )
}

const PANEL_WIDTH_KEY = 'converge:panel-width'
const PANEL_MIN = 280
const PANEL_MAX = 800
const PANEL_DEFAULT = 384

function ResizablePanel({ children }: { children: React.ReactNode }) {
  const [width, setWidth] = useState(() => {
    const stored = localStorage.getItem(PANEL_WIDTH_KEY)
    const n = stored ? parseInt(stored, 10) : NaN
    return Number.isFinite(n) && n >= PANEL_MIN && n <= PANEL_MAX ? n : PANEL_DEFAULT
  })
  const dragging = useRef(false)
  const startX = useRef(0)
  const startW = useRef(0)

  const onMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault()
    dragging.current = true
    startX.current = e.clientX
    startW.current = width
    document.body.style.cursor = 'col-resize'
    document.body.style.userSelect = 'none'

    const onMove = (ev: MouseEvent) => {
      if (!dragging.current) return
      const delta = startX.current - ev.clientX
      const next = Math.min(PANEL_MAX, Math.max(PANEL_MIN, startW.current + delta))
      setWidth(next)
    }
    const onUp = () => {
      dragging.current = false
      document.body.style.cursor = ''
      document.body.style.userSelect = ''
      document.removeEventListener('mousemove', onMove)
      document.removeEventListener('mouseup', onUp)
      setWidth((w) => {
        localStorage.setItem(PANEL_WIDTH_KEY, String(w))
        return w
      })
    }
    document.addEventListener('mousemove', onMove)
    document.addEventListener('mouseup', onUp)
  }, [width])

  return (
    <aside className="relative border-l border-slate-200 bg-white overflow-y-auto shrink-0" style={{ width }}>
      <div
        onMouseDown={onMouseDown}
        className="absolute left-0 top-0 bottom-0 w-1.5 cursor-col-resize hover:bg-blue-400/30 active:bg-blue-400/50 z-10"
      />
      {children}
    </aside>
  )
}

function qsString(params: URLSearchParams): string {
  const s = params.toString()
  return s ? `?${s}` : ''
}

// Breadcrumbs render the Resources page header: the uniform all-caps "Resources"
// title (matching Cluster / Kinds / Provider configs / Reactor bindings), the
// active filter / selected-resource context as a small breadcrumb trail beneath
// it, and the "Apply manifest" action. The title is a link home so it doubles as
// breadcrumb root.
function Breadcrumbs({
  filters,
  kind,
  name,
  onNew,
}: {
  filters: Filter[]
  kind: string | undefined
  name: string | undefined
  onNew: () => void
}) {
  const navigate = useNavigate()
  // Context segments shown under the title: the active-filter count and the
  // drilled-into resource. Empty on the bare list, so the header reads as just
  // the title — same as the other pages.
  const trail: string[] = []
  if (filters.length > 0) {
    trail.push(`${filters.length} filter${filters.length === 1 ? '' : 's'}`)
  }
  if (kind && name) {
    trail.push(`${kind}/${name}`)
  }
  return (
    <div className="px-4 pt-4 pb-2 border-b border-slate-100 flex items-end justify-between">
      <div>
        <h2 className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">
          <button onClick={() => navigate('/')} className="hover:text-slate-800 uppercase tracking-wider">
            Resources
          </button>
        </h2>
        <div className="text-sm text-slate-600 tabular-nums flex items-center gap-1">
          {trail.length === 0 ? (
            'All resources'
          ) : (
            trail.map((seg, i) => (
              <span key={i} className="flex items-center gap-1">
                {i > 0 && <span className="text-slate-300">/</span>}
                <span className="text-slate-700">{seg}</span>
              </span>
            ))
          )}
        </div>
      </div>
      <button
        onClick={onNew}
        className="ml-auto shrink-0 text-sm px-3 py-1.5 rounded bg-blue-600 hover:bg-blue-500 text-white"
        title="Apply a manifest to create or update a root resource"
      >
        Apply manifest
      </button>
    </div>
  )
}

function ViewSwitch({
  current,
  onChange,
  hasResource,
}: {
  current: View
  onChange: (v: View) => void
  hasResource: boolean
}) {
  const tabs: { id: View; label: string; visible: boolean; hint?: string; disabled?: boolean }[] = [
    { id: 'summary', label: 'Summary', visible: true },
    { id: 'list', label: 'List', visible: true },
    {
      id: 'topology',
      label: 'Topology',
      visible: true,
      // Always enabled — the Topology tab has its own in-tab owner picker, so
      // it no longer depends on a single owner being set in the top filter.
    },
    { id: 'subgraph', label: 'Subgraph', visible: hasResource },
  ]
  return (
    <div role="tablist" className="flex gap-1 px-3 pt-2 border-b border-slate-200 bg-white">
      {tabs
        .filter((t) => t.visible)
        .map((t) => {
          const active = current === t.id
          return (
            <button
              key={t.id}
              role="tab"
              aria-selected={active}
              disabled={t.disabled}
              onClick={() => onChange(t.id)}
              title={t.hint}
              className={`px-3 py-1.5 text-sm rounded-t border-b-2 transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 disabled:opacity-40 disabled:cursor-not-allowed ${
                active
                  ? 'border-blue-500 text-slate-900 bg-slate-100'
                  : 'border-transparent text-slate-500 hover:text-slate-800'
              }`}
            >
              {t.label}
            </button>
          )
        })}
    </div>
  )
}

// ResourceList is the virtualized table. It always uses the multi
// endpoint (which gracefully handles 0/1/N owner scopes) so we don't
// branch on selection size up here.
function ResourceList({
  ownerRefs,
  kv,
  phases,
  name,
  labels,
  ownerKinds,
  onResourceClick,
  hasFilters,
  onNew,
}: {
  ownerRefs: string[]
  // kv = the (kind, version) pair filter values ("kind/N"), passed through as
  // repeatable ?kv=.
  kv: string[]
  phases: Phase[]
  name?: string
  labels: Record<string, string>
  ownerKinds: string[]
  onResourceClick: (kind: string, name: string) => void
  // hasFilters distinguishes the two zero-row cases: with filters active it's
  // "nothing matched"; with none it's a genuinely empty cluster (first run), so
  // we show a getting-started prompt instead. onNew opens the Apply modal.
  hasFilters: boolean
  onNew: () => void
}) {
  // owner_kind via labels: the API recognizes the synth owner_kind label
  // key and routes it to a native filter.
  const labelsForAPI = useMemo(() => {
    const out: Record<string, string> = { ...labels }
    if (ownerKinds.length === 1) out['owner_kind'] = ownerKinds[0]
    // Multiple owner_kind chips would require ANY-match — not currently
    // wired. Skip; they'll be a no-op.
    return out
  }, [labels, ownerKinds])

  // Token (cursor) pagination, AWS-console style: one page in memory at a
  // time, navigated by Next/Prev. This is correct by construction — there's
  // no accumulating page array to dedup and no periodic all-pages refetch to
  // drift, which is what made the old infinite-scroll list show stale
  // duplicates (and a count that grew when a filter was REMOVED). The server
  // does ALL filtering (incl. the phase union) and returns an opaque
  // next-cursor; '' means last page.
  const isMulti = ownerRefs.length >= 2 || ownerRefs.length === 0

  // Filter identity — when any of these change we're looking at a different
  // result set, so reset to page 1 (clear the back-stack + cursor).
  const ownersKey = ownerRefs.slice().sort().join(',')
  const kvKey = kv.slice().sort().join(',')
  const phaseKey = phases.slice().sort().join(',')
  const labelsKey = JSON.stringify(labelsForAPI)
  const filterKey = `${ownersKey}|${kvKey}|${phaseKey}|${name ?? ''}|${labelsKey}`

  const FIRST = ''
  const [pageSize, setPageSize] = useState(50)
  // stack = cursors of the PRECEDING pages (page 1 has an empty stack); the
  // current page's cursor is `cursor` (a single opaque token). Prev pops,
  // Next pushes.
  const [stack, setStack] = useState<string[]>([])
  const [cursor, setCursor] = useState<string>(FIRST)

  // Reset to page 1 whenever the filter set or page size changes. Render-phase
  // reset keyed on the previous value (React's "adjust state on prop change"
  // pattern) so the new query fires in the same render — no extra round-trip.
  const [lastReset, setLastReset] = useState(`${filterKey}#${pageSize}`)
  const resetKey = `${filterKey}#${pageSize}`
  if (resetKey !== lastReset) {
    setLastReset(resetKey)
    setStack([])
    setCursor(FIRST)
  }

  const query = useQuery({
    queryKey: ['resources-page', filterKey, pageSize, cursor],
    queryFn: () =>
      api.listMultiOwnerResourcesPage(ownerRefs, {
        kv: kv.length > 0 ? kv : undefined,
        phases: phases.length > 0 ? phases : undefined,
        name: name,
        labels: Object.keys(labelsForAPI).length > 0 ? labelsForAPI : undefined,
        limit: pageSize,
        cursor: cursor,
      }),
    // Re-fetch ONLY the current page on an interval — safe now that a single
    // page is in memory (no cross-page drift). Keep the prior page visible
    // while the next/prev page loads so navigation doesn't flash empty.
    refetchInterval: 10000,
    refetchIntervalInBackground: false,
    placeholderData: keepPreviousData,
  })

  // Stable reference per fetch — `?? []` would mint a new empty array each
  // render and churn the row memo/virtualizer below.
  const rows: Resource[] = useMemo(() => query.data?.rows ?? [], [query.data])
  const hasNext = !!query.data?.next_cursor
  const pageNumber = stack.length + 1

  const goNext = () => {
    if (!hasNext || !query.data) return
    setStack((s) => [...s, cursor])
    setCursor(query.data.next_cursor)
  }
  const goPrev = () => {
    if (stack.length === 0) return
    setStack((s) => {
      const next = s.slice(0, -1)
      setCursor(s[s.length - 1])
      return next
    })
  }

  // Owners ride along on each page response (page.owners), so the per-group
  // header gets the owner NAME. Keyed by the owner's "kind/name" ref, which
  // is what each row's owner_kind/owner_name compose into.
  const ownersByRef = useMemo(() => {
    const out = new Map<string, ResourceInfo>()
    for (const o of query.data?.owners ?? []) out.set(`${o.kind}/${o.name}`, o)
    return out
  }, [query.data])

  // Auto-group by owner when the scope spans multiple owners (or none, where
  // every row could be from a different one). With exactly one owner the
  // grouping is just noise.
  const showOwnerColumn = isMulti
  const grouped = useMemo(() => {
    if (!showOwnerColumn) return null
    const map = new Map<string, Resource[]>()
    for (const r of rows) {
      // Root rows (no owner) group under the empty key.
      const ownerKey = r.owner_kind && r.owner_name ? `${r.owner_kind}/${r.owner_name}` : ''
      const arr = map.get(ownerKey) ?? []
      arr.push(r)
      map.set(ownerKey, arr)
    }
    return [...map.entries()]
  }, [rows, showOwnerColumn])

  const [collapsed, setCollapsed] = useState<Set<string>>(new Set())
  const toggleCollapsed = (id: string) => {
    setCollapsed((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  // Flatten grouped data into a single virtualized list, with sentinel
  // group-header rows interleaved.
  type FlatRow =
    | { type: 'header'; ownerRef: string; count: number }
    | { type: 'data'; r: Resource }
  const flat: FlatRow[] = useMemo(() => {
    if (!grouped) return rows.map((r) => ({ type: 'data' as const, r }))
    const out: FlatRow[] = []
    for (const [oref, orows] of grouped) {
      out.push({ type: 'header', ownerRef: oref, count: orows.length })
      if (!collapsed.has(oref)) {
        for (const r of orows) out.push({ type: 'data', r })
      }
    }
    return out
  }, [grouped, rows, collapsed])

  const parentRef = useRef<HTMLDivElement>(null)
  const virtualizer = useVirtualizer({
    count: flat.length,
    getScrollElement: () => parentRef.current,
    estimateSize: (i) => (flat[i]?.type === 'header' ? 32 : 36),
    overscan: 12,
  })

  // Jump back to the top of the scroll area whenever the page changes, so a
  // Next/Prev lands the user at row 1 of the new page (not mid-scroll).
  useEffect(() => {
    parentRef.current?.scrollTo({ top: 0 })
  }, [cursor, pageSize, filterKey])

  const items = virtualizer.getVirtualItems()

  // Kind column is min 200 / max 260px: wide enough for the long PaC kind names
  // (e.g. pac-codebuild-aws-enable_ebs_encryption, ~40 mono chars) without clipping,
  // capped so it never starves the Name column. The kind cell truncates with a
  // title tooltip for anything still longer; Name takes the remaining 1fr.
  const cols = 'grid-cols-[minmax(200px,260px)_1fr_120px_100px_140px]'

  return (
    <div className="flex flex-col flex-1 overflow-hidden">
      <div className="px-4 py-1 text-xs text-slate-500 border-b border-slate-200 bg-slate-50/95 flex items-center gap-2">
        <span className="tabular-nums">
          {rows.length.toLocaleString()} resource{rows.length === 1 ? '' : 's'}
          {pageNumber > 1 || hasNext ? ` · page ${pageNumber}` : ''}
        </span>
        {showOwnerColumn && ownerRefs.length === 0 && (
          <span className="text-slate-400">across all owners</span>
        )}
        {showOwnerColumn && ownerRefs.length > 0 && (
          <span className="text-slate-400">across {ownerRefs.length} owners</span>
        )}
        {query.isFetching && <span className="text-slate-400">updating…</span>}
      </div>

      <div
        className={`grid ${cols} gap-2 px-4 py-1.5 text-xs text-slate-500 border-b border-slate-200 sticky top-0 bg-slate-50/95 backdrop-blur`}
      >
        <span>Kind</span>
        <span>Name</span>
        <span>State</span>
        <span className="text-right" title="synced generation / spec generation">Synced</span>
        <span>Last update</span>
      </div>

      <div ref={parentRef} className="flex-1 overflow-y-auto bg-white">
        {query.isError && (
          <div className="p-4">
            <div className="max-w-lg p-3 bg-red-50 border border-red-200 rounded text-xs text-red-800">
              <div className="font-semibold mb-1">Failed to load resources</div>
              <div className="font-mono">{(query.error as Error)?.message}</div>
            </div>
          </div>
        )}
        {!query.isError && rows.length === 0 && !query.isFetching && (
          hasFilters ? (
            <div className="p-8 text-center text-sm text-slate-500">
              No resources match these filters.
            </div>
          ) : (
            // No filters and no rows ⇒ a genuinely empty cluster (first run).
            // Point the operator at the first step rather than showing a bare
            // "no matches" that reads like a filter mistake.
            <div className="p-10 text-center max-w-md mx-auto">
              <div className="text-base font-semibold text-slate-800 mb-1">No resources yet</div>
              <p className="text-sm text-slate-500 mb-4">
                Apply a resource manifest ({'{'} kind, name, kind_version, spec {'}'}) to declare
                desired state — the engine reconciles the rest. A kind must have a CRD applied
                first (see Kinds).
              </p>
              <button
                onClick={onNew}
                className="text-sm px-3 py-1.5 rounded bg-blue-600 hover:bg-blue-500 text-white"
              >
                Apply your first resource
              </button>
            </div>
          )
        )}
        <div style={{ height: virtualizer.getTotalSize(), position: 'relative' }}>
          {items.map((vrow) => {
            const item = flat[vrow.index]
            const baseStyle = {
              position: 'absolute' as const,
              top: vrow.start,
              left: 0,
              right: 0,
              height: vrow.size,
            }
            if (item.type === 'header') {
              const o = ownersByRef.get(item.ownerRef)
              const isCollapsed = collapsed.has(item.ownerRef)
              // Group label by OWNER NAME (resolved from the page's echoed
              // owners). Rows whose owner is a root itself (no owner) group
              // under the empty key — label that "Roots". An owner not yet in
              // the map (page still loading) falls back to the name portion of
              // its "kind/name" ref so the header isn't blank.
              const isRootGroup = item.ownerRef === ''
              const refName = item.ownerRef.slice(item.ownerRef.indexOf('/') + 1)
              return (
                <div
                  key={`h-${item.ownerRef}`}
                  style={baseStyle}
                  className="flex items-center gap-2 px-4 bg-slate-100 border-b border-slate-200 cursor-pointer hover:bg-slate-200/70"
                  onClick={() => toggleCollapsed(item.ownerRef)}
                >
                  <span className="text-xs text-slate-500">{isCollapsed ? '▶' : '▼'}</span>
                  {o && (
                    <span className="text-[10px] font-mono px-1.5 py-0.5 rounded border bg-indigo-50 text-indigo-800 border-indigo-200">
                      {o.kind}
                    </span>
                  )}
                  <span className="text-sm font-medium text-slate-800 truncate">
                    {isRootGroup ? 'Roots (no owner)' : o ? o.name : refName}
                  </span>
                  <span className="text-xs text-slate-500 tabular-nums">
                    {item.count} resource{item.count === 1 ? '' : 's'}
                  </span>
                </div>
              )
            }
            const r = item.r
            const phase = r.phase
            return (
              <div
                key={`${r.kind}/${r.name}`}
                onClick={() => onResourceClick(r.kind, r.name)}
                className={`grid ${cols} gap-2 px-4 items-center text-sm cursor-pointer hover:bg-slate-50 border-b border-slate-100`}
                style={baseStyle}
              >
                <span className="font-mono text-xs text-slate-600 truncate" title={r.kind_version && r.kind_version > 1 ? `${r.kind}/v${r.kind_version}` : r.kind}>
                  {r.kind}
                  {/* Show the web-API version suffix only for v2+ (v1 = the tidy common case). */}
                  {r.kind_version && r.kind_version > 1 ? <span className="text-slate-400">/v{r.kind_version}</span> : null}
                </span>
                <span className="font-mono text-xs text-slate-800 truncate" title={r.name}>{r.name}</span>
                <span>
                  <span
                    className={`inline-block px-1.5 py-0.5 rounded text-[11px] text-white ${PHASE_BG[phase]}`}
                  >
                    {PHASE_LABELS[phase].toLowerCase()}
                  </span>
                </span>
                <span className="text-right tabular-nums text-xs text-slate-700">
                  {r.synced_gen}/{r.generation}
                </span>
                <span className="text-xs text-slate-500 truncate">
                  {r.updated_at ? formatDateTime(r.updated_at) : ''}
                </span>
              </div>
            )
          })}
        </div>
      </div>

      {/* Pager — AWS-console style: page size + Prev/Next on opaque cursors.
          No global total (the server runs no COUNT(*) on this hot path), so
          there's no "of N"; Next is disabled on the last page. */}
      <div className="flex items-center gap-3 px-4 py-1.5 border-t border-slate-200 bg-slate-50/95 text-xs text-slate-600">
        <label className="flex items-center gap-1.5">
          <span className="text-slate-500">Rows</span>
          <select
            value={pageSize}
            onChange={(e) => setPageSize(Number(e.target.value))}
            className="bg-white border border-slate-300 rounded px-1.5 py-0.5 text-slate-700 outline-none focus:ring-1 focus:ring-blue-500"
          >
            {[25, 50, 100, 200].map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
        </label>
        <button
          onClick={() => query.refetch()}
          disabled={query.isFetching}
          className="ml-auto px-2 py-0.5 rounded border border-slate-300 bg-white text-slate-700 hover:bg-slate-100 disabled:opacity-40 disabled:cursor-not-allowed"
          title="Refetch this page now"
        >
          ↻ Refresh
        </button>
        <span className="tabular-nums text-slate-500">page {pageNumber}</span>
        <button
          onClick={goPrev}
          disabled={pageNumber === 1 || query.isFetching}
          className="px-2 py-0.5 rounded border border-slate-300 bg-white text-slate-700 hover:bg-slate-100 disabled:opacity-40 disabled:cursor-not-allowed"
        >
          ‹ Prev
        </button>
        <button
          onClick={goNext}
          disabled={!hasNext || query.isFetching}
          className="px-2 py-0.5 rounded border border-slate-300 bg-white text-slate-700 hover:bg-slate-100 disabled:opacity-40 disabled:cursor-not-allowed"
        >
          Next ›
        </button>
      </div>
    </div>
  )
}
