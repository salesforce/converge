import { useEffect, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api, type Phase, PHASE_LABELS, PHASE_VALUES } from '../api'
import { useDebounced } from '../hooks'
import type { Filter, FilterDim } from './filters'

// Re-export the filter model types so existing importers of
// '../components/FilterBar' keep working. The types + URL serialization
// helpers live in filters.ts (not here) so this component file only exports
// components — required for React Fast Refresh. Import the URL helpers
// (filtersFromURL / filtersToURL) directly from './filters'.
export type { Filter, FilterDim } from './filters'

const PHASE_OPTIONS: Phase[] = PHASE_VALUES

// splitKindVer parses a "kind/N" pair value into [kind, version]. The version is
// the substring after the LAST slash (kind names never contain '/'); a malformed
// value with no version falls back to ['<value>', '1'] so a chip still renders.
function splitKindVer(pair: string): [string, string] {
  const i = pair.lastIndexOf('/')
  if (i <= 0 || i === pair.length - 1) return [pair, '1']
  return [pair.slice(0, i), pair.slice(i + 1)]
}

// Single "selected" palette for kind/version chips. The chips themselves come
// from the registered-manifest census (GET /api/kinds), so the UI needs no
// per-kind color map — any kind added on the Go side shows up automatically with
// no UI change.
const KIND_BADGE_ON = 'bg-amber-100 text-amber-900 border-amber-300'
const PHASE_BADGE_ON: Record<Phase, string> = {
  Ready: 'bg-emerald-100 text-emerald-900 border-emerald-300',
  Reconciling: 'bg-amber-100 text-amber-900 border-amber-300',
  Degraded: 'bg-orange-100 text-orange-900 border-orange-300',
  Failed: 'bg-red-100 text-red-900 border-red-300',
  Deleting: 'bg-slate-200 text-slate-800 border-slate-300',
  Orphaned: 'bg-purple-100 text-purple-900 border-purple-300',
  Quarantined: 'bg-yellow-100 text-yellow-900 border-yellow-300',
}
const OFF_CHIP =
  'bg-white border-slate-300 text-slate-600 hover:bg-slate-100 hover:text-slate-800'

interface Props {
  filters: Filter[]
  onChange: (next: Filter[]) => void
}

// FilterBar — inline, every dimension always visible.
//
// Layout: one row per dimension (Owner, Kind, Phase, Name, Labels).
// Multi-select dims (kind, phase, owner, owner_kind, label) toggle
// chips; single-value dims (name) take a text input.
export function FilterBar({ filters, onChange }: Props) {
  // Kind/version chips come from the REGISTERED manifests (GET /api/kinds), not
  // from a DISTINCT scan of the resources table — so the filter offers every
  // declared (kind, version) even before a resource of it exists, and shows each
  // version as its own chip (vpc/v1, vpc/v2, account/v1). Cached briefly (kinds
  // change only when an operator applies/deletes a CRD).
  const { data: kindSchemas } = useQuery({
    queryKey: ['kind-schemas'],
    queryFn: () => api.listKindSchemas(),
    staleTime: 30_000,
  })
  // Expand each kind into its declared versions → "kind/N" pair values, always
  // version-qualified (a single-version kind still reads account/1).
  const kindVerPairs = (kindSchemas ?? [])
    .flatMap((k) => (k.kind_versions && k.kind_versions.length ? k.kind_versions : [1]).map((v) => `${k.kind}/${v}`))
    .sort()
  const removeAt = (i: number) => onChange(filters.filter((_, j) => j !== i))
  const replaceFilter = (predicate: (f: Filter) => boolean, next: Filter[]) =>
    onChange([...filters.filter((f) => !predicate(f)), ...next])
  const toggleSimple = (dim: FilterDim, value: string) => {
    const has = filters.some((f) => f.dim === dim && f.value === value)
    if (has) {
      onChange(filters.filter((f) => !(f.dim === dim && f.value === value)))
    } else {
      onChange([...filters, { dim, value }])
    }
  }

  const activeKindVers = filters.filter((f) => f.dim === 'kindver').map((f) => f.value)
  const activePhase = filters.filter((f) => f.dim === 'phase').map((f) => f.value)
  const activeOwners = filters.filter((f) => f.dim === 'owner')
  const nameFilter = filters.find((f) => f.dim === 'name')
  const labelFilters = filters.filter((f) => f.dim === 'label')

  return (
    <div className="px-4 py-2 border-b border-slate-200 bg-white relative">
      <div className="grid grid-cols-[80px_minmax(0,1fr)] items-center gap-x-3 gap-y-2 text-xs">
        <FilterRow label="Owner">
          <OwnerPicker
            active={activeOwners}
            onAdd={(ref, name) =>
              onChange([...filters, { dim: 'owner', value: ref, displayLabel: name }])
            }
            onRemove={(ref) =>
              onChange(filters.filter((f) => !(f.dim === 'owner' && f.value === ref)))
            }
          />
        </FilterRow>

        <FilterRow label="Kind">
          {/* One chip per REGISTERED (kind, version) — always version-qualified
              (vpc/v1, vpc/v2, account/v1). Union with any active chip so a chip
              stays clickable-to-deselect even if its kind/version was just deleted
              and dropped from the census. Value is "kind/N"; the chip label shows
              the friendlier "kind/vN". */}
          {Array.from(new Set([...kindVerPairs, ...activeKindVers]))
            .sort()
            .map((kvPair) => {
              const on = activeKindVers.includes(kvPair)
              const [kName, kVer] = splitKindVer(kvPair)
              return (
                <button
                  key={kvPair}
                  onClick={() => toggleSimple('kindver', kvPair)}
                  className={`px-2 py-0.5 font-mono rounded border transition-colors ${
                    on ? KIND_BADGE_ON : OFF_CHIP
                  }`}
                  title={`Filter to ${kName} at version ${kVer}`}
                >
                  {kName}/v{kVer}
                </button>
              )
            })}
        </FilterRow>

        <FilterRow label="Phase">
          {PHASE_OPTIONS.map((p) => {
            const on = activePhase.includes(p)
            return (
              <button
                key={p}
                onClick={() => toggleSimple('phase', p)}
                className={`px-2 py-0.5 font-mono rounded border transition-colors ${
                  on ? PHASE_BADGE_ON[p] : OFF_CHIP
                }`}
              >
                {PHASE_LABELS[p].toLowerCase()}
              </button>
            )
          })}
        </FilterRow>

        <FilterRow label="Name">
          <NameFilterInput
            value={nameFilter?.value ?? ''}
            onCommit={(v) =>
              replaceFilter(
                (f) => f.dim === 'name',
                v.trim() ? [{ dim: 'name', value: v }] : [],
              )
            }
          />
        </FilterRow>

        <FilterRow label="Labels">
          <LabelInput
            active={labelFilters}
            onAdd={(kv) => onChange([...filters, { dim: 'label', value: kv }])}
            onRemove={(i) =>
              // labelFilters is a filtered view of `filters`, so the
              // element references are identical — match by reference so
              // duplicate label values (e.g. two env:prod chips) remove
              // the exact one clicked, not always the first.
              removeAt(filters.findIndex((f) => f === labelFilters[i]))
            }
          />
        </FilterRow>
      </div>

      {filters.length > 0 && (
        <button
          onClick={() => onChange([])}
          className="absolute top-2 right-4 text-xs text-slate-500 hover:text-slate-800"
        >
          clear all
        </button>
      )}
    </div>
  )
}

function FilterRow({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <>
      <span className="text-slate-500 uppercase text-[10px] font-medium tracking-wider">{label}</span>
      <div className="flex items-center gap-1.5 flex-wrap min-w-0">{children}</div>
    </>
  )
}

// OwnerPicker is the autocomplete-driven root-resource selector.
// Uses /api/resources/roots with server-side name ILIKE filtering
// so it works at 100k+ owners without fetching them all.
function OwnerPicker({
  active,
  onAdd,
  onRemove,
}: {
  active: Filter[]
  // ref is the owner's "kind/name" identity; name is the display label.
  onAdd: (ref: string, name: string) => void
  onRemove: (ref: string) => void
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

  const activeRefs = new Set(active.map((f) => f.value))
  const matches = (data?.rows ?? []).filter((r) => !activeRefs.has(`${r.kind}/${r.name}`))

  return (
    <div ref={wrapRef} className="flex items-center gap-1 flex-wrap relative">
      {active.map((f) => (
        <span
          key={f.value}
          className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded border bg-indigo-50 border-indigo-200 text-indigo-800 font-mono"
        >
          <span>{f.displayLabel ?? f.value.slice(f.value.indexOf('/') + 1)}</span>
          <button
            onClick={() => onRemove(f.value)}
            className="ml-0.5 opacity-60 hover:opacity-100"
            aria-label={`remove owner ${f.displayLabel ?? f.value}`}
          >
            ×
          </button>
        </span>
      ))}
      <button
        onClick={() => setOpen((v) => !v)}
        className={`px-2 py-0.5 rounded border border-dashed transition-colors ${
          open
            ? 'border-blue-400 bg-blue-50 text-blue-700'
            : 'border-slate-300 text-slate-500 hover:border-slate-400 hover:text-slate-700 hover:bg-slate-50'
        }`}
      >
        {active.length === 0 ? 'pick owner…' : '+ add'}
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
                    onAdd(`${r.kind}/${r.name}`, r.name)
                    setQ('')
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

// NameFilterInput debounces the name substring filter. It holds the
// typed text locally so the field stays responsive on every keystroke,
// but only commits to the parent filter (which drives the URL + the
// paginated query) after a short pause — so we don't fire a request mid-
// word. Re-syncs when `value` changes externally (e.g. "clear all").
function NameFilterInput({
  value,
  onCommit,
}: {
  value: string
  onCommit: (v: string) => void
}) {
  const [draft, setDraft] = useState(value)
  const debounced = useDebounced(draft, 350)

  // External change (clear all, URL nav) → reflect it in the field. Done
  // as a render-phase reset keyed on the previous committed value (React's
  // "adjusting state when a prop changes" pattern) rather than an effect,
  // so the new value paints in the same render with no cascading re-render.
  const [lastValue, setLastValue] = useState(value)
  if (value !== lastValue) {
    setLastValue(value)
    setDraft(value)
  }

  // Commit the debounced draft upward — but only when it (a) differs from the
  // committed `value` (skip redundant commits on mount/sync) and (b) still
  // matches what's actually in the field (`draft`). Condition (b) closes a
  // stale-commit hole: if the user types "foo" then triggers an external reset
  // (clear all) before the 350ms elapses, `draft` resets to "" but a timer
  // carrying "foo" is still pending — without the `debounced === draft` guard
  // that timer would commit "foo" back over the cleared state. `value` is in
  // the deps so the guard re-evaluates on an external reset too.
  useEffect(() => {
    if (debounced === draft && debounced !== value) onCommit(debounced)
    // onCommit is recreated each render; intentionally excluded so the effect
    // fires on draft/debounced/value changes, not on every parent render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [debounced, draft, value])

  return (
    <input
      value={draft}
      onChange={(e) => setDraft(e.target.value)}
      placeholder="substring (case-insensitive)"
      className="bg-white border border-slate-300 px-2 py-1 rounded text-slate-800 placeholder-slate-400 outline-none focus:ring-1 focus:ring-blue-500 min-w-[280px]"
    />
  )
}

function LabelInput({
  active,
  onAdd,
  onRemove,
}: {
  active: Filter[]
  onAdd: (kv: string) => void
  onRemove: (i: number) => void
}) {
  const [draft, setDraft] = useState('')
  const submit = () => {
    const v = draft.trim()
    if (!v) return
    const i = v.indexOf('=')
    if (i <= 0 || i === v.length - 1) return
    onAdd(v)
    setDraft('')
  }
  return (
    <>
      {active.map((f, i) => {
        const sep = f.value.indexOf('=')
        const k = sep > 0 ? f.value.slice(0, sep) : f.value
        const v = sep > 0 ? f.value.slice(sep + 1) : ''
        return (
          <span
            key={`${f.value}:${i}`}
            className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded border bg-blue-50 border-blue-200 text-blue-800 font-mono"
          >
            <span className="text-blue-600">{k}</span>
            <span className="text-blue-400">=</span>
            <span>{v}</span>
            <button
              onClick={() => onRemove(i)}
              className="ml-0.5 opacity-60 hover:opacity-100"
              aria-label={`remove label ${k}=${v}`}
            >
              ×
            </button>
          </span>
        )
      })}
      <input
        value={draft}
        onChange={(e) => setDraft(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === 'Enter') {
            e.preventDefault()
            submit()
          }
        }}
        placeholder="key=value (Enter)"
        className="bg-white border border-slate-300 px-2 py-1 rounded text-slate-800 placeholder-slate-400 outline-none focus:ring-1 focus:ring-blue-500 font-mono min-w-[180px]"
      />
    </>
  )
}

