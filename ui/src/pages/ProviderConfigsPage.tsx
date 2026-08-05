import { useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { keepPreviousData, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  api,
  retryReadAfterWrite,
  retryDelayReadAfterWrite,
  type ListProviderConfigsQuery,
  type ProviderConfig,
} from '../api'
import { formatDateTime } from '../utils'
import { ApplyConfigModal } from '../components/ApplyConfigModal'

// ProviderConfigsPage is the admin view of the runtime-editable providerconfigs
// table (Crossplane's ProviderConfig, adapted). It lists every config with
// search filters (kind, name, default/custom) and a paginated table whose rows
// expand to the full document: kind, owner (if a composer emitted it), spec
// JSON, is_default, created/updated. The /providerconfigs/:name route deep-links
// one config (the link on a resource's detail panel lands here).
const PAGE_SIZE = 50

type DefaultFilter = 'all' | 'default' | 'custom'

export function ProviderConfigsPage() {
  const { name: routeName } = useParams()
  const navigate = useNavigate()

  const [kind, setKind] = useState('')
  const [nameFilter, setNameFilter] = useState('')
  const [def, setDef] = useState<DefaultFilter>('all')
  // KindVersion (web-API version) filter: '' = any; else an exact vN match.
  const [kind_version, setMajor] = useState('')
  const [offset, setOffset] = useState(0)
  // "Apply config" — create-or-update a config from a pasted/loaded manifest.
  const [creating, setCreating] = useState(false)

  // Any filter change resets paging to the first page — otherwise a narrower
  // result set could leave you stranded on an empty trailing page.
  const resetTo = <T,>(setter: (v: T) => void) => (v: T) => {
    setter(v)
    setOffset(0)
  }

  const query: ListProviderConfigsQuery = {
    kind: kind || undefined,
    name: nameFilter || undefined,
    is_default: def === 'all' ? undefined : def === 'default',
    // 0/empty = any; the client omits the param unless a real kind_version is set.
    kind_version: kind_version ? Number(kind_version) : undefined,
    limit: PAGE_SIZE,
    offset,
  }

  const { data, isPending, isPlaceholderData, isError, error } = useQuery({
    queryKey: ['provider-configs', query],
    queryFn: () => api.listProviderConfigs(query),
    refetchInterval: 5000,
    refetchIntervalInBackground: false,
    // keepPreviousData holds the prior page so the 5s poll doesn't flash a
    // loading skeleton. On a FILTER change the old rows are stale until the
    // new ones land — `isPlaceholderData` flags that so the header can show
    // "updating…" instead of silently presenting the old filter's results.
    placeholderData: keepPreviousData,
  })

  // Kind suggestions for the filter come from the REGISTERED manifests, so a kind
  // is offered as soon as its CRD is applied — even before it has any resource or
  // provider config yet.
  const { data: kindSchemas } = useQuery({
    queryKey: ['kind-schemas'],
    queryFn: () => api.listKindSchemas(),
    staleTime: 30_000,
  })
  const kinds = (kindSchemas ?? []).map((k) => k.kind)

  const rows = data?.rows ?? []
  const total = data?.total ?? 0
  const start = total === 0 ? 0 : offset + 1
  const end = Math.min(offset + PAGE_SIZE, total)

  return (
    <main className="flex-1 flex flex-col overflow-hidden">
      <div className="flex-1 overflow-y-auto p-6 space-y-4">
        <section className="flex items-start justify-between gap-3">
          <div>
            <h2 className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">
              Provider configs
            </h2>
            <div className="text-sm text-slate-600 tabular-nums">
              {isPending ? 'Loading…' : `${total} config${total === 1 ? '' : 's'}`}
              {isPlaceholderData && !isPending && (
                <span className="ml-2 text-xs text-slate-400">updating…</span>
              )}
            </div>
          </div>
          <button
            onClick={() => setCreating(true)}
            className="shrink-0 text-sm px-3 py-1.5 rounded bg-blue-600 hover:bg-blue-500 text-white"
            title="Apply a manifest to create or update a provider config"
          >
            Apply config
          </button>
        </section>

        {/* Deep-linked single config (from a resource's detail panel). Fetched
            individually so it shows regardless of the current page/filters. */}
        {routeName && <ConfigDetailCard name={routeName} />}

        {/* Filters: kind (exact, with suggestions), name (substring), and the
            default/custom tri-state — the three the server filters on. */}
        <div className="flex flex-wrap items-end gap-3">
          <Filter label="Kind">
            <input
              list="provider-config-kinds"
              value={kind}
              onChange={(e) => resetTo(setKind)(e.target.value)}
              placeholder="all kinds"
              className="w-44 px-2 py-1 text-sm border border-slate-300 rounded focus:outline-none focus:ring-1 focus:ring-blue-400"
            />
            <datalist id="provider-config-kinds">
              {(kinds ?? []).map((k) => (
                <option key={k} value={k} />
              ))}
            </datalist>
          </Filter>
          <Filter label="Name">
            <input
              value={nameFilter}
              onChange={(e) => resetTo(setNameFilter)(e.target.value)}
              placeholder="substring…"
              className="w-52 px-2 py-1 text-sm border border-slate-300 rounded focus:outline-none focus:ring-1 focus:ring-blue-400"
            />
          </Filter>
          <Filter label="Role">
            <select
              value={def}
              onChange={(e) => resetTo(setDef)(e.target.value as DefaultFilter)}
              className="px-2 py-1 text-sm border border-slate-300 rounded bg-white focus:outline-none focus:ring-1 focus:ring-blue-400"
            >
              <option value="all">All</option>
              <option value="default">Default only</option>
              <option value="custom">Custom only</option>
            </select>
          </Filter>
          {/* KindVersion (web-API version) — a positive integer; blank = any version. */}
          <Filter label="Version">
            <input
              type="number"
              min={1}
              value={kind_version}
              onChange={(e) => resetTo(setMajor)(e.target.value)}
              placeholder="any"
              className="w-20 px-2 py-1 text-sm border border-slate-300 rounded focus:outline-none focus:ring-1 focus:ring-blue-400"
            />
          </Filter>
          {(kind || nameFilter || def !== 'all' || kind_version) && (
            <button
              onClick={() => {
                setKind('')
                setNameFilter('')
                setDef('all')
                setMajor('')
                setOffset(0)
              }}
              className="px-2 py-1 text-xs text-slate-600 hover:text-slate-900 underline"
            >
              Clear
            </button>
          )}
        </div>

        {isError ? (
          <div className="max-w-lg p-4 bg-red-50 border border-red-200 rounded text-sm text-red-800">
            <div className="font-semibold mb-1">Failed to load provider configs</div>
            <div className="text-xs font-mono">{(error as Error)?.message}</div>
          </div>
        ) : (
          <div className="bg-white border border-slate-200 rounded overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-slate-500 bg-slate-50 border-b border-slate-200">
                  <th className="font-normal py-1.5 pl-3 pr-2 w-6"></th>
                  <th className="font-normal py-1.5 px-2">Name</th>
                  <th className="font-normal py-1.5 px-2">Kind</th>
                  <th className="font-normal py-1.5 px-2">Version</th>
                  <th className="font-normal py-1.5 px-2">Role</th>
                  <th className="font-normal py-1.5 px-2">Owner</th>
                  <th className="font-normal py-1.5 px-2">Updated</th>
                </tr>
              </thead>
              <tbody>
                {rows.length === 0 && !isPending && (
                  <tr>
                    <td colSpan={7} className="py-6 text-center text-slate-400">
                      No provider configs match.
                    </td>
                  </tr>
                )}
                {rows.map((c) => (
                  <ConfigRow key={c.name} c={c} />
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Pagination — limit/offset over the (kind,name)-ordered list. Rows are
            SLIM (no spec/data — the list query omits the document), so paging
            thousands of configs is cheap. Prev/Next disabled at the ends. */}
        {total > PAGE_SIZE && (
          <div className="flex items-center gap-3 text-sm text-slate-600">
            <span className="tabular-nums">
              {start}–{end} of {total}
            </span>
            <button
              onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
              disabled={offset === 0}
              className="px-2 py-1 rounded bg-slate-200 hover:bg-slate-300 text-slate-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              ← Prev
            </button>
            <button
              onClick={() => setOffset(offset + PAGE_SIZE)}
              disabled={end >= total}
              className="px-2 py-1 rounded bg-slate-200 hover:bg-slate-300 text-slate-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Next →
            </button>
          </div>
        )}
      </div>

      {/* Apply-config modal (new config). Opens empty; on a successful apply it
          deep-links to the config so its stored role/spec render immediately.
          The per-config "Apply…" button (prefilled) lives on ConfigDetailCard. */}
      <ApplyConfigModal
        open={creating}
        onClose={() => setCreating(false)}
        onApplied={(c) => {
          setCreating(false)
          navigate(`/providerconfigs/${encodeURIComponent(c.name)}`)
        }}
      />
    </main>
  )
}

function Filter({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="flex flex-col gap-1">
      <span className="text-[10px] font-medium uppercase tracking-wider text-slate-500">{label}</span>
      {children}
    </label>
  )
}

// ConfigRow is one expandable list row: collapsed it shows identity + role +
// owner; expanded it shows the full document via ConfigDetail.
function ConfigRow({ c }: { c: ProviderConfig }) {
  const [open, setOpen] = useState(false)
  return (
    <>
      <tr
        className="border-b border-slate-100 hover:bg-slate-50 cursor-pointer text-slate-700"
        onClick={() => setOpen((v) => !v)}
      >
        <td className="py-1.5 pl-3 pr-2 text-slate-400 select-none">{open ? '▼' : '▶'}</td>
        <td className="py-1.5 px-2 font-medium break-all">{c.name}</td>
        <td className="py-1.5 px-2 font-mono text-xs">{c.kind}</td>
        <td className="py-1.5 px-2">
          <MajorBadge kind_version={c.kind_version} />
        </td>
        <td className="py-1.5 px-2">
          <RoleBadge isDefault={c.is_default} />
        </td>
        <td className="py-1.5 px-2 text-xs">
          {c.owner_name ? (
            <span className="font-mono">
              <span className="text-slate-400">{c.owner_kind}/</span>
              {c.owner_name}
            </span>
          ) : (
            <span className="text-slate-400">—</span>
          )}
        </td>
        <td className="py-1.5 px-2 text-xs text-slate-500 tabular-nums" title={c.updated_at}>
          {formatDateTime(c.updated_at)}
        </td>
      </tr>
      {open && (
        <tr className="bg-slate-50 border-b border-slate-200">
          <td></td>
          <td colSpan={6} className="py-3 px-2">
            {/* The list row `c` is SLIM (no spec/data — the list query omits the
                document), so fetch the FULL config by name to render its manifest.
                Only fires while expanded (enabled: open). */}
            <ConfigRowDetail name={c.name} />
          </td>
        </tr>
      )}
    </>
  )
}

// ConfigRowDetail fetches one config's full document (spec + data) by name for
// the inline list-row expansion — the list row itself carries only identity/
// role/owner. Shares the ['provider-config', name] cache with the deep-link card.
function ConfigRowDetail({ name }: { name: string }) {
  const { data, isPending, isError, error } = useQuery({
    queryKey: ['provider-config', name],
    queryFn: () => api.getProviderConfig(name),
    refetchInterval: 5000,
    refetchIntervalInBackground: false,
    placeholderData: (prev) => prev,
  })
  if (isError) {
    return <div className="text-xs text-red-600 font-mono">{(error as Error)?.message}</div>
  }
  if (isPending || !data) {
    return <div className="text-xs text-slate-500 italic">Loading…</div>
  }
  return <ConfigDetail c={data} />
}

// ConfigDetailCard fetches and renders a single config by name for the
// /providerconfigs/:name deep link (a resource's "Provider config" link). The
// edit (Apply…) and delete affordances live on the inner ConfigDetail, so they
// are identical here and in the list-row expansion.
function ConfigDetailCard({ name }: { name: string }) {
  const navigate = useNavigate()
  const { data, isPending, isError, error } = useQuery({
    queryKey: ['provider-config', name],
    queryFn: () => api.getProviderConfig(name),
    refetchInterval: 5000,
    refetchIntervalInBackground: false,
    placeholderData: (prev) => prev,
    retry: retryReadAfterWrite,
    retryDelay: retryDelayReadAfterWrite,
  })

  return (
    <div className="bg-white border border-blue-200 rounded p-3">
      <div className="flex items-center justify-between gap-2 mb-2">
        <h3 className="text-sm font-semibold text-slate-900 break-all">
          <span className="font-mono text-slate-500">provider config / </span>
          {name}
        </h3>
        <Link to="/providerconfigs" className="text-xs text-blue-700 hover:underline shrink-0">
          ← all configs
        </Link>
      </div>
      {isError ? (
        <div className="text-xs text-red-600 font-mono">{(error as Error)?.message}</div>
      ) : isPending || !data ? (
        <div className="text-xs text-slate-500 italic">Loading…</div>
      ) : (
        <ConfigDetail c={data} onDeleted={() => navigate('/providerconfigs')} />
      )}
    </div>
  )
}

// ConfigDetail renders the full info for one config: kind, role, owner (linked
// to the owning resource when present), timestamps, and the spec document, plus
// the Edit (Apply…) and Delete affordances. Edit + Delete live HERE (not only on
// the deep-link card) so the list-row expansion on /providerconfigs is fully
// editable too — same affordance whether you reach a config via the list or the
// /providerconfigs/:name URL. onDeleted lets the deep-link detail card navigate
// away after a delete; the list-row expansion omits it and just relies on query
// invalidation to drop the row. A composer-owned config (owner_kind/owner_name
// set) may be re-created on the owner's next compose, so we warn rather than
// block — the delete still goes through.
function ConfigDetail({ c, onDeleted }: { c: ProviderConfig; onDeleted?: () => void }) {
  const queryClient = useQueryClient()
  const [confirming, setConfirming] = useState(false)
  const [applying, setApplying] = useState(false)

  const del = useMutation({
    mutationFn: () => api.deleteProviderConfig(c.name),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['provider-configs'] })
      queryClient.invalidateQueries({ queryKey: ['provider-config', c.name] })
      onDeleted?.()
    },
  })

  return (
    <div>
      <dl className="grid grid-cols-[8rem_1fr] gap-x-3 gap-y-1 text-xs text-slate-700 mb-3 max-w-2xl">
        <dt className="text-slate-400">Kind</dt>
        <dd className="font-mono">{c.kind}</dd>
        <dt className="text-slate-400">Version</dt>
        <dd>
          <MajorBadge kind_version={c.kind_version} />
        </dd>
        <dt className="text-slate-400">Role</dt>
        <dd>
          <RoleBadge isDefault={c.is_default} />
        </dd>
        <dt className="text-slate-400">Owner</dt>
        <dd>
          {c.owner_kind && c.owner_name ? (
            <Link
              to={`/resources/summary/r/${encodeURIComponent(c.owner_kind)}/${encodeURIComponent(c.owner_name)}`}
              className="font-mono text-blue-700 hover:underline"
              title="Open the owning resource"
            >
              <span className="text-slate-400">{c.owner_kind}/</span>
              {c.owner_name}
            </Link>
          ) : (
            <span className="text-slate-400">— (user / API created)</span>
          )}
        </dd>
        <dt className="text-slate-400">Created</dt>
        <dd className="tabular-nums" title={c.created_at}>
          {formatDateTime(c.created_at)}
        </dd>
        <dt className="text-slate-400">Updated</dt>
        <dd className="tabular-nums" title={c.updated_at}>
          {formatDateTime(c.updated_at)}
        </dd>
      </dl>
      {/* Manifest: the config DOCUMENT the detail read returns — spec plus the
          opaque data bundle (base64), the exact shape the Apply editor accepts,
          so it round-trips verbatim. spec and data live INSIDE the manifest;
          there are no separate spec/bundle controls. Download saves this manifest
          JSON so it re-applies as-is (bundle included). */}
      <div className="flex items-center gap-2 mb-1">
        <span className="text-[11px] font-medium text-slate-500 uppercase tracking-wider">Manifest</span>
        <button
          onClick={() => downloadManifest(c)}
          className="text-[11px] px-1.5 py-0.5 rounded bg-slate-200 hover:bg-slate-300 text-slate-700"
          title="Download this config's manifest JSON (kind, kind_version, name, is_default, spec, data) — re-applies verbatim"
        >
          Download
        </button>
      </div>
      <pre className="max-w-2xl p-2 bg-slate-100 border border-slate-200 rounded text-xs text-slate-800 overflow-x-auto whitespace-pre-wrap break-all">
        {manifestJSON(c)}
      </pre>

      <div className="flex items-center gap-2 mt-3">
        {confirming ? (
          <>
            <span className="text-xs text-slate-600">
              {c.owner_kind && c.owner_name
                ? 'Delete this composer-owned config? Its owner may re-create it.'
                : 'Delete this config?'}
            </span>
            <button
              onClick={() => del.mutate()}
              disabled={del.isPending}
              className="text-xs px-2 py-0.5 rounded bg-red-600 hover:bg-red-500 text-white disabled:opacity-50"
            >
              {del.isPending ? 'Deleting…' : 'Confirm delete'}
            </button>
            <button
              onClick={() => setConfirming(false)}
              disabled={del.isPending}
              className="text-xs px-2 py-0.5 rounded bg-white border border-slate-300 hover:bg-slate-100 text-slate-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Cancel
            </button>
          </>
        ) : (
          <>
            <button
              onClick={() => setApplying(true)}
              className="text-xs px-2 py-0.5 rounded bg-slate-200 hover:bg-slate-300 text-slate-700"
              title="Edit this config: apply a new manifest (kind+name locked, role+spec editable)"
            >
              Apply…
            </button>
            <button
              onClick={() => setConfirming(true)}
              className="text-xs px-2 py-0.5 rounded bg-red-50 hover:bg-red-100 text-red-700 border border-red-200"
            >
              Delete…
            </button>
          </>
        )}
        {del.isError && (
          <span className="text-xs text-red-600 font-mono">{(del.error as Error)?.message}</span>
        )}
      </div>

      {/* Prefilled apply (edit) for THIS config — kind+name locked, role+spec
          editable. On success the modal invalidates the list + this config's
          detail query, so the new spec re-renders in place. */}
      <ApplyConfigModal
        open={applying}
        applyTo={c}
        onClose={() => setApplying(false)}
        onApplied={() => setApplying(false)}
      />
    </div>
  )
}

// manifestJSON renders a config as its apply document — {kind, name, kind_version,
// is_default, spec, data?} — the exact shape the Apply editor + POST
// /api/providerconfigs accept, so a download re-applies verbatim (version +
// bundle included). kind_version is REQUIRED and ALWAYS emitted (>= 1): there is no
// implicit v1 default, so a downloaded manifest that omitted it would 422 on
// re-apply. data only when the config has a bundle.
function manifestJSON(c: ProviderConfig): string {
  return JSON.stringify(
    {
      kind: c.kind,
      name: c.name,
      // A stored config always has a version; fall back to 1 only for a config read
      // from an older server that omitted it.
      kind_version: c.kind_version && c.kind_version >= 1 ? c.kind_version : 1,
      is_default: c.is_default,
      spec: c.spec,
      ...(c.data ? { data: c.data } : {}),
    },
    null,
    2,
  )
}

// downloadManifest saves the config's manifest JSON to a file (<name>.json).
function downloadManifest(c: ProviderConfig) {
  const blob = new Blob([manifestJSON(c)], { type: 'application/json' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `${c.name}.json`
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}

// MajorBadge renders a config's web-API version as a "vN" chip (omitted/0 = v1).
function MajorBadge({ kind_version }: { kind_version?: number }) {
  return (
    <span className="inline-block px-1.5 py-0.5 rounded text-[11px] bg-slate-100 text-slate-600 border border-slate-200 font-mono">
      v{kind_version && kind_version > 0 ? kind_version : 1}
    </span>
  )
}

function RoleBadge({ isDefault }: { isDefault: boolean }) {
  return isDefault ? (
    <span className="inline-block px-1.5 py-0.5 rounded text-[11px] bg-emerald-50 text-emerald-800 border border-emerald-200">
      default
    </span>
  ) : (
    <span className="inline-block px-1.5 py-0.5 rounded text-[11px] bg-slate-100 text-slate-600 border border-slate-200">
      custom
    </span>
  )
}

