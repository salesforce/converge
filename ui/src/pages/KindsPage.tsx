import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, type KindSummary, type KindOperational, type ReactionDecl } from '../api'
import { KindConfigModal } from '../components/KindConfigModal'
import { KindManifestModal } from '../components/KindManifestModal'

// KindsPage is the admin view of every DECLARED kind and its CRD + OPERATIONAL
// config. "Apply CRD" applies a KindManifest (schemas + reactions +
// finalizer) — the operator-facing registration path (update-in-place OR a new
// version, exactly like a resource apply); the two DB triggers then derive the
// kind's kind_config + lifecycle bindings. There is no separate edit-CRD-in-a-
// modal flow — every CRD change goes through Apply. Each row shows the declared
// versions + cap/resync at a glance, expands to the kind's reactions + per-kind_version
// spec/status/config schema + live default config, and has "Edit config" (the
// live cap/resync — takes effect on the next claim, no restart).
export function KindsPage() {
  const [editKind, setEditKind] = useState<{
    kind: string
    current?: KindOperational
    kindVersions?: number[]
  } | null>(null)
  // "Apply CRD" opens the manifest modal on a NEW (blank) CRD; applying an
  // update just re-applies the same document (there is no edit-prefill path).
  const [applyingCrd, setApplyingCrd] = useState(false)

  const { data, isPending, isError, error } = useQuery({
    // The ['kind-schemas'] key holds the richer KindSummary[] (api.listKindSchemas)
    // — the single registered-kind census shared by the Kinds page, the Resources
    // FilterBar, ProviderConfigsPage, and ReactorBindingModal.
    queryKey: ['kind-schemas'],
    queryFn: () => api.listKindSchemas(),
    refetchInterval: 5000,
    refetchIntervalInBackground: false,
    placeholderData: (prev) => prev,
  })

  const kinds = data ?? []

  return (
    <main className="flex-1 flex flex-col overflow-hidden">
      <div className="flex-1 overflow-y-auto p-6 space-y-4">
        <section className="flex items-end justify-between">
          <div>
            <h2 className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">Kinds</h2>
            <div className="text-sm text-slate-600 tabular-nums">
              {isPending ? 'Loading…' : `${kinds.length} kind${kinds.length === 1 ? '' : 's'}`}
            </div>
          </div>
          <button
            onClick={() => setApplyingCrd(true)}
            className="shrink-0 text-sm px-3 py-1.5 rounded bg-blue-600 hover:bg-blue-500 text-white"
            title="Apply a kind CRD to define or update a kind (a new version too)"
          >
            Apply CRD
          </button>
        </section>

        {isError && (
          <div className="p-2 bg-red-50 border border-red-200 rounded text-sm text-red-800">
            {(error as Error).message}
          </div>
        )}

        <div className="bg-white border border-slate-200 rounded overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-slate-50 text-left text-xs text-slate-500 uppercase tracking-wider">
              <tr>
                <th className="py-2 pl-3 pr-2 w-6"></th>
                <th className="py-2 px-2">Kind</th>
                <th className="py-2 px-2">Versions</th>
                <th className="py-2 px-2">Max in-flight</th>
                <th className="py-2 px-2">Task deadline</th>
                <th className="py-2 px-2">Resync</th>
                <th className="py-2 px-2">Orphan grace</th>
                <th className="py-2 px-2">Shape</th>
                <th className="py-2 px-2 w-16"></th>
              </tr>
            </thead>
            <tbody>
              {kinds.map((k) => (
                <KindRow
                  key={k.kind}
                  k={k}
                  onEdit={() =>
                    setEditKind({ kind: k.kind, current: k.operational, kindVersions: k.kind_versions })
                  }
                />
              ))}
              {!isPending && kinds.length === 0 && (
                <tr>
                  <td colSpan={9} className="py-6 text-center text-slate-400">
                    No kinds declared.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      <KindConfigModal
        open={editKind !== null}
        kind={editKind?.kind ?? ''}
        current={editKind?.current}
        kindVersions={editKind?.kindVersions}
        onClose={() => setEditKind(null)}
      />

      {/* Apply CRD (new/update-in-place). No prefill — a CRD change re-applies
          the whole document, exactly like a resource apply; there is no
          edit-CRD-in-a-modal path. */}
      <KindManifestModal
        open={applyingCrd}
        onClose={() => setApplyingCrd(false)}
      />
    </main>
  )
}

// KindRow is one expandable kind: collapsed it shows the cap + resync + shape
// flags; expanded it shows the kind's description and its spec/status/config
// JSON Schema + live default config document (fetched on first expand).
function KindRow({
  k,
  onEdit,
}: {
  k: KindSummary
  onEdit: () => void
}) {
  const [open, setOpen] = useState(false)
  return (
    <>
      <tr
        className="border-b border-slate-100 hover:bg-slate-50 cursor-pointer text-slate-700"
        onClick={() => setOpen((v) => !v)}
      >
        <td className="py-1.5 pl-3 pr-2 text-slate-400 select-none">{open ? '▼' : '▶'}</td>
        <td className="py-1.5 px-2 font-mono">
          <span className="inline-flex items-center gap-1.5">
            {k.kind}
            {k.is_reactor && (
              <span className="inline-block px-1.5 py-0.5 rounded text-[11px] bg-purple-50 text-purple-800 border border-purple-200">
                reactor
              </span>
            )}
          </span>
        </td>
        <td className="py-1.5 px-2">
          <VersionChips versions={k.kind_versions} retired={k.operational?.retired} />
        </td>
        {/* Reactors have no kind_config (uncapped, no deadline/resync) and aren't
            tuned here — show a dash across the operational columns. */}
        <td className="py-1.5 px-2 tabular-nums">
          {k.is_reactor ? <span className="text-slate-300">—</span> : <CapBadge max={k.operational?.max_inflight ?? 0} />}
        </td>
        <td className="py-1.5 px-2 text-xs">
          {k.is_reactor ? <span className="text-slate-300">—</span> : <DeadlineBadge secs={k.operational?.task_deadline_seconds ?? 0} />}
        </td>
        <td className="py-1.5 px-2 text-xs">
          {k.is_reactor ? <span className="text-slate-300">—</span> : <ResyncBadge op={k.operational} />}
        </td>
        <td className="py-1.5 px-2 text-xs">
          {k.is_reactor ? <span className="text-slate-300">—</span> : <OrphanGraceBadge secs={k.operational?.orphan_grace_seconds ?? 0} />}
        </td>
        <td className="py-1.5 px-2 text-xs text-slate-500">{shapeTags(k)}</td>
        <td className="py-1.5 px-2">
          <div className="flex items-center justify-end gap-1.5">
            {/* Config = the live cap/resync (kind_config). Reactors have no
                kind_config row, so only work kinds get it. A CRD change (schemas /
                reactions / a new version) goes through the page's Apply CRD, not
                an edit here. */}
            {!k.is_reactor && (
              <button
                onClick={(e) => {
                  e.stopPropagation()
                  onEdit()
                }}
                className="text-xs px-2 py-0.5 rounded bg-slate-200 hover:bg-slate-300 text-slate-700"
              >
                Config
              </button>
            )}
          </div>
        </td>
      </tr>
      {open && (
        <tr className="bg-slate-50 border-b border-slate-200">
          <td></td>
          <td colSpan={8} className="py-3 px-2">
            <KindDetail kind={k.kind} description={k.description} kindVersions={k.kind_versions} isReactor={k.is_reactor} />
          </td>
        </tr>
      )}
    </>
  )
}

// KindDetail lazy-fetches a kind's full schema (spec/status/config) + live
// default config document only when the row is expanded. When the kind declares
// more than one web-API version a small kind_version selector re-fetches the schema for
// the chosen kind_version, so the /docs links point at that version's shape refs (v1
// unsuffixed, v2+ suffixed). Reactor kinds are served too — they carry a config
// schema (their sink config) but no spec/status, so those blocks simply omit
// themselves.
function KindDetail({
  kind,
  description,
  kindVersions,
  isReactor,
}: {
  kind: string
  description?: string
  kindVersions?: number[]
  isReactor?: boolean
}) {
  const queryClient = useQueryClient()
  // Available versions (always ≥ [1]); the selector defaults to the newest.
  const versions = kindVersions && kindVersions.length ? kindVersions : [1]
  const [kind_version, setKindVersion] = useState(() => versions[versions.length - 1])
  // Per-version delete: a two-click confirm (the button arms, a second click
  // fires) so a CRD is never dropped by a single stray click. A 409 (still
  // referenced by a resource / pinned binding) surfaces inline.
  const [confirmDelete, setConfirmDelete] = useState(false)
  const del = useMutation({
    mutationFn: () => api.deleteKindManifest(kind, kind_version),
    onSuccess: () => {
      setConfirmDelete(false)
      // Drop the whole version from every view (the row's version list, the
      // per-version schema/manifest caches).
      queryClient.invalidateQueries({ queryKey: ['kind-schemas'] })
      queryClient.invalidateQueries({ queryKey: ['kind', kind] })
      queryClient.invalidateQueries({ queryKey: ['kind-manifest', kind] })
      queryClient.invalidateQueries({ queryKey: ['kind-manifests'] })
      queryClient.invalidateQueries({ queryKey: ['kinds'] })
    },
  })

  const { data, isPending, isError, error } = useQuery({
    // Keyed by (kind, kind_version): switching the selector re-fetches that version's
    // schema refs rather than re-deriving them client-side.
    queryKey: ['kind', kind, kind_version],
    queryFn: () => api.getKindSchema(kind, kind_version),
    staleTime: 30_000,
  })
  // The manifest carries the declared reactions (the schema endpoint doesn't),
  // so fetch it too for the reactions table + manifest_version.
  const manifestQ = useQuery({
    queryKey: ['kind-manifest', kind, kind_version],
    queryFn: () => api.getKindManifest(kind, kind_version),
    staleTime: 30_000,
  })

  if (isPending) return <div className="text-xs text-slate-400">Loading schema…</div>
  if (isError) return <div className="text-xs text-red-700">{(error as Error).message}</div>

  const hasSchema = data?.spec_schema_ref || data?.status_schema_ref || data?.config_schema_ref

  return (
    <div className="space-y-3">
      {description && <p className="text-sm text-slate-600">{description}</p>}
      {isReactor && (
        <p className="text-xs text-slate-400">
          Reactor — it owns no resource and isn’t created directly. This CRD only registers its{' '}
          <span className="font-mono">reactor</span> reaction; wire it to a kind’s lifecycle
          transition with an editable subscription on the{' '}
          <a href="/reactor-bindings" className="text-blue-700 hover:underline">
            Reactor bindings
          </a>{' '}
          page.
        </p>
      )}
      <ReactionsTable reactions={manifestQ.data?.reactions} />

      {/* Per-version delete. The selector (when >1 version) picks the target;
          otherwise it's the sole version. Two-click confirm; a 409 (still
          referenced) shows inline. Deleting removes the CRD + its derived config
          + its provider configs — never a resource. */}
      <div className="flex items-center flex-wrap gap-2 text-xs">
        <span className="text-slate-500">
          Delete <span className="font-mono">{kind}/v{kind_version}</span>
          {versions.length > 1 && (
            <select
              value={kind_version}
              onChange={(e) => {
                setKindVersion(Number(e.target.value))
                setConfirmDelete(false)
              }}
              className="ml-1.5 px-1 py-0.5 text-[11px] border border-slate-300 rounded bg-white text-slate-700 focus:outline-none focus:ring-1 focus:ring-blue-400"
              title="Choose the version to delete"
            >
              {versions.map((v) => (
                <option key={v} value={v}>
                  v{v}
                </option>
              ))}
            </select>
          )}
        </span>
        {!confirmDelete ? (
          <button
            onClick={() => setConfirmDelete(true)}
            disabled={del.isPending}
            className="px-2 py-0.5 rounded border border-red-200 text-red-700 hover:bg-red-50 disabled:opacity-50"
          >
            Delete this version
          </button>
        ) : (
          <span className="inline-flex items-center gap-1.5">
            <span className="text-slate-500">Delete the CRD + configs for this version?</span>
            <button
              onClick={() => del.mutate()}
              disabled={del.isPending}
              className="px-2 py-0.5 rounded bg-red-600 hover:bg-red-500 text-white disabled:opacity-50"
            >
              {del.isPending ? 'Deleting…' : 'Confirm delete'}
            </button>
            <button
              onClick={() => setConfirmDelete(false)}
              disabled={del.isPending}
              className="px-2 py-0.5 rounded bg-white border border-slate-300 hover:bg-slate-100 text-slate-700 disabled:opacity-50"
            >
              Cancel
            </button>
          </span>
        )}
        {del.isError && (
          <span className="text-red-700">{(del.error as Error).message}</span>
        )}
      </div>

      {hasSchema && (
        <div>
          <div className="flex items-center gap-2 mb-1">
            <div className="text-xs font-medium text-slate-500 uppercase tracking-wider">
              Shapes (API docs)
            </div>
            {/* KindVersion selector — only when the kind serves more than one version.
                Re-fetches the schema for the chosen kind_version so the /docs links use
                that version's refs. */}
            {versions.length > 1 && (
              <select
                value={kind_version}
                onChange={(e) => setKindVersion(Number(e.target.value))}
                className="px-1.5 py-0.5 text-[11px] border border-slate-300 rounded bg-white text-slate-700 focus:outline-none focus:ring-1 focus:ring-blue-400"
                title="Show the schema for this web-API version"
              >
                {versions.map((v) => (
                  <option key={v} value={v}>
                    v{v}
                  </option>
                ))}
              </select>
            )}
          </div>
          <div className="flex flex-wrap gap-2">
            <SchemaDocsLink label="Spec" schemaRef={data?.spec_schema_ref} />
            <SchemaDocsLink label="Status" schemaRef={data?.status_schema_ref} />
            <SchemaDocsLink label="Config" schemaRef={data?.config_schema_ref} />
          </div>
        </div>
      )}
      <DocValueBlock label="Default config (live)" value={data?.default_config} />
    </div>
  )
}

// VersionChips renders a kind's declared web-API versions as small "vN" chips
// (always ≥ v1). A second/third chip flags a kind that serves multiple versions
// concurrently. `retired` tags the v1 chip when v1 is retired (the list row only
// carries the representative operational block; the per-version retired state is
// on the expanded census panel).
function VersionChips({ versions, retired }: { versions?: number[]; retired?: boolean }) {
  const vs = versions && versions.length ? versions : [1]
  return (
    <div className="flex flex-wrap gap-1">
      {vs.map((v) => (
        <span
          key={v}
          className={`inline-block px-1.5 py-0.5 rounded text-[11px] border font-mono ${
            retired && v === 1
              ? 'bg-amber-50 text-amber-800 border-amber-200 line-through'
              : 'bg-slate-100 text-slate-600 border-slate-200'
          }`}
          title={retired && v === 1 ? 'v1 is retired (new creates frozen)' : undefined}
        >
          v{v}
        </span>
      ))}
    </div>
  )
}

// SchemaDocsLink opens the kind's shape at its concrete /docs component
// (Stoplight Elements deep link, #/schemas/{ref}) in a new tab — so a shape is
// inspected in the live API docs rather than dumped as JSON text here. Omitted
// when the kind has no such shape.
function SchemaDocsLink({ label, schemaRef }: { label: string; schemaRef?: string }) {
  if (!schemaRef) return null
  return (
    <a
      href={`/docs#/schemas/${encodeURIComponent(schemaRef)}`}
      target="_blank"
      rel="noreferrer"
      className="inline-flex items-center gap-1 text-xs px-2 py-1 rounded bg-blue-50 hover:bg-blue-100 text-blue-700 border border-blue-200"
    >
      {label} <span className="font-mono text-blue-400">{schemaRef}</span> ↗
    </a>
  )
}

// DocValueBlock renders an optional JSON VALUE (the live default config
// document) as pretty-printed JSON; omitted when absent. Schemas link to /docs
// instead (SchemaDocsLink) — this is for a value that has no docs component.
function DocValueBlock({ label, value }: { label: string; value: unknown }) {
  if (value === undefined || value === null) return null
  return (
    <div>
      <div className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">{label}</div>
      <pre className="bg-white border border-slate-200 rounded p-2 font-mono text-xs text-slate-800 overflow-x-auto">
        {JSON.stringify(value, null, 2)}
      </pre>
    </div>
  )
}

// ReactionsTable shows a kind's declared reactions — each a (trigger, emits)
// pair the core dispatches off: when the trigger fires it applies the declared
// emits. Omitted while loading / when none.
function ReactionsTable({ reactions }: { reactions?: ReactionDecl[] }) {
  if (!reactions || reactions.length === 0) return null
  return (
    <div>
      <div className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">Reactions</div>
      <div className="flex flex-col gap-1.5">
        {reactions.map((r) => (
          <div key={r.name} className="flex items-center flex-wrap gap-1.5 text-xs">
            <span className="font-mono text-slate-700">{r.name}</span>
            <span className="text-slate-300">·</span>
            <span className="inline-block px-1.5 py-0.5 rounded text-[11px] bg-indigo-50 text-indigo-800 border border-indigo-200">
              {r.trigger}
              {r.trigger === 'operation' && r.verb ? `:${r.verb}` : ''}
            </span>
            <span className="text-slate-400">→</span>
            {r.emits.map((e) => (
              <span
                key={e}
                className="inline-block px-1.5 py-0.5 rounded text-[11px] bg-slate-100 text-slate-600 border border-slate-200 font-mono"
              >
                {e}
              </span>
            ))}
          </div>
        ))}
      </div>
    </div>
  )
}

// CapBadge shows the global in-flight cap, or "uncapped" (max 0).
function CapBadge({ max }: { max: number }) {
  if (max > 0) return <span className="font-medium">{max}</span>
  return <span className="text-slate-400">uncapped</span>
}

// DeadlineBadge shows the per-task deadline as a friendly duration, or "none" (0).
function DeadlineBadge({ secs }: { secs: number }) {
  if (secs <= 0) return <span className="text-slate-400">none</span>
  const label =
    secs % 3600 === 0 ? `${secs / 3600}h` : secs % 60 === 0 ? `${secs / 60}m` : `${secs}s`
  return (
    <span className="inline-block px-1.5 py-0.5 rounded text-[11px] bg-violet-50 text-violet-800 border border-violet-200">
      {label}
    </span>
  )
}

// ResyncBadge shows the drift-resync policy: interval (+ "recompose"), or "off".
function ResyncBadge({ op }: { op?: KindOperational }) {
  const secs = op?.resync_interval_seconds ?? 0
  if (secs <= 0) return <span className="text-slate-400">off</span>
  const label =
    secs % 3600 === 0 ? `${secs / 3600}h` : secs % 60 === 0 ? `${secs / 60}m` : `${secs}s`
  return (
    <span className="inline-flex items-center gap-1">
      <span className="inline-block px-1.5 py-0.5 rounded text-[11px] bg-blue-50 text-blue-800 border border-blue-200">
        {label}
      </span>
      {op?.resync_recomposes && (
        <span className="inline-block px-1.5 py-0.5 rounded text-[11px] bg-amber-50 text-amber-800 border border-amber-200">
          recompose
        </span>
      )}
    </span>
  )
}

// OrphanGraceBadge shows the orphan-grace window as a friendly duration, or
// "none" (0 = a dropped child is pruned immediately). Purple to match the
// Orphaned phase color.
function OrphanGraceBadge({ secs }: { secs: number }) {
  if (secs <= 0) return <span className="text-slate-400">none</span>
  const label =
    secs % 3600 === 0 ? `${secs / 3600}h` : secs % 60 === 0 ? `${secs / 60}m` : `${secs}s`
  return (
    <span className="inline-block px-1.5 py-0.5 rounded text-[11px] bg-purple-50 text-purple-800 border border-purple-200">
      {label}
    </span>
  )
}

// shapeTags summarises which capability schemas the kind declares.
function shapeTags(k: KindSummary): string {
  const tags: string[] = []
  if (k.has_spec) tags.push('spec')
  if (k.has_status) tags.push('status')
  if (k.has_config) tags.push('config')
  return tags.length ? tags.join(' · ') : '—'
}
