import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, type ReactorBinding } from '../api'
import { ReactorBindingModal } from '../components/ReactorBindingModal'

// ReactorBindingsPage is the admin view of the runtime-editable reactor_bindings
// table — the SOLE place reactor wiring lives. Each row is a SUBSCRIPTION ("when
// a <watch_kind> resource crosses <transition>, run <reactor>"); the table lists
// them, a row expands to the full subscription, and Edit / Delete manage it live
// (the dispatcher reads bindings fresh on every claim — no restart). Every
// binding is editable — there is no derived / manual split. A binding is the
// whole definition of a durable saga step.
export function ReactorBindingsPage() {
  const queryClient = useQueryClient()
  const [editing, setEditing] = useState<ReactorBinding | null>(null)
  const [creating, setCreating] = useState(false)

  const { data, isPending, isError, error } = useQuery({
    queryKey: ['reactor-bindings'],
    queryFn: () => api.listReactorBindings(),
    refetchInterval: 5000,
    refetchIntervalInBackground: false,
    placeholderData: (prev) => prev,
  })

  const bindings = data ?? []

  return (
    <main className="flex-1 flex flex-col overflow-hidden">
      <div className="flex-1 overflow-y-auto p-6 space-y-4">
        <section className="flex items-start justify-between gap-3">
          <div>
            <h2 className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">
              Reactor bindings
            </h2>
            <div className="text-sm text-slate-600 tabular-nums">
              {isPending
                ? 'Loading…'
                : `${bindings.length} subscription${bindings.length === 1 ? '' : 's'}`}
            </div>
          </div>
          <button
            onClick={() => setCreating(true)}
            className="shrink-0 text-sm px-3 py-1.5 rounded bg-blue-600 hover:bg-blue-500 text-white"
            title="Apply a reactor subscription to create or update it"
          >
            Apply binding
          </button>
        </section>

        <p className="text-xs text-slate-500 max-w-2xl">
          A binding subscribes a reactor to a <span className="font-mono">kind</span>’s lifecycle{' '}
          <span className="font-mono">transition</span> — e.g. on{' '}
          <span className="font-mono">synced</span>, run a reactor that uploads the resource’s status.
          This is the only wiring: a reactor’s CRD just registers it. Edits apply live.
        </p>

        {isError ? (
          <div className="max-w-lg p-4 bg-red-50 border border-red-200 rounded text-sm text-red-800">
            <div className="font-semibold mb-1">Failed to load reactor bindings</div>
            <div className="text-xs font-mono">{(error as Error)?.message}</div>
          </div>
        ) : (
          <div className="bg-white border border-slate-200 rounded overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-slate-500 bg-slate-50 border-b border-slate-200">
                  <th className="font-normal py-1.5 pl-3 pr-2 w-6"></th>
                  <th className="font-normal py-1.5 px-2">Name</th>
                  <th className="font-normal py-1.5 px-2">Watch kind</th>
                  <th className="font-normal py-1.5 px-2">Transition</th>
                  <th className="font-normal py-1.5 px-2">Reactor</th>
                  <th className="font-normal py-1.5 px-2">Status</th>
                </tr>
              </thead>
              <tbody>
                {bindings.length === 0 && !isPending && (
                  <tr>
                    <td colSpan={6} className="py-6 text-center text-slate-400">
                      No reactor bindings yet. Create one to subscribe a reactor to a resource lifecycle transition.
                    </td>
                  </tr>
                )}
                {bindings.map((b) => (
                  <BindingRow key={b.name} b={b} onEdit={() => setEditing(b)} />
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <ReactorBindingModal
        open={creating}
        onClose={() => setCreating(false)}
        onApplied={() => {
          setCreating(false)
          queryClient.invalidateQueries({ queryKey: ['reactor-bindings'] })
        }}
      />
      <ReactorBindingModal
        open={editing !== null}
        editing={editing}
        onClose={() => setEditing(null)}
        onApplied={() => setEditing(null)}
      />
    </main>
  )
}

// BindingRow is one expandable list row: collapsed it shows identity + the
// (watch_kind, transition) edge + reactor + enabled badge; expanded it shows the
// full subscription plus Edit / Delete.
function BindingRow({ b, onEdit }: { b: ReactorBinding; onEdit: () => void }) {
  const [open, setOpen] = useState(false)
  return (
    <>
      <tr
        className="border-b border-slate-100 hover:bg-slate-50 cursor-pointer text-slate-700"
        onClick={() => setOpen((v) => !v)}
      >
        <td className="py-1.5 pl-3 pr-2 text-slate-400 select-none">{open ? '▼' : '▶'}</td>
        <td className="py-1.5 px-2 font-medium break-all">{b.name}</td>
        <td className="py-1.5 px-2 font-mono text-xs">
          {b.watch_kind}
          {/* A version-scoped binding shows "kind/vN"; unscoped watches all versions. */}
          {b.watch_kind_version ? <span className="text-slate-400">/v{b.watch_kind_version}</span> : null}
        </td>
        <td className="py-1.5 px-2">
          <TransitionBadge transition={b.transition} />
        </td>
        <td className="py-1.5 px-2 font-mono text-xs">
          {b.reactor}
          {/* A pinned reactor_version shows as "reactor/vN"; unpinned resolves the
              reactor's highest published version at claim time, so no suffix. */}
          {b.reactor_version ? <span className="text-slate-400">/v{b.reactor_version}</span> : null}
        </td>
        <td className="py-1.5 px-2">
          <EnabledBadge enabled={b.enabled} />
        </td>
      </tr>
      {open && (
        <tr className="bg-slate-50 border-b border-slate-200">
          <td></td>
          <td colSpan={5} className="py-3 px-2">
            <BindingDetail b={b} onEdit={onEdit} />
          </td>
        </tr>
      )}
    </>
  )
}

function BindingDetail({ b, onEdit }: { b: ReactorBinding; onEdit: () => void }) {
  const queryClient = useQueryClient()
  const [confirming, setConfirming] = useState(false)

  const del = useMutation({
    mutationFn: () => api.deleteReactorBinding(b.name),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['reactor-bindings'] })
    },
  })

  const labels = b.label_match as Record<string, unknown> | undefined
  const hasLabels = labels && Object.keys(labels).length > 0

  return (
    <div>
      <dl className="grid grid-cols-[9rem_1fr] gap-x-3 gap-y-1 text-xs text-slate-700 mb-3 max-w-2xl">
        <dt className="text-slate-400">Watch kind</dt>
        <dd className="font-mono">
          {b.watch_kind}
          {b.watch_kind_version ? (
            <span className="text-slate-400"> /v{b.watch_kind_version}</span>
          ) : (
            <span className="text-slate-400"> (all versions)</span>
          )}
        </dd>
        <dt className="text-slate-400">Transition</dt>
        <dd>
          <TransitionBadge transition={b.transition} />
        </dd>
        <dt className="text-slate-400">Reactor</dt>
        <dd className="font-mono">
          {b.reactor}
          {b.reactor_version ? (
            <span className="text-slate-400"> /v{b.reactor_version}</span>
          ) : (
            <span className="text-slate-400"> (highest published)</span>
          )}
        </dd>
        <dt className="text-slate-400">Label match</dt>
        <dd className="font-mono">
          {hasLabels ? JSON.stringify(labels) : <span className="text-slate-400">— (all)</span>}
        </dd>
        <dt className="text-slate-400">Status</dt>
        <dd>
          <EnabledBadge enabled={b.enabled} />
        </dd>
      </dl>

      {/* Manifest: the binding's apply document — the exact body POST
          /api/reactor-bindings accepts ({name, watch_kind, transition, reactor,
          label_match?, enabled}), so a download re-applies verbatim. */}
      <div className="flex items-center gap-2 mb-1">
        <span className="text-[11px] font-medium text-slate-500 uppercase tracking-wider">Manifest</span>
        <button
          onClick={() => downloadManifest(b)}
          className="text-[11px] px-1.5 py-0.5 rounded bg-slate-200 hover:bg-slate-300 text-slate-700"
          title="Download this binding's manifest JSON — re-applies verbatim"
        >
          Download
        </button>
      </div>
      <pre className="max-w-2xl p-2 bg-slate-100 border border-slate-200 rounded text-xs text-slate-800 overflow-x-auto whitespace-pre-wrap break-all">
        {manifestJSON(b)}
      </pre>

      <div className="flex items-center gap-2">
        <button
          onClick={onEdit}
          className="text-xs px-2 py-0.5 rounded bg-slate-200 hover:bg-slate-300 text-slate-700"
        >
          Edit…
        </button>
        {confirming ? (
          <>
            <span className="text-xs text-slate-600">Delete this binding?</span>
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
          <button
            onClick={() => setConfirming(true)}
            className="text-xs px-2 py-0.5 rounded bg-red-50 hover:bg-red-100 text-red-700 border border-red-200"
          >
            Delete…
          </button>
        )}
        {del.isError && (
          <span className="text-xs text-red-600 font-mono">{(del.error as Error)?.message}</span>
        )}
      </div>
    </div>
  )
}

// manifestJSON renders a binding as its apply document — the exact body
// POST /api/reactor-bindings accepts, so a download re-applies verbatim.
// label_match is omitted when empty (matches all).
function manifestJSON(b: ReactorBinding): string {
  const labels = b.label_match as Record<string, unknown> | undefined
  const hasLabels = labels && Object.keys(labels).length > 0
  return JSON.stringify(
    {
      name: b.name,
      watch_kind: b.watch_kind,
      // Emit watch_kind_version only when scoped (unscoped = all versions).
      ...(b.watch_kind_version ? { watch_kind_version: b.watch_kind_version } : {}),
      transition: b.transition,
      reactor: b.reactor,
      // Emit reactor_version only when pinned (unpinned = highest published).
      ...(b.reactor_version ? { reactor_version: b.reactor_version } : {}),
      ...(hasLabels ? { label_match: labels } : {}),
      enabled: b.enabled,
    },
    null,
    2,
  )
}

// downloadManifest saves the binding's manifest JSON to a file (<name>.json).
function downloadManifest(b: ReactorBinding) {
  const blob = new Blob([manifestJSON(b)], { type: 'application/json' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `${b.name}.json`
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}

function TransitionBadge({ transition }: { transition: string }) {
  const tone =
    transition === 'synced'
      ? 'bg-emerald-50 text-emerald-800 border-emerald-200'
      : transition === 'failed' || transition === 'degraded'
        ? 'bg-amber-50 text-amber-800 border-amber-200'
        : transition === 'deleted'
          ? 'bg-red-50 text-red-700 border-red-200'
          : 'bg-slate-100 text-slate-600 border-slate-200'
  return (
    <span className={`inline-block px-1.5 py-0.5 rounded text-[11px] border ${tone}`}>
      {transition}
    </span>
  )
}

function EnabledBadge({ enabled }: { enabled: boolean }) {
  return enabled ? (
    <span className="inline-block px-1.5 py-0.5 rounded text-[11px] bg-emerald-50 text-emerald-800 border border-emerald-200">
      enabled
    </span>
  ) : (
    <span className="inline-block px-1.5 py-0.5 rounded text-[11px] bg-slate-100 text-slate-500 border border-slate-200">
      disabled
    </span>
  )
}
