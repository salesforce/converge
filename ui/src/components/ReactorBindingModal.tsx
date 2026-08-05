import { useEffect, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useQuery } from '@tanstack/react-query'
import {
  api,
  type ReactorBinding,
  type LifecycleTransition,
} from '../api'

interface Props {
  open: boolean
  /** When set, the form starts pre-filled to edit this binding (name locked). */
  editing?: ReactorBinding | null
  onClose: () => void
  onApplied?: (b: ReactorBinding) => void
}

const TRANSITIONS: LifecycleTransition[] = ['created', 'synced', 'degraded', 'failed', 'deleted']

// ReactorBindingModal creates or edits one reactor_bindings SUBSCRIPTION —
// "when a <watch_kind> resource crosses <transition>, run the <reactor> kind's
// reaction". A structured form (not a JSON editor) since a subscription is a
// handful of typed fields. Upsert is keyed by name: editing locks the name (it's
// the identity), creating leaves it open. The change is LIVE — the dispatcher
// reads bindings fresh on every claim, no restart.
export function ReactorBindingModal({ open, editing, onClose, onApplied }: Props) {
  const queryClient = useQueryClient()
  const [name, setName] = useState('')
  const [watchKind, setWatchKind] = useState('')
  const [transition, setTransition] = useState<LifecycleTransition>('synced')
  // watchKindVersion scopes the subscription to one version of the WATCHED kind
  // ('' = all versions).
  const [watchKindVersion, setWatchKindVersion] = useState('')
  const [reactor, setReactor] = useState('')
  // reactorVersion is a free-text pin ('' = unpinned → resolve the reactor's
  // highest published version at claim time; a number pins that exact version).
  const [reactorVersion, setReactorVersion] = useState('')
  const [labelMatch, setLabelMatch] = useState('')
  const [enabled, setEnabled] = useState(true)
  const [error, setError] = useState<string>()

  // Registered kinds (from the manifests) drive BOTH field suggestions — a single
  // source, so a kind is offered as soon as its CRD is applied (even before any
  // resource of it exists). watch_kind = every registered kind; reactor = only the
  // is_reactor ones (a reactor owns no resource, so it's never a watch_kind).
  const { data: kindSchemas } = useQuery({
    queryKey: ['kind-schemas'],
    queryFn: () => api.listKindSchemas(),
    staleTime: 30_000,
    enabled: open,
  })
  const kinds = (kindSchemas ?? []).map((k) => k.kind)
  const reactorKinds = (kindSchemas ?? []).filter((k) => k.is_reactor).map((k) => k.kind)

  // Reset the form each time it opens — to the edited binding, or empty
  // defaults. This is an intentional sync from props on the open→true edge
  // (and on swapping which binding is edited); the one extra render it
  // causes is correct and bounded, so the cascading-render rule is disabled
  // for this deliberate case.
  /* eslint-disable react-hooks/set-state-in-effect */
  useEffect(() => {
    if (!open) return
    setName(editing?.name ?? '')
    setWatchKind(editing?.watch_kind ?? '')
    setWatchKindVersion(editing?.watch_kind_version ? String(editing.watch_kind_version) : '')
    setTransition(editing?.transition ?? 'synced')
    setReactor(editing?.reactor ?? '')
    setReactorVersion(editing?.reactor_version ? String(editing.reactor_version) : '')
    setLabelMatch(
      editing?.label_match && Object.keys(editing.label_match as object).length > 0
        ? JSON.stringify(editing.label_match)
        : '',
    )
    setEnabled(editing?.enabled ?? true)
    setError(undefined)
  }, [open, editing])
  /* eslint-enable react-hooks/set-state-in-effect */

  const mutate = useMutation({
    mutationFn: () => {
      const n = name.trim()
      if (n === '') throw new Error('Name is required.')
      if (watchKind.trim() === '') throw new Error('Watch kind is required.')
      if (reactor.trim() === '') throw new Error('Reactor is required.')
      if (reactor.trim() === watchKind.trim())
        throw new Error('Reactor must differ from watch kind (a reactor owns no resource of its own).')
      let labels: unknown
      if (labelMatch.trim() !== '') {
        try {
          labels = JSON.parse(labelMatch)
        } catch {
          throw new Error('Label match must be a JSON object, e.g. {"team":"alpha"}.')
        }
      }
      // reactor_version: blank = unpinned; else a positive integer pinning the
      // reactor's web-API version. Reject a non-positive/non-integer entry.
      let reactorVer: number | undefined
      if (reactorVersion.trim() !== '') {
        const v = Number(reactorVersion)
        if (!Number.isInteger(v) || v < 1) {
          throw new Error('Reactor version must be a positive whole number (blank = highest published).')
        }
        reactorVer = v
      }
      // watch_kind_version: blank = all versions; else a positive integer scoping to
      // that version of the watched kind.
      let watchVer: number | undefined
      if (watchKindVersion.trim() !== '') {
        const v = Number(watchKindVersion)
        if (!Number.isInteger(v) || v < 1) {
          throw new Error('Watch kind version must be a positive whole number (blank = all versions).')
        }
        watchVer = v
      }
      const b: ReactorBinding = {
        name: n,
        watch_kind: watchKind.trim(),
        watch_kind_version: watchVer,
        transition,
        reactor: reactor.trim(),
        reactor_version: reactorVer,
        label_match: labels,
        enabled,
      }
      return api.applyReactorBinding(b)
    },
    onSuccess: (b) => {
      queryClient.invalidateQueries({ queryKey: ['reactor-bindings'] })
      onApplied?.(b)
      onClose()
    },
    onError: (err: Error) => setError(err.message),
  })

  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !mutate.isPending) onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [open, onClose, mutate.isPending])

  if (!open) return null

  // Required-field gate for Save — mirrors the mutation's own validation
  // (name + watch_kind + reactor). Disabling the button stops submitting
  // known-invalid input and spam-clicking the error.
  const canSave = name.trim() !== '' && watchKind.trim() !== '' && reactor.trim() !== ''

  return (
    <div
      className="fixed inset-0 bg-slate-900/40 backdrop-blur-sm flex items-center justify-center z-50"
      onClick={() => {
        if (!mutate.isPending) onClose()
      }}
      role="presentation"
    >
      <div
        className="bg-white border border-slate-200 rounded-lg shadow-xl w-full max-w-md flex flex-col"
        onClick={(e) => e.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-labelledby="reactor-binding-title"
      >
        <div className="flex items-center justify-between p-4 border-b border-slate-200">
          <h2 id="reactor-binding-title" className="text-base font-semibold text-slate-900">
            {editing ? (
              <>
                Edit binding: <span className="font-mono">{editing.name}</span>
              </>
            ) : (
              'New reactor binding'
            )}
          </h2>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-slate-700 text-2xl leading-none"
            aria-label="Close"
          >
            &times;
          </button>
        </div>

        <div className="p-4 space-y-4">
          <Field label="Name" hint="Unique subscription name (the identity; locked when editing).">
            <input
              value={name}
              disabled={!!editing}
              onChange={(e) => setName(e.target.value)}
              placeholder="my-subscription"
              className="w-full px-2 py-1 text-sm border border-slate-300 rounded outline-none focus:ring-1 focus:ring-blue-400 disabled:bg-slate-100 disabled:text-slate-500"
            />
          </Field>

          <div className="grid grid-cols-2 gap-3">
            <Field label="Watch kind" hint="Resource kind to watch.">
              <input
                list="reactor-binding-kinds"
                value={watchKind}
                onChange={(e) => setWatchKind(e.target.value)}
                placeholder="a resource kind"
                className="w-full px-2 py-1 text-sm border border-slate-300 rounded outline-none focus:ring-1 focus:ring-blue-400"
              />
              <datalist id="reactor-binding-kinds">
                {(kinds ?? []).map((k) => (
                  <option key={k} value={k} />
                ))}
              </datalist>
            </Field>
            <Field label="Transition" hint="Lifecycle edge that fires it.">
              <select
                value={transition}
                onChange={(e) => setTransition(e.target.value as LifecycleTransition)}
                className="w-full px-2 py-1 text-sm border border-slate-300 rounded bg-white outline-none focus:ring-1 focus:ring-blue-400"
              >
                {TRANSITIONS.map((t) => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))}
              </select>
            </Field>
          </div>

          <Field
            label="Watch kind version (optional)"
            hint="Scope the subscription to ONE web-API version of the watched kind. Blank = all versions (fire on the transition of any version's resource)."
          >
            <input
              type="number"
              min={1}
              value={watchKindVersion}
              onChange={(e) => setWatchKindVersion(e.target.value)}
              placeholder="blank = all versions"
              className="w-full px-2 py-1 text-sm border border-slate-300 rounded outline-none focus:ring-1 focus:ring-blue-400"
            />
          </Field>

          <Field
            label="Reactor"
            hint="Reactor kind whose reaction runs (a worker advertising this kind handles it). Its delivery config comes from the reactor kind's default providerconfig."
          >
            <input
              list="reactor-binding-reactors"
              value={reactor}
              onChange={(e) => setReactor(e.target.value)}
              placeholder="a reactor kind"
              className="w-full px-2 py-1 text-sm border border-slate-300 rounded outline-none focus:ring-1 focus:ring-blue-400"
            />
            <datalist id="reactor-binding-reactors">
              {reactorKinds.map((k) => (
                <option key={k} value={k} />
              ))}
            </datalist>
          </Field>

          <Field
            label="Reactor version (optional)"
            hint="Pin which web-API version of the reactor this binding runs. Blank = the reactor's highest published version (a reactor upgrade takes effect automatically); a value pins it so a later reactor publish never changes what this binding runs."
          >
            <input
              type="number"
              min={1}
              value={reactorVersion}
              onChange={(e) => setReactorVersion(e.target.value)}
              placeholder="blank = highest published"
              className="w-full px-2 py-1 text-sm border border-slate-300 rounded outline-none focus:ring-1 focus:ring-blue-400"
            />
          </Field>

          <Field
            label="Label match"
            hint="Optional JSON object the resource labels must contain. Blank = match all."
          >
            <input
              value={labelMatch}
              onChange={(e) => setLabelMatch(e.target.value)}
              placeholder='{"team":"alpha"}'
              className="w-full px-2 py-1 text-sm font-mono border border-slate-300 rounded outline-none focus:ring-1 focus:ring-blue-400"
            />
          </Field>

          <label className="flex items-center gap-2 text-sm text-slate-700">
            <input type="checkbox" checked={enabled} onChange={(e) => setEnabled(e.target.checked)} />
            Enabled (a disabled binding never fires; takes effect on the next claim)
          </label>

          {error && (
            <div className="p-2 bg-red-50 border border-red-200 rounded text-sm text-red-800">
              {error}
            </div>
          )}
        </div>

        <div className="flex items-center justify-end gap-2 p-4 border-t border-slate-200 bg-slate-50">
          <button
            onClick={onClose}
            disabled={mutate.isPending}
            className="text-sm px-3 py-1.5 rounded bg-white border border-slate-300 hover:bg-slate-100 text-slate-700 disabled:opacity-50"
          >
            Cancel
          </button>
          <button
            onClick={() => {
              setError(undefined)
              mutate.mutate()
            }}
            disabled={mutate.isPending || !canSave}
            className="text-sm px-3 py-1.5 rounded bg-blue-600 hover:bg-blue-500 text-white disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {mutate.isPending ? 'Saving…' : 'Save'}
          </button>
        </div>
      </div>
    </div>
  )
}

function Field({
  label,
  hint,
  children,
}: {
  label: string
  hint?: string
  children: React.ReactNode
}) {
  return (
    <div>
      <label className="text-xs font-medium text-slate-500 uppercase tracking-wider block mb-1">
        {label}
      </label>
      {children}
      {hint && <p className="text-xs text-slate-500 mt-1">{hint}</p>}
    </div>
  )
}
