import { useEffect, useRef, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { api, type KindManifest } from '../api'

interface Props {
  open: boolean
  /** Prefill the editor from an existing manifest (edit), or undefined for a new kind. */
  current?: KindManifest
  onClose: () => void
  onApplied?: (manifestVersion: number) => void
}

// An empty CRD skeleton seeded into the editor for a new kind, so the operator
// has the shape to fill in rather than a blank box. Mirrors the resource-apply
// modal's load-or-paste-JSON flow — the whole CRD travels as one JSON document.
const SKELETON = `{
  "kind": "",
  "description": "",
  "spec_schema": {},
  "status_schema": {},
  "config_schema": {},
  "reactions": [
    { "name": "work", "trigger": "specChange", "emits": ["status"] }
  ],
  "finalizer_name": ""
}
`

// KindManifestModal applies a kind's CRD (KindManifest) from a JSON document —
// loaded from a file or pasted/edited inline, exactly like the resource Apply
// Manifest modal. The whole CRD (schemas + reactions + policy) is one JSON blob;
// there are no per-field pickers. Applying it (PUT /kinds/{kind}/manifest)
// registers the kind: the DB triggers derive its kind_config + reactor_bindings.
// An additive schema change is auto-allowed; a BREAKING one is gated 409 (it
// would invalidate stored specs), and the modal surfaces the server's reason and
// steers the operator to publish a new kind_version — there is no in-place override.
export function KindManifestModal({ open, current, onClose, onApplied }: Props) {
  if (!open) return null
  return (
    <KindManifestForm
      key={current?.kind ?? '__new__'}
      current={current}
      onClose={onClose}
      onApplied={onApplied}
    />
  )
}

function KindManifestForm({ current, onClose, onApplied }: Omit<Props, 'open'>) {
  const queryClient = useQueryClient()
  const editing = current !== undefined

  // The manifest JSON shown in the editor: the existing manifest (pretty) when
  // editing, else the skeleton. CRDs are small, so a single textarea is fine.
  const [text, setText] = useState(() =>
    current ? JSON.stringify(current, null, 2) + '\n' : SKELETON,
  )
  const [error, setError] = useState<string>()
  // A 409 from the publish gate means a BREAKING schema change to an existing
  // (kind, kind_version): there is no in-place override, so we surface the server's
  // reason and tell the operator to bump kind_version. Additive changes are
  // auto-allowed (200), so a 409 is always terminal here.
  const [breakingChange, setBreakingChange] = useState<string | null>(null)
  const [appliedVersion, setAppliedVersion] = useState<number | null>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)

  // parse turns the editor text into a KindManifest, throwing a friendly error
  // on malformed JSON or a missing kind. Light client-side checks only — the
  // server's ValidateManifest is authoritative.
  const parse = (): KindManifest => {
    let m: KindManifest
    try {
      m = JSON.parse(text)
    } catch (e) {
      throw new Error(`Manifest is not valid JSON — ${(e as Error).message}`, { cause: e })
    }
    if (!m || typeof m !== 'object') throw new Error('Manifest must be a JSON object.')
    if (!m.kind || String(m.kind).trim() === '') throw new Error('Manifest needs a non-empty "kind".')
    // Drop empty-string finalizer/description so they serialize as absent, and
    // empty schema objects stay as-is (the server treats {} as untyped).
    return m
  }

  const apply = useMutation({
    mutationFn: (m: KindManifest) => api.putKindManifest(m.kind, m),
    onSuccess: (res) => {
      queryClient.invalidateQueries({ queryKey: ['kind-schemas'] })
      queryClient.invalidateQueries({ queryKey: ['kind', res.kind] })
      queryClient.invalidateQueries({ queryKey: ['kind-manifest', res.kind] })
      queryClient.invalidateQueries({ queryKey: ['kind-manifests'] })
      queryClient.invalidateQueries({ queryKey: ['kinds'] })
      setBreakingChange(null)
      setAppliedVersion(res.manifest_version)
      onApplied?.(res.manifest_version)
    },
    onError: (err: Error) => {
      // 409 = a BREAKING schema change to an existing (kind, kind_version). There is
      // no override — surface the server's precise reason and steer the operator to
      // publish it as a new kind_version. (Additive changes return 200.)
      if (err.message.startsWith('409:')) {
        setBreakingChange(err.message.replace(/^409:\s*/, ''))
        setError(undefined)
        return
      }
      setError(err.message)
    },
  })

  const submit = () => {
    setError(undefined)
    setBreakingChange(null)
    setAppliedVersion(null)
    try {
      apply.mutate(parse())
    } catch (e) {
      setError((e as Error).message)
    }
  }

  const onFile = async (file: File) => {
    setError(undefined)
    setAppliedVersion(null)
    try {
      setText(await file.text())
    } catch (e) {
      setError(`could not read file: ${(e as Error).message}`)
    }
  }

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !apply.isPending) onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose, apply.isPending])

  return (
    <div
      className="fixed inset-0 bg-slate-900/40 backdrop-blur-sm flex items-center justify-center z-50 p-4"
      onClick={() => {
        if (!apply.isPending) onClose()
      }}
      role="presentation"
    >
      <div
        className="bg-white border border-slate-200 rounded-lg shadow-xl w-full max-w-2xl max-h-[90vh] flex flex-col"
        onClick={(e) => e.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-labelledby="kind-manifest-title"
      >
        <div className="flex items-center justify-between p-4 border-b border-slate-200">
          <h2 id="kind-manifest-title" className="text-base font-semibold text-slate-900">
            {editing ? (
              <>Edit CRD: <span className="font-mono">{current?.kind}</span></>
            ) : (
              'Apply a CRD'
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

        <div className="p-4 space-y-3 overflow-y-auto">
          <div className="flex items-center justify-between">
            <p className="text-xs text-slate-500">
              The CRD is one JSON document: <span className="font-mono">kind</span>,{' '}
              <span className="font-mono">kind_version</span> (the web-API version, ≥ 1), the
              spec/status/config JSON Schemas, and the declared{' '}
              <span className="font-mono">reactions</span> (each a{' '}
              <span className="font-mono">(trigger, emits)</span> pair). Load a file or edit inline.
            </p>
            <button
              onClick={() => fileInputRef.current?.click()}
              className="shrink-0 ml-3 text-xs px-2 py-1 rounded bg-slate-200 hover:bg-slate-300 text-slate-700"
            >
              Load from file…
            </button>
            <input
              ref={fileInputRef}
              type="file"
              accept=".json,application/json"
              className="hidden"
              onChange={(e) => {
                const f = e.target.files?.[0]
                if (f) onFile(f)
                e.target.value = '' // allow re-selecting the same file
              }}
            />
          </div>

          <textarea
            value={text}
            onChange={(e) => setText(e.target.value)}
            spellCheck={false}
            rows={20}
            className="w-full px-2 py-1.5 text-xs border border-slate-300 rounded font-mono outline-none focus:ring-1 focus:ring-blue-400 resize-y"
          />

          {appliedVersion !== null && (
            <div className="p-2 bg-emerald-50 border border-emerald-200 rounded text-sm text-emerald-800">
              Applied. manifest_version = <span className="font-mono">{appliedVersion}</span>. The kind
              is registered; the broker picks it up and its kind_config is derived. (Reactor kinds are
              wired to transitions separately, on the Reactor bindings page.)
            </div>
          )}

          {breakingChange && (
            <div className="p-3 bg-amber-50 border border-amber-300 rounded text-sm text-amber-900 space-y-2">
              <p className="font-medium">Breaking schema change — bump kind_version.</p>
              <p>
                This edit is backward-INCOMPATIBLE with the schema the existing resources of this{' '}
                <span className="font-mono">kind_version</span> were validated against, so it cannot be
                applied in place (there is no override). Publish it as a NEW{' '}
                <span className="font-mono">kind_version</span> instead — existing resources keep
                reconciling on the old version and can migrate over.
              </p>
              <p className="text-xs font-mono bg-amber-100 border border-amber-200 rounded p-2 whitespace-pre-wrap">
                {breakingChange}
              </p>
            </div>
          )}

          {error && (
            <div className="p-2 bg-red-50 border border-red-200 rounded text-sm text-red-800 whitespace-pre-wrap">
              {error}
            </div>
          )}
        </div>

        <div className="flex items-center justify-end gap-2 p-4 border-t border-slate-200 bg-slate-50">
          <button
            onClick={onClose}
            disabled={apply.isPending}
            className="text-sm px-3 py-1.5 rounded bg-white border border-slate-300 hover:bg-slate-100 text-slate-700 disabled:opacity-50"
          >
            Close
          </button>
          <button
            onClick={submit}
            disabled={apply.isPending}
            className="text-sm px-3 py-1.5 rounded bg-blue-600 hover:bg-blue-500 text-white disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {apply.isPending ? 'Applying…' : 'Apply CRD'}
          </button>
        </div>
      </div>
    </div>
  )
}
