import { useEffect, useRef, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { api, type ProviderConfig, type ProviderConfigManifest } from '../api'

interface Props {
  open: boolean
  onClose: () => void
  onApplied: (c: ProviderConfig) => void
  // When set, the modal prefills from an existing config: the editor is
  // pre-populated with its current manifest ({kind, name, is_default, spec}).
  // kind+name are the config's identity — a changed pair is rejected on apply
  // (the upsert is keyed by name); re-applying overwrites role + spec.
  applyTo?: ProviderConfig | null
}

// MAX_BUNDLE_BYTES mirrors the server's 10 MiB cap on the DECODED bundle, so an
// oversize file is rejected in the browser before a pointless multi-MB upload.
const MAX_BUNDLE_BYTES = 10 * 1024 * 1024

// configManifest renders a ProviderConfig as its apply document — the same
// {kind, name, kind_version, is_default, spec, data?} shape the editor accepts — so
// the prefill round-trips verbatim, INCLUDING the web-API version (`kind_version`,
// REQUIRED — no implicit v1 default) and the opaque bundle (base64 `data`) so
// re-applying an existing config doesn't silently drop them.
function configManifest(c: ProviderConfig): string {
  const m: ProviderConfigManifest = {
    kind: c.kind,
    name: c.name,
    // kind_version is REQUIRED and explicit (>= 1). A stored config always has one;
    // fall back to 1 only for a config read from an older server that omitted it.
    kind_version: c.kind_version && c.kind_version >= 1 ? c.kind_version : 1,
    is_default: c.is_default,
    spec: c.spec,
  }
  if (c.data) m.data = c.data
  return JSON.stringify(m, null, 2)
}

// ApplyConfigModal: load or paste a provider-config manifest
// ({kind, name, is_default, spec}), then Apply. Mirrors CreateResourceModal
// but for the providerconfigs table — kind/name/is_default travel inside the
// manifest (no separate pickers), so a config viewed in the UI re-applies
// as-is. Apply is a single create-or-update keyed by name: a new name creates,
// an existing one overwrites kind/role/spec. is_default omitted ⇒ false (a
// CUSTOM override); true makes it the kind's single live default (the server
// rejects a second default per kind with 409). The server validates the spec
// against the kind's config schema, so the modal makes no assumptions about
// spec shape — it works for ANY registered kind.
export function ApplyConfigModal({ open, onClose, onApplied, applyTo }: Props) {
  const queryClient = useQueryClient()
  // Prefill mode: opened from an existing config (kind+name locked).
  const prefill = !!applyTo
  const [manifestText, setManifestText] = useState('')
  const [error, setError] = useState<string | undefined>()
  const fileInputRef = useRef<HTMLInputElement>(null)
  // Guards against a rapid second file selection: only the latest onFile()
  // read is allowed to write state.
  const fileReadToken = useRef(0)

  // Reset on open. When prefilling, seed the editor from the config already in
  // hand (the detail view fetched it) — no extra round-trip needed since a
  // config's spec is small. Intentional sync from props on the open→true
  // edge; the single follow-up render is correct, so the cascading-render
  // rule is disabled for this deliberate case.
  /* eslint-disable react-hooks/set-state-in-effect */
  useEffect(() => {
    if (!open) return
    setError(undefined)
    setManifestText(prefill && applyTo ? configManifest(applyTo) : '')
  }, [open, prefill, applyTo])
  /* eslint-enable react-hooks/set-state-in-effect */

  const mutate = useMutation({
    mutationFn: async () => {
      let parsed: unknown
      try {
        parsed = JSON.parse(manifestText)
      } catch (err) {
        throw new Error(`invalid manifest JSON: ${(err as Error).message}`, { cause: err })
      }
      if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
        throw new Error('manifest must be a JSON object with kind, name, is_default and spec')
      }
      const m = parsed as Record<string, unknown>

      const kind = typeof m.kind === 'string' ? m.kind.trim() : ''
      const name = typeof m.name === 'string' ? m.name.trim() : ''
      if (!kind) throw new Error('manifest.kind is required (a string)')
      if (!name) throw new Error('manifest.name is required (a string)')
      // In prefill mode the upsert is keyed on the (fixed) name; a changed
      // identity would create/clobber a different config, so reject it loudly.
      if (prefill && applyTo) {
        if (kind !== applyTo.kind) {
          throw new Error(`manifest.kind "${kind}" can't change; this config is "${applyTo.kind}"`)
        }
        if (name !== applyTo.name) {
          throw new Error(`manifest.name "${name}" can't change; this config is "${applyTo.name}"`)
        }
      }

      if (!('spec' in m)) throw new Error('manifest.spec is required')

      // is_default: optional, defaults false. If present it must be a boolean.
      let isDefault = false
      if (m.is_default !== undefined && m.is_default !== null) {
        if (typeof m.is_default !== 'boolean') {
          throw new Error('manifest.is_default must be a boolean (true/false)')
        }
        isDefault = m.is_default
      }

      // kind_version: REQUIRED web-API version (v1, v2, … — any positive integer). No
      // implicit v1 default; the server rejects a missing/0 kind_version (422).
      // Default-ness and the config schema are per-(kind, kind_version); a custom config
      // attaches only to a resource of the same (kind, kind_version).
      if (
        m.kind_version === undefined ||
        m.kind_version === null ||
        typeof m.kind_version !== 'number' ||
        !Number.isInteger(m.kind_version) ||
        m.kind_version < 1
      ) {
        throw new Error('manifest.kind_version is required and must be a positive integer (v1, v2, …)')
      }
      const kind_version = m.kind_version

      // data: optional opaque bundle, base64 string. Validate the shape + size
      // (the server enforces the same 10 MiB decoded cap; base64 inflates ~4/3).
      let data: string | undefined
      if (m.data !== undefined && m.data !== null && m.data !== '') {
        if (typeof m.data !== 'string') {
          throw new Error('manifest.data must be a base64 string (the opaque bundle)')
        }
        if (m.data.length > Math.ceil(MAX_BUNDLE_BYTES / 3) * 4) {
          throw new Error('manifest.data exceeds the 10 MiB bundle limit')
        }
        data = m.data
      }

      const manifest: ProviderConfigManifest = { kind, name, kind_version, is_default: isDefault, spec: m.spec, data }
      return api.applyProviderConfig(manifest)
    },
    onSuccess: ({ config }) => {
      // Refresh the list and (if this was a prefilled re-apply) the deep-linked
      // detail so the new role/spec/updated_at render immediately.
      queryClient.invalidateQueries({ queryKey: ['provider-configs'] })
      queryClient.invalidateQueries({ queryKey: ['provider-config', config.name] })
      onApplied(config)
    },
    onError: (err: Error) => setError(err.message),
  })

  // Esc to close (but not mid-save — would lose data).
  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !mutate.isPending) onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [open, onClose, mutate.isPending])

  const onFile = async (file: File) => {
    setError(undefined)
    const myToken = ++fileReadToken.current
    try {
      const text = await file.text()
      if (myToken !== fileReadToken.current) return
      // Pretty-print on load so the editor is readable; fall back to raw if the
      // file isn't valid JSON (apply surfaces the parse error on submit).
      let display = text
      try {
        display = JSON.stringify(JSON.parse(text), null, 2)
      } catch {
        /* keep raw */
      }
      setManifestText(display)
    } catch (err) {
      if (myToken !== fileReadToken.current) return
      setError(`could not read file: ${(err as Error).message}`)
    }
  }

  if (!open) return null

  return (
    <div
      className="fixed inset-0 bg-slate-900/40 backdrop-blur-sm flex items-center justify-center z-50"
      onClick={() => {
        if (!mutate.isPending) onClose()
      }}
      role="presentation"
    >
      <div
        className="bg-white border border-slate-200 rounded-lg shadow-xl w-full max-w-2xl max-h-[90vh] flex flex-col"
        onClick={(e) => e.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-labelledby="config-modal-title"
      >
        <div className="flex items-center justify-between p-4 border-b border-slate-200">
          <h2 id="config-modal-title" className="text-base font-semibold text-slate-900">
            {prefill ? `Apply config: ${applyTo?.name}` : 'Apply config'}
          </h2>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-slate-700 text-2xl leading-none"
            aria-label="Close"
          >
            &times;
          </button>
        </div>

        <div className="flex-1 overflow-y-auto p-4 space-y-4">
          <div>
            <label className="text-xs font-medium text-slate-500 uppercase tracking-wider block mb-1">
              Config manifest (JSON)
            </label>
            <div className="flex items-center gap-2 mb-2">
              <button
                onClick={() => fileInputRef.current?.click()}
                className="text-xs px-2 py-1 rounded bg-slate-200 hover:bg-slate-300 text-slate-700"
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
                  if (f) void onFile(f)
                  e.target.value = '' // allow re-loading the same file
                }}
              />
              {manifestText && (
                <button
                  onClick={() => setManifestText('')}
                  className="ml-auto text-xs text-slate-500 hover:text-slate-800"
                  title="Clear the editor"
                >
                  clear
                </button>
              )}
            </div>
            <textarea
              value={manifestText}
              onChange={(e) => setManifestText(e.target.value)}
              placeholder={
                '{\n  "kind": "a-kind",\n  "name": "my-config",\n  "kind_version": 1,\n  "is_default": false,\n  "spec": {}\n}'
              }
              rows={16}
              className="w-full bg-slate-50 border border-slate-300 px-2 py-1.5 rounded font-mono text-xs text-slate-800 placeholder-slate-400 outline-none focus:ring-1 focus:ring-blue-500"
            />
            <p className="text-xs text-slate-500 mt-1">
              A full manifest:{' '}
              <span className="font-mono">{`{ "kind", "name", "kind_version", "is_default", "spec", "data"? }`}</span>.{' '}
              <span className="font-mono">kind_version</span> is required (the web-API version, ≥ 1); a config is
              per (kind, kind_version) and a custom one must match the resource it attaches to.{' '}
              <span className="font-mono">is_default</span> defaults to{' '}
              <span className="font-mono">false</span>; at most one default per (kind, kind_version).{' '}
              <span className="font-mono">data</span> (optional) is a base64 opaque bundle (a
              zip of <span className="font-mono">.star</span> files etc.), max 10 MiB decoded.
            </p>
          </div>

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
            disabled={mutate.isPending || !manifestText.trim()}
            className="text-sm px-3 py-1.5 rounded bg-blue-600 hover:bg-blue-500 text-white disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {mutate.isPending ? 'Applying…' : 'Apply config'}
          </button>
        </div>
      </div>
    </div>
  )
}
