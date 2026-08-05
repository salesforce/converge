// Filter model + URL serialization for the Resources page.
//
// Kept in its own module (not in FilterBar.tsx) so the component file only
// exports components — that's what keeps React Fast Refresh working. The
// FilterBar component and the ResourcesPage both import from here.

// Filter is the abstract shape of one filter chip. The Resources page
// translates these into URL search params and into API filter calls.
//
// dim: 'owner'      value: owner "kind/name" ref, displayLabel: owner name
// dim: 'owner_kind' value: a root kind name (registry-defined)
// dim: 'kindver'    value: a (kind, version) pair "kind/N" (e.g. "vpc/2"); the
//                   Resources filter's registered kind/version chips. Each is an
//                   EXACT (kind, kind_version) match; the server prunes by kind first
//                   (index) then matches the pair. Serialized as repeatable ?kv=.
// dim: 'phase'      value: 'Ready' | 'Reconciling' | 'Degraded' | 'Failed' | 'Deleting'
// dim: 'name'       value: substring; rendered "name~foo"
// dim: 'label'      value: "k=v" pair
export type FilterDim = 'owner' | 'owner_kind' | 'kindver' | 'phase' | 'name' | 'label'

export interface Filter {
  dim: FilterDim
  value: string
  // displayLabel is what shows in the chip — for owners, the human
  // name resolved against the roots list.
  displayLabel?: string
}

// ─── URL serialization helpers ─────────────────────────────────────────

// filtersFromURL rebuilds filter chips from the URL. Owner chips carry the
// owner's "kind/name" ref as their value; the name is right there in the ref,
// so the chip renders it directly (no lazy id→name resolve needed).
export function filtersFromURL(params: URLSearchParams): Filter[] {
  const out: Filter[] = []
  for (const ref of params.getAll('owner')) {
    // displayLabel = the name portion of the ref, so a URL-restored chip
    // reads as the owner's name rather than the raw kind/name string.
    out.push({ dim: 'owner', value: ref, displayLabel: ref.slice(ref.indexOf('/') + 1) })
  }
  for (const k of params.getAll('owner_kind')) {
    out.push({ dim: 'owner_kind', value: k })
  }
  // Registered (kind, version) chips — repeatable ?kv=kind/N. Each is one exact
  // (kind, kind_version) pair; the server AND-matches them.
  for (const kv of params.getAll('kv')) {
    out.push({ dim: 'kindver', value: kv })
  }
  for (const r of params.getAll('phase')) {
    out.push({ dim: 'phase', value: r })
  }
  const name = params.get('name')
  if (name) out.push({ dim: 'name', value: name })
  for (const lv of params.getAll('label')) {
    out.push({ dim: 'label', value: lv })
  }
  return out
}

export function filtersToURL(filters: Filter[]): URLSearchParams {
  const out = new URLSearchParams()
  for (const f of filters) {
    switch (f.dim) {
      case 'owner':
        out.append('owner', f.value)
        break
      case 'owner_kind':
        out.append('owner_kind', f.value)
        break
      case 'kindver':
        out.append('kv', f.value)
        break
      case 'phase':
        out.append('phase', f.value)
        break
      case 'name':
        out.set('name', f.value)
        break
      case 'label':
        out.append('label', f.value)
        break
    }
  }
  return out
}
