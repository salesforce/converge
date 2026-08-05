import { useEffect, useState } from 'react'

// useDebounced returns `value` delayed by `ms` — it only updates after the input
// has stopped changing for that long. Used by the type-to-search pickers (owner
// filter, topology owner) so a request fires once the user pauses, not per key.
export function useDebounced<T>(value: T, ms: number): T {
  const [v, setV] = useState(value)
  useEffect(() => {
    const id = setTimeout(() => setV(value), ms)
    return () => clearTimeout(id)
  }, [value, ms])
  return v
}
