import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { BrowserRouter } from 'react-router-dom'
import './index.css'
import App from './App'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Don't refetch when the tab regains focus — too noisy with our
      // 1.5–5 s refetchInterval on every screen.
      refetchOnWindowFocus: false,
      retry: 1,
      // Treat data as fresh for 30 s. Tab switches and re-mounts
      // (StrictMode dev double-mount, route navigation back to a
      // previously-visited screen) reuse cached data instantly while
      // any active refetchInterval still keeps it live in the
      // background. This is what eliminates the "real data → Loading
      // flash → real data" flicker.
      staleTime: 30_000,
      gcTime: 5 * 60_000,
    },
  },
})

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <BrowserRouter>
      <QueryClientProvider client={queryClient}>
        <App />
      </QueryClientProvider>
    </BrowserRouter>
  </StrictMode>,
)
