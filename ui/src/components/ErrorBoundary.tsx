import { Component, type ErrorInfo, type ReactNode } from 'react'

interface Props {
  children: ReactNode
}

interface State {
  error: Error | null
}

// ErrorBoundary stops a single page's render-time throw from blanking the whole
// SPA. Without it, any uncaught error in a route component unmounts the entire
// React tree, leaving a blank screen that only a full reload recovers — the
// exact failure mode of the ['kinds'] query-key shape collision. Wrap <Routes>
// in this, keyed by pathname, so navigating to another route clears the error
// and re-mounts a fresh tree.
export class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null }

  static getDerivedStateFromError(error: Error): State {
    return { error }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    // Surface it in the console for debugging; the UI shows the recoverable card.
    console.error('UI render error:', error, info.componentStack)
  }

  render() {
    if (this.state.error) {
      return (
        <main className="flex-1 flex items-center justify-center p-6">
          <div className="max-w-lg w-full bg-white border border-red-200 rounded-lg p-4 space-y-3">
            <h2 className="text-base font-semibold text-red-800">Something went wrong on this page</h2>
            <pre className="bg-red-50 border border-red-200 rounded p-2 font-mono text-xs text-red-800 overflow-x-auto whitespace-pre-wrap">
              {this.state.error.message}
            </pre>
            <p className="text-sm text-slate-600">
              Switch to another tab in the rail, or reload the page. (Details are in the browser
              console.)
            </p>
            <button
              onClick={() => this.setState({ error: null })}
              className="text-sm px-3 py-1.5 rounded bg-blue-600 hover:bg-blue-500 text-white"
            >
              Try again
            </button>
          </div>
        </main>
      )
    }
    return this.props.children
  }
}
