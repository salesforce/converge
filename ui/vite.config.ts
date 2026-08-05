import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      '/api': 'http://localhost:8080',
    },
  },
  build: {
    outDir: 'dist',
    // Do NOT let vite empty dist/ itself: its wipe also deletes the committed
    // dist/.gitkeep placeholder (which keeps `//go:embed dist/*` in ../embed.go
    // resolving on a fresh clone), so the tracked file would show up as deleted
    // after every build. The `just ui` recipe cleans dist/ deterministically
    // instead — everything except .gitkeep — so builds are still fresh (no stale
    // hashed assets) while the placeholder is never touched.
    emptyOutDir: false,
  },
})
