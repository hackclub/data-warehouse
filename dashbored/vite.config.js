import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'

export default defineConfig({
  plugins: [svelte()],
  base: '/static/vite/',
  build: {
    outDir: 'app/static/vite',
    emptyOutDir: true,
    manifest: true,
    rollupOptions: {
      input: 'frontend/entrypoints/application.js',
    },
  },
})
