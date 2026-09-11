import { svelte } from '@sveltejs/vite-plugin-svelte';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [svelte()],
  build: {
    outDir: 'dist',
    emptyOutDir: true
  },
  server: {
    proxy: {
      '/api': 'http://127.0.0.1:8080',
      '/graphql/ws': { target: 'ws://127.0.0.1:8080', ws: true },
      '/graphql': 'http://127.0.0.1:8080',
      '/http': 'http://127.0.0.1:8080',
      '/sse': 'http://127.0.0.1:8080',
      '/ws': { target: 'ws://127.0.0.1:8080', ws: true }
    }
  }
});
