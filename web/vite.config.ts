import { svelte } from '@sveltejs/vite-plugin-svelte';
import { defineConfig } from 'vite';

const backend = 'http://127.0.0.1:8080';
const httpFixtureProxy = Object.fromEntries(
  [
    '/openapi',
    '/anything',
    '/get',
    '/post',
    '/put',
    '/patch',
    '/delete',
    '/status',
    '/headers',
    '/ip',
    '/user-agent',
    '/delay',
    '/redirect',
    '/bytes',
    '/stream-bytes',
    '/gzip',
    '/deflate',
    '/basic-auth',
    '/image',
    '/video',
    '/audio',
    '/response-headers',
    '/cookies',
    '/cache',
    '/etag',
    '/redirect-to',
    '/json',
    '/html',
    '/xml',
    '/encoding',
    '/range',
    '/drip',
    '/unstable',
    '/bearer'
  ].map((path) => [path, backend])
);

export default defineConfig({
  plugins: [svelte()],
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    rollupOptions: {
      input: {
        app: 'index.html',
        openapi: 'openapi.html'
      }
    }
  },
  server: {
    proxy: {
      '/api': backend,
      '/assets': backend,
      '/graphql/ws': { target: 'ws://127.0.0.1:8080', ws: true },
      '/graphql': backend,
      ...httpFixtureProxy,
      '/sse': backend,
      '/ws': { target: 'ws://127.0.0.1:8080', ws: true }
    }
  }
});
