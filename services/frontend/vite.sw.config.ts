import { defineConfig } from 'vite';

// Push service worker: one classic IIFE file at a stable, unhashed URL. Module workers are not
// supported everywhere, and the page registers this exact path (src/push/client.ts).
export default defineConfig({
  publicDir: false,
  define: { 'process.env.NODE_ENV': JSON.stringify('production') },
  build: {
    emptyOutDir: false,
    copyPublicDir: false,
    lib: {
      entry: 'src/push/sw.ts',
      formats: ['iife'],
      name: 'bnpbPushWorker',
      fileName: () => 'firebase-messaging-sw.js',
    },
  },
});
