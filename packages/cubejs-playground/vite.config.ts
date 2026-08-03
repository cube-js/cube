import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig(({ mode }) => ({
  base: './',
  build: {
    outDir: 'build',
    target: 'es2020',
  },
  server: {
    port: 3080,
    proxy: {
      '^/playground/*': 'http://localhost:4000',
      '^/cubejs-api/*': 'http://localhost:4000',
    },
  },
  plugins: [
    react(),
  ],
  css: {
    preprocessorOptions: {
      less: {
        javascriptEnabled: true,
        additionalData: '@root-entry-name: default;',
      },
    },
  },
  define: {
    'process.env.SC_DISABLE_SPEEDY': JSON.stringify('false'),
    ...(mode === 'development' ? { global: {} } : {}),
  },
}));
