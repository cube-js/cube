/// <reference types="vitest" />
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import environmentPlugin from 'vite-plugin-environment';

export default defineConfig(({ mode }) => ({
  base: './',
  build: {
    outDir: 'build',
    target: 'es2018',
  },
  server: {
    port: 3080,
    proxy: {
      '^/playground/*': 'http://localhost:4000',
      '^/cubejs-api/*': 'http://localhost:4000',
    },
  },
  plugins: [
    environmentPlugin(
      {
        SC_DISABLE_SPEEDY: 'false',
      },
      { loadEnvFiles: true }
    ),
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
  define:
    mode === 'development'
      ? {
          global: {},
        }
      : undefined,
}));
