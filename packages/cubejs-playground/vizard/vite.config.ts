import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import environmentPlugin from 'vite-plugin-environment';

// https://vitejs.dev/config/
export default defineConfig({
  base: '/vizard/',
  build: {
    outDir: 'build',
    target: ['chrome104', 'safari14', 'firefox103'],
    rollupOptions: {
      output: {
        manualChunks: {
          react: ['react', 'react-dom'],
          other: ['react-router-dom', 'styled-components'],
          icons: ['@ant-design/icons'],
          clients: ['@cubejs-client/core', '@cubejs-client/react'],
          uikit: ['@cube-dev/ui-kit'],
          monaco: ['monaco-editor'],
        },
      },
    },
  },
  plugins: [
    react(),
    environmentPlugin(
      {
        SC_DISABLE_SPEEDY: 'false',
        NODE_ENV: process.env.NODE_ENV,
      },
      { loadEnvFiles: true }
    ),
    {
      name: 'configure-response-headers',
      configureServer: (server) => {
        server.middlewares.use((req, res, next) => {
          res.setHeader('Cross-Origin-Embedder-Policy', 'require-corp');
          res.setHeader('Cross-Origin-Opener-Policy', 'same-origin');
          next();
        });
      },
      configurePreviewServer: (server) => {
        server.middlewares.use((req, res, next) => {
          res.setHeader('Cross-Origin-Embedder-Policy', 'require-corp');
          res.setHeader('Cross-Origin-Opener-Policy', 'same-origin');
          next();
        });
      },
    },
  ],
});
