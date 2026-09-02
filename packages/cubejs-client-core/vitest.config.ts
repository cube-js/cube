import path from 'path';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  resolve: {
    alias: [
      {
        // d3-format exports map ("./locale/*": "./locale/*.json") doubles the
        // .json extension when the import already includes it. Rewrite to the
        // actual file path so Vite skips the exports map resolution.
        find: /^d3-format\/locale\/(.+)\.json$/,
        replacement: path.resolve(__dirname, '../../node_modules/d3-format/locale/$1.json'),
      },
    ],
  },
  test: {
    globals: true,
    environment: 'jsdom',
    exclude: ['dist/**', 'node_modules/**'],
    coverage: {
      provider: 'v8',
      reportsDirectory: 'coverage',
      include: ['src/**/*.{ts,tsx}'],
      exclude: ['src/index.umd.ts', 'src/**/*.d.ts'],
    },
  },
});
