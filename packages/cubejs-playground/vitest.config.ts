import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'jsdom',
    exclude: ['dist/**', 'lib/**', 'build/**', 'node_modules/**', 'vizard/**'],
  },
});
