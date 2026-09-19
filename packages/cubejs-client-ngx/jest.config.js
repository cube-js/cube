const base = require('../../jest.base.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  preset: 'jest-preset-angular',
  collectCoverage: false,
  testEnvironment: 'jsdom',
  // Drops the base config's <rootDir>/node_modules, which resolves this
  // nohoisted package's ESM copies (e.g. ansi-styles) over the hoisted CJS ones.
  moduleDirectories: ['node_modules'],
  testMatch: ['<rootDir>/test/**/*.test.ts'],
  setupFilesAfterEnv: ['<rootDir>/test/setup-jest.ts'],
  transform: {
    '^.+\\.(ts|js|mjs|html|svg)$': ['jest-preset-angular', {
      tsconfig: '<rootDir>/tsconfig.spec.json',
    }],
  },
  moduleNameMapper: {
    ...base.moduleNameMapper,
    // src/index.ts does not re-export format.ts, so the ESM-only d3-format
    // never enters the graph.
    '^@cubejs-client/core$': '<rootDir>/../cubejs-client-core/src/index.ts',
  },
};
