const base = require('../../jest.base.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  preset: 'jest-preset-angular',
  collectCoverage: false,
  testEnvironment: 'jsdom',
  // The base config adds <rootDir>/node_modules to the lookup, which makes every
  // module -- including jest's own -- resolve here first. This package is in the
  // root nohoist list, so that shadows e.g. the hoisted CJS ansi-styles with the
  // ESM copy the Angular CLI brings in. Plain node resolution already finds both.
  moduleDirectories: ['node_modules'],
  testMatch: ['<rootDir>/test/**/*.test.ts'],
  setupFilesAfterEnv: ['<rootDir>/test/setup-jest.ts'],
  transform: {
    '^.+\\.(ts|js|mjs)$': ['jest-preset-angular', {
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
