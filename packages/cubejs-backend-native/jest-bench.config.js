const base = require('../../jest.base.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  rootDir: '.',
  testEnvironment: 'jest-bench/environment',
  testEnvironmentOptions: {
    testEnvironment: 'node',
    testEnvironmentOptions: {}
  },
  // Include default reporter for error reporting alongside jest-bench reporter
  reporters: ['default', 'jest-bench/reporter'],
  // Pick up *.bench.ts files
  testRegex: '\\.bench\\.(ts|tsx|js)$',
  roots: [
    '<rootDir>/dist/benchmarks/'
  ],
  snapshotResolver: '<rootDir>/test/snapshotResolver.js',
  // Coverage is not needed for benchmarks
  collectCoverage: false
};
