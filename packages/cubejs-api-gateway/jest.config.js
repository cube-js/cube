const base = require('../../jest.base.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  rootDir: '.',
  // Tests run from compiled output: there is no transform, so the sources under
  // test/ cannot execute. Run `yarn tsc` first, and name a suite without its
  // extension — `jest date-parser` — since the runtime path is
  // dist/test/date-parser.test.js. `.spec.` is included because jest's default
  // testMatch accepted it, and pinning this must not quietly drop a suite.
  // test/test-collection.test.ts enforces that dist is what gets collected.
  testMatch: ['<rootDir>/dist/test/**/*.{test,spec}.js'],
  snapshotResolver: '<rootDir>/test/snapshotResolver.js',
};
