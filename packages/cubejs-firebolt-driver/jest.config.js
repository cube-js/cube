const base = require('../../jest.base.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  rootDir: '.',
  // Suites run from the compiled output — the sources under test/ have no
  // transform and cannot execute.
  testMatch: [
    '<rootDir>/dist/test/**/*.{test,spec}.{ts,js}'
  ],
  // test-env.js maps DRIVERS_TESTS_FIREBOLT_* onto the CUBEJS_DB_* names the
  // driver reads, so it must survive alongside the base setup file. It stays a
  // source path: tsc has no allowJs, so it is never emitted into dist/.
  setupFiles: [...base.setupFiles, '<rootDir>/test/test-env.js'],
};
