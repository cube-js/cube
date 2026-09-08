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
};
