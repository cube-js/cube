const base = require('../../jest.base.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  rootDir: '.',
  // Suites run from the compiled output. This names every one the package has;
  // the `unit` / `integration` scripts each pass a path to select their own.
  testMatch: [
    '<rootDir>/dist/test/**/*.{test,spec}.{ts,js}'
  ],
};
