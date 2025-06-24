const base = require('../../jest.base.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  rootDir: '.',
  testMatch: [
    '<rootDir>/dist/test/*.{test,spec}.{ts,js}'
  ],
  snapshotResolver: '<rootDir>/test/snapshotResolver.js',
};
