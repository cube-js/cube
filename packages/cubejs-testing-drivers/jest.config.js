const base = require('../../jest.base.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  rootDir: '.',
  testMatch: [
    '<rootDir>/dist/test/*.{test,spec}.{ts,js}'
  ],
  moduleNameMapper: {
    ...base.moduleNameMapper,
    '^axios$': require.resolve('axios'),
  },
  snapshotResolver: '<rootDir>/test/snapshotResolver.js',
};
