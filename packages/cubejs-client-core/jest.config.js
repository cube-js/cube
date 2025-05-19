const base = require('../../jest.base-ts.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  rootDir: '.',
  collectCoverageFrom: [
    ...base.collectCoverageFrom,
    '!<rootDir>/src/index.umd.ts',
  ],
};
