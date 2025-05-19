const base = require('../../jest.base.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  rootDir: '.',
  snapshotResolver: '<rootDir>/test/snapshotResolver.js',
};
