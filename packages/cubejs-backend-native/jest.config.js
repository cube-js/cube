const base = require('../../jest.base.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  rootDir: '.',
  setupFilesAfterEnv: [
    '<rootDir>/dist/test/setup/index.js'
  ],
  roots: [
    '<rootDir>/dist/test/'
  ],
  snapshotResolver: '<rootDir>/test/snapshotResolver.js',
};
