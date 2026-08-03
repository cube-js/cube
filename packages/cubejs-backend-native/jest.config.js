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
  // Bridge tests live under `dist/test/bridge/` and require a native module
  // built with `--features bridge-test-harness`. They are run separately via
  // `yarn test:bridge` (see jest-bridge.config.js); excluding them here keeps
  // `yarn unit` working with a regular debug build.
  testPathIgnorePatterns: ['/dist/test/bridge/'],
  snapshotResolver: '<rootDir>/test/snapshotResolver.js',
};
