const base = require('../../jest.base.config');

// Dedicated config for the Tesseract bridge regression test harness.
//
// Intentionally does NOT use `test/snapshotResolver.js` from the main jest
// config — that resolver wipes the entire `test/__snapshots__/` directory
// when running on a non-python build (see `isFallbackBuild()` check there).
// Bridge tests don't use snapshots, but the side effect would still nuke
// unrelated snapshots in this package.

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  rootDir: '.',
  roots: [
    '<rootDir>/dist/test/bridge/'
  ],
  collectCoverage: false,
};
