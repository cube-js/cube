const path = require('path');
const fs = require('fs');
const native = require('../dist/js');

// Ugly hack to ignore obsolete snapshots for fallback build
// https://github.com/jestjs/jest/issues/4898
// Runs once in the main process: doing this in snapshotResolver.js raced across
// jest workers and failed with EPERM/ENOENT on Windows.
module.exports = () => {
  if (native.isFallbackBuild()) {
    fs.rmSync(path.join(__dirname, '__snapshots__'), {
      recursive: true, force: true, maxRetries: 5, retryDelay: 100
    });
  }
};
