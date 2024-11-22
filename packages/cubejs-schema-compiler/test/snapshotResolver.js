const path = require('path');

function resolveSnapshotPath(testPath, snapshotExtension) {
  const testSourcePath = testPath.replace('dist/', '');
  const testDirectory = path.dirname(testSourcePath);
  const testFilename = path.basename(testSourcePath).replace('.js', '.ts');

  return `${testDirectory}/__snapshots__/${testFilename}${snapshotExtension}`;
}

function resolveTestPath(snapshotFilePath, snapshotExtension) {
  const testSourceFile = snapshotFilePath
    .replace('test/unit/__snapshots__', 'dist/test/unit')
    .replace('test/integration/__snapshots__', 'dist/test/integration')
    .replace('.ts', '.js')
    .replace(snapshotExtension, '');

  return testSourceFile;
}

module.exports = {
  resolveSnapshotPath,
  resolveTestPath,

  testPathForConsistencyCheck: 'dist/test/unit/Test.spec.js'
};
