const path = require('path');

function resolveSnapshotPath(testPath, snapshotExtension) {
  const testSourcePath = testPath.replace('dist/', '');
  const testDirectory = path.dirname(testSourcePath);
  const testFilename = path.basename(testSourcePath).replace('.js', '.ts');

  return `${testDirectory}/__snapshots__/${testFilename}${snapshotExtension}`;
}

function resolveTestPath(snapshotFilePath, snapshotExtension) {
  const testSourceFile = snapshotFilePath
    .replace('test/__snapshots__', 'dist/test')
    .replace('.ts', '.js')
    .replace(snapshotExtension, '');

  return testSourceFile;
}

module.exports = {
  resolveSnapshotPath,
  resolveTestPath,

  testPathForConsistencyCheck: 'dist/test/Test.spec.js'
};
