/** @type {import('jest').Config} */
module.exports = {
  testEnvironment: 'node',
  collectCoverage: true,
  coverageDirectory: 'coverage/',
  coverageReporters: ['text', 'html', 'lcov'],
  coveragePathIgnorePatterns: ['.*\\.d\\.ts'],
  collectCoverageFrom: [
    'dist/src/**/*.js',
    'dist/src/**/*.ts',
  ],
  moduleDirectories: ['node_modules', '<rootDir>/node_modules'],
  moduleNameMapper: {
    // Force module uuid to resolve with the CJS entry point, because Jest does not support package.json.exports.
    // @See https://github.com/uuidjs/uuid/issues/451
    '^uuid$': require.resolve('uuid'),
    '^yaml$': require.resolve('yaml'),
    '^antlr4$': require.resolve('antlr4'),
  },
  setupFiles: ['../../jest.setup.js'],
  snapshotFormat: {
    escapeString: true, // To keep existing variant of snapshots
    printBasicPrototype: true
  }
};
