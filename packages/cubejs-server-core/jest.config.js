/** @type {import('jest').Config} */
module.exports = {
  testEnvironment: 'node',
  setupFilesAfterEnv: [
    '<rootDir>/dist/test/setup.js'
  ],
  collectCoverage: true,
  coverageReporters: [
    'text',
    'html',
    'lcov'
  ],
  coverageDirectory: 'coverage/',
  collectCoverageFrom: [
    'dist/src/**/*.js',
    'dist/src/**/*.ts'
  ],
  coveragePathIgnorePatterns: [
    '.*\\.d\\.ts'
  ],
  moduleNameMapper: {
    // Can not use jest config in package.json because of need of require.resolve(...)
    '^uuid$': require.resolve('uuid'),
    '^yaml$': require.resolve('yaml'),
  }
};
