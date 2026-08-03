const base = require('../../jest.base.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  rootDir: '.',
  collectCoverageFrom: [
    ...base.collectCoverageFrom,
    '!dist/src/parser/GenericSql*.js',
    '!dist/src/parser/Python3*.js'
  ],
  globalSetup: '<rootDir>/dist/test/global-setup.js',
  snapshotResolver: '<rootDir>/test/snapshotResolver.js',
  transformIgnorePatterns: [
    '/node_modules/(?!node-fetch).+\\.js$',
    'node_modules/(?!axios)/'
  ],
  moduleNameMapper: {
    ...base.moduleNameMapper,
    '^yaml$': require.resolve('yaml'), // Because we have `yaml` as direct dependency here we need to resolve it relative to this package
    '^axios$': require.resolve('axios'),
  }
};
