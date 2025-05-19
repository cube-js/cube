const base = require('../../jest.base.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  rootDir: '.',
  transformIgnorePatterns: [
    'node_modules/(?!axios)/'
  ],
  moduleNameMapper: {
    ...base.moduleNameMapper,
    '^axios$': require.resolve('axios'),
  }
};
