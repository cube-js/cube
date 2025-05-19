const base = require('../../jest.base.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  rootDir: '.',
  setupFiles: [
    './test/test-env.js'
  ],
  transformIgnorePatterns: [
    'node_modules/(?!axios)/'
  ],
  moduleNameMapper: {
    ...base.moduleNameMapper,
    '^axios$': require.resolve('axios'),
  }
};
