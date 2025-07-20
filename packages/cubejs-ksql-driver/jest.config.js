const base = require('../../jest.base.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  rootDir: '.',
  moduleNameMapper: {
    ...base.moduleNameMapper,
    '^axios$': require.resolve('axios'),
  }
};
