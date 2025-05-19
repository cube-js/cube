const base = require('../../jest.base.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  rootDir: '.',
  setupFilesAfterEnv: [
    '<rootDir>/dist/test/setup.js'
  ],
};
