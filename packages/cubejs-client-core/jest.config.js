const base = require('../../jest.base.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  rootDir: '.',
  collectCoverageFrom: [...base.collectCoverageFrom, '!dist/src/index.umd.js']
};
