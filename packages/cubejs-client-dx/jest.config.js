const base = require('../../jest.base.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  rootDir: '.',
  transform: {
    '^.+\\.js$': 'babel-jest',
  },
};
