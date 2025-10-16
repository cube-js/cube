const base = require('../../jest.base-ts.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  rootDir: '.',
  transform: {
    '^.+\\.ts$': ['ts-jest', {
      tsconfig: '<rootDir>/tsconfig.jest.json'
    }],
  },
};
