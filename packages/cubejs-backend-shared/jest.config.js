const base = require('../../jest.base-ts.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  rootDir: '.',
  // This package needs its own tsconfig for ts-jest — see tsconfig.jest.json.
  transform: {
    '^.+\\.ts$': ['ts-jest', { tsconfig: '<rootDir>/tsconfig.jest.json' }],
  },
};
