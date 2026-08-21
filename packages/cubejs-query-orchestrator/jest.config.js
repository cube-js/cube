const base = require('../../jest.base-ts.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  rootDir: '.',
  // The ts-jest base matches *.test.ts only; this package also has a .js suite.
  testMatch: ['<rootDir>/test/**/*.test.ts', '<rootDir>/test/**/*.test.js'],
  transform: {
    '^.+\\.[tj]s$': ['ts-jest', {
      tsconfig: '<rootDir>/tsconfig.jest.json',
    }],
  },
};
