const base = require('../../jest.base.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  collectCoverage: false,
  testEnvironment: 'jsdom',
  watchman: false,
  transform: {
    '^.+\\.[jt]sx?$': 'babel-jest',
  },
  moduleNameMapper: {
    ...base.moduleNameMapper,
    '^@cubejs-client/core$': '<rootDir>/test/core-mock.ts',
  },
};
