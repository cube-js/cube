const base = require('../../jest.base.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  preset: 'ts-jest',
  rootDir: '.',
  testMatch: ['<rootDir>/test/**/*.test.ts'],
  moduleFileExtensions: ['ts', 'js', 'json'],
  transform: {
    '^.+\\.ts$': ['ts-jest', { tsconfig: 'tsconfig.jest.json' }],
  },
  collectCoverageFrom: [
    ...base.collectCoverageFrom,
    '!<rootDir>/dist/index.umd.ts',
    '!<rootDir>/dist/index.cjs.ts',
  ],
};
