const base = require('./jest.base.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  preset: 'ts-jest',
  testMatch: ['<rootDir>/test/**/*.test.ts'],
  moduleFileExtensions: ['ts', 'js', 'json'],
  transform: {
    '^.+\\.ts$': ['ts-jest', { tsconfig: '../../tsconfig.jest.json' }],
  },
  collectCoverageFrom: [
    '<rootDir>/src/**/*.{ts,tsx}',
    '!<rootDir>/src/**/*.d.ts',
  ]
};
