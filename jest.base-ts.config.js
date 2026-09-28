const base = require('./jest.base.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  preset: 'ts-jest',
  testMatch: ['<rootDir>/test/**/*.test.{ts,js}'],
  moduleFileExtensions: ['ts', 'js', 'json'],
  transform: {
    '^.+\\.ts$': ['ts-jest', { tsconfig: '../../tsconfig.jest.json' }],
    // Scoped to test/: sibling workspace packages resolve to their built dist and must stay untransformed.
    '/test/.+\\.js$': ['ts-jest', { tsconfig: '../../tsconfig.jest.json' }],
  },
  collectCoverageFrom: [
    '<rootDir>/src/**/*.{ts,tsx}',
    '!<rootDir>/src/**/*.d.ts',
  ]
};
