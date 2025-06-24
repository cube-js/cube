const base = require('../../jest.base.config');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  rootDir: '.',
  testMatch: [
    '**/__tests__/**/*.js?(x)',
    '**/?(*.)+(spec|test|integration).js?(x)'
  ]
};
