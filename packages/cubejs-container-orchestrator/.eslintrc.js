const merge = require('deepmerge');
const eslintrcbase = require('../../.eslintrc.js');

module.exports = merge(
  {
    plugins: ["import", "jest", "promise", "prettier"],
    overrides: [
      {
        files: [
          "**/__mocks__/*.js",
          "**/__mockData__/*.js",
          "**/*.test.js",
          "**/*.spec.js"
        ],
        env: {
          "jest/globals": true
        },
        plugins: ["jest"],
        rules: {
          "jest/no-disabled-tests": "warn",
          "jest/no-focused-tests": "error",
          "jest/no-identical-title": "error",
          "jest/prefer-to-have-length": "warn",
          "jest/valid-expect": "error"
        }
      },
      {
        files: [
          "**/*.test.js",
          "scripts/*.js",
          "config/*.js"
        ],
        rules: {
          "import/no-extraneous-dependencies": "off",
        }
      }
    ]
  },
  eslintrcbase
);
