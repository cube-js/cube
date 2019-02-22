module.exports = {
  "extends": "airbnb-base",
  "plugins": [
    "import"
  ],
  "rules": {
    "import/no-unresolved": 0,
    "comma-dangle": 0,
    "no-console": 0,
    "arrow-parens": 0,
    "import/extensions": 0,
    "quotes": 0,
    "no-prototype-builtins": 0,
    "class-methods-use-this": 0,
    "no-param-reassign": 0,
    "no-mixed-operators": 0,
    "no-else-return": 0,
    "prefer-promise-reject-errors": 0,
    "operator-linebreak": ["error", "after"],
    'max-len': ['error', 120, 2, {
      ignoreUrls: true,
      ignoreComments: false,
      ignoreRegExpLiterals: true,
      ignoreStrings: true,
      ignoreTemplateLiterals: true,
    }]
  }
};
