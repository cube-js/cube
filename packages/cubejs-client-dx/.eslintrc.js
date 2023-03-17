module.exports = {
  extends: 'airbnb-base',
  plugins: [
    'import'
  ],
  parser: 'babel-eslint',
  rules: {
    'max-classes-per-file': 0,
    'prefer-object-spread': 0,
    'import/no-unresolved': 0,
    'comma-dangle': 0,
    'no-console': 0,
    'arrow-parens': 0,
    'import/extensions': 0,
    quotes: ['warn', 'single'],
    'no-prototype-builtins': 0,
    'class-methods-use-this': 0,
    'no-param-reassign': 0,
    'no-mixed-operators': 0,
    'no-else-return': 0,
    'prefer-promise-reject-errors': 0,
    'no-plusplus': 0,
    'no-await-in-loop': 0,
    'operator-linebreak': 0,
    'max-len': ['error', 120, 2, {
      ignoreUrls: true,
      ignoreComments: false,
      ignoreRegExpLiterals: true,
      ignoreStrings: true,
      ignoreTemplateLiterals: true,
    }],
    'no-trailing-spaces': ['warn', { skipBlankLines: true }],
    'no-unused-vars': ['warn'],
    'object-curly-newline': 0
  },
  // env: {
  //   'jest/globals': true
  // }
};
