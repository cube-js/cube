module.exports = {
  root: true,
  extends: 'airbnb-base',
  env: {
    node: true,
  },
  plugins: [
    'import',
    '@typescript-eslint/eslint-plugin',
  ],
  parser: '@typescript-eslint/parser',
  parserOptions: {
    sourceType: 'module',
    ecmaVersion: 2020,
    ecmaFeatures: {
      legacyDecorators: true
    },
  },
  rules: {
    'no-useless-constructor': 0,
    'max-classes-per-file': 0,
    'prefer-object-spread': 0,
    'import/no-unresolved': 0,
    'comma-dangle': 0,
    'no-console': 0,
    'arrow-parens': 0,
    'import/prefer-default-export': 0,
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
    'object-curly-newline': 0,
    // TypeScript Recommended
    'no-array-constructor': 'off',
    '@typescript-eslint/no-array-constructor': 'error',
    'no-empty-function': 'off',
    '@typescript-eslint/no-empty-function': 'error',
    'no-extra-semi': 'off',
    '@typescript-eslint/no-extra-semi': 'error',
    'no-unused-vars': 'off',
    '@typescript-eslint/no-unused-vars': 'warn',
    // '@typescript-eslint/no-var-requires': 'error',
    '@typescript-eslint/prefer-as-const': 'error',
    '@typescript-eslint/prefer-namespace-keyword': 'error',
    '@typescript-eslint/triple-slash-reference': 'error',
    '@typescript-eslint/type-annotation-spacing': 'error',
  },
  overrides: [
    {
      files: ['*.ts', '*.tsx'],
      rules: {
        // https://github.com/typescript-eslint/typescript-eslint/blob/master/packages/eslint-plugin/src/configs/eslint-recommended.ts
        'constructor-super': 'off', // ts(2335) & ts(2377)
        'getter-return': 'off', // ts(2378)
        'no-const-assign': 'off', // ts(2588)
        'no-dupe-args': 'off', // ts(2300)
        'no-dupe-class-members': 'off', // ts(2393) & ts(2300)
        'no-dupe-keys': 'off', // ts(1117)
        'no-func-assign': 'off', // ts(2539)
        'no-import-assign': 'off', // ts(2539) & ts(2540)
        'no-new-symbol': 'off', // ts(2588)
        'no-obj-calls': 'off', // ts(2349)
        'no-redeclare': 'off', // ts(2451)
        'no-setter-return': 'off', // ts(2408)
        'no-this-before-super': 'off', // ts(2376)
        'no-undef': 'off', // ts(2304)
        'no-unreachable': 'off', // ts(7027)
        'no-unsafe-negation': 'off', // ts(2365) & ts(2360) & ts(2358)
        'no-var': 'error', // ts transpiles let/const to var, so no need for vars any more
        'prefer-const': 'error', // ts provides better types with const
        'prefer-rest-params': 'error', // ts provides better types with rest args over arguments
        'prefer-spread': 'error', // ts transpiles spread to apply, so no need for manual apply
        'valid-typeof': 'off', // ts(2367)
        // TypeScript Additional
        '@typescript-eslint/explicit-member-accessibility': 'error',
        'no-shadow': 'off',
        '@typescript-eslint/no-shadow': ['error', { ignoreTypeValueShadow: true }],
        //
        'no-duplicate-imports': 'off',
        '@typescript-eslint/no-duplicate-imports': 'error',
        semi: 'off',
        '@typescript-eslint/semi': 'error',
      },
    },
  ],
};
