module.exports = {
  root: true,
  extends: 'react-app',
  plugins: ['react', 'jsx-a11y', 'import'],
  rules: {
    "react/jsx-uses-react": 0,
    'jsx-a11y/click-events-have-key-events': 0,
    'jsx-a11y/no-static-element-interactions': 0,
    'jsx-a11y/anchor-is-valid': 0,
    'react/jsx-no-bind': 0,
    'react/jsx-first-prop-new-line': 0,
    'react/jsx-indent-props': 0,
    'react/jsx-filename-extension': 0,
    'react/destructuring-assignment': 0,
    'react/react-in-jsx-scope': 0,
    'import/no-unresolved': 0,
    'comma-dangle': 0,
    'no-console': 0,
    'no-plusplus': 0,
    'import/prefer-default-export': 0,
    'import/no-named-as-default': 0,
    'import/no-named-as-default-member': 0,
    'arrow-parens': 0,
    'react/jsx-no-undef': 0,
    'react/jsx-tag-spacing': 0,
    'react/prefer-stateless-function': 0,
    'react/forbid-prop-types': 0,
    'react/prop-types': 0,
    'import/extensions': 0,
    quotes: 0,
    'no-prototype-builtins': 0,
    'class-methods-use-this': 0,
    'no-param-reassign': 0,
    'no-mixed-operators': 0,
    'no-else-return': 0,
    'max-len': [
      'error',
      120,
      2,
      {
        ignoreUrls: true,
        ignoreComments: true,
        ignoreRegExpLiterals: true,
        ignoreStrings: true,
        ignoreTemplateLiterals: true,
      }
    ],
    'no-trailing-spaces': ['error', { skipBlankLines: true }],
    'react/sort-comp': [
      1,
      {
        order: ['static-methods', 'lifecycle', 'everything-else', 'render']
      }
    ],
    'react/jsx-props-no-spreading': 0
  }
};
