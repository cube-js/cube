import babel from '@rollup/plugin-babel';
import localResolve from 'rollup-plugin-local-resolve';
import postcss from 'rollup-plugin-postcss';

import { LESS_VARIABLES } from './src/variables-esm';

const bundle = (name, globalName, { globals = {}, ...baseConfig }) => {
  return [
    {
      ...baseConfig,
      plugins: [
        postcss({
          extensions: ['.less'],
          use: [
            [
              'less',
              {
                javascriptEnabled: true,
                modifyVars: LESS_VARIABLES,
              },
            ],
          ],
          extract: 'antd.min.css',
          minimize: true
        }),
        babel({
          extensions: ['.js', '.jsx', '.ts', '.tsx'],
          exclude: 'node_modules/**',
          babelHelpers: 'runtime',
          presets: [
            [
              '@babel/preset-react',
              {
                runtime: 'automatic',
              },
            ],
            '@babel/preset-typescript',
            [
              '@babel/preset-env',
              {
                shippedProposals: true,
                useBuiltIns: 'usage',
                corejs: 3,
              },
            ],
          ],
          plugins: [
            [
              '@babel/plugin-transform-runtime',
              {
                corejs: false,
                helpers: true,
                regenerator: true,
                useESModules: false,
              },
            ],
          ],
        }),
        localResolve(),
      ],
      output: {
        file: `./lib/${name}.esm.js`,
        format: 'es',
        sourcemap: true,
        globals,
      },
    },
  ];
};

export default bundle('cubejs-playground', 'cubejsPlayground', {
  input: './src/playground/public_api.js',
  external: [
    'react',
    'react-dom',
    'react-router',
    'prop-types',
    'styled-components',
  ],
});
