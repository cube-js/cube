import babel from '@rollup/plugin-babel';
// import resolve from '@rollup/plugin-node-resolve';
// import commonjs from '@rollup/plugin-commonjs';
// import alias from '@rollup/plugin-alias';
import localResolve from 'rollup-plugin-local-resolve';
import css from 'rollup-plugin-postcss';

import { LESS_VARIABLES } from './src/variables-esm';

const bundle = (name, globalName, { globals = {}, ...baseConfig }) => {
  const cssLoader = css({
    extensions: ['.css', '.scss', '.less'],
    use: [
      [
        'less',
        {
          javascriptEnabled: true,
          modifyVars: LESS_VARIABLES,
        },
      ],
    ],
  });

  return [
    // CommonJS
    // {
    //   ...baseConfig,
    //   plugins: [
    //     cssLoader,
    //     babel({
    //       extensions: ['.js', '.jsx', '.ts', '.tsx'],
    //       exclude: 'node_modules/**',
    //       babelHelpers: 'runtime',
    //       presets: [
    //         [
    //           '@babel/preset-react',
    //           {
    //             runtime: 'automatic',
    //           },
    //         ],
    //         '@babel/preset-typescript',
    //         [
    //           '@babel/preset-env',
    //           {
    //             shippedProposals: true,
    //             useBuiltIns: 'usage',
    //             corejs: 3,
    //           },
    //         ],
    //       ],
    //       plugins: [
    //         [
    //           '@babel/plugin-transform-runtime',
    //           {
    //             corejs: false,
    //             helpers: true,
    //             regenerator: true,
    //             useESModules: false,
    //           },
    //         ],
    //       ],
    //     }),
    //     localResolve(),
    //     resolve(),
    //     commonjs({
    //       namedExports: {
    //         'node_modules/react-js/index.js': ['isValidElementType'],
    //       },
    //       include: [/node_modules/],
    //     }),
    //   ],
    //   output: [
    //     {
    //       file: `./lib/${name}.js`,
    //       format: 'cjs',
    //       sourcemap: true,
    //     },
    //   ],
    // },
    // ES module (for bundlers) build.
    {
      ...baseConfig,
      plugins: [
        cssLoader,
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
      output: [
        {
          file: `./lib/${name}.esm.js`,
          format: 'es',
          sourcemap: true,
          globals,
        },
      ],
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
