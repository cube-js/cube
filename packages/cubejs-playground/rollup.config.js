import commonjs from '@rollup/plugin-commonjs';
import localResolve from 'rollup-plugin-local-resolve';
import postcss from 'rollup-plugin-postcss';
import typescript from '@rollup/plugin-typescript';
import svg from 'rollup-plugin-svg';

import { LESS_VARIABLES } from './variables-esm';

export default [
  {
    input: './src/playground/index.ts',
    plugins: [
      postcss({
        extensions: ['.less', '.css'],
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
        minimize: true,
      }),
      typescript({
        tsconfig: 'tsconfig.json',
      }),
      commonjs(),
      localResolve(),
      svg(),
    ],
    output: {
      dir: 'lib',
      format: 'esm',
      sourcemap: true,
    },
  },
  {
    input: './src/cloud/index.ts',
    plugins: [
      typescript({
        tsconfig: 'tsconfig.json',
        declarationDir: 'lib/cloud'
      }),
      commonjs(),
      localResolve(),
      svg(),
    ],
    output: {
      dir: 'lib/cloud',
      format: 'esm',
      sourcemap: true,
    },
  },
  {
    input: './src/rollup-designer/index.ts',
    plugins: [
      typescript({
        tsconfig: 'tsconfig.json',
        declarationDir: 'lib/rollup-designer'
      }),
      commonjs(),
      localResolve(),
      svg(),
    ],
    output: {
      dir: 'lib/rollup-designer',
      format: 'esm',
      sourcemap: true,
    },
  },
];
