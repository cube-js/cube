import commonjs from '@rollup/plugin-commonjs';
import localResolve from 'rollup-plugin-local-resolve';
import postcss from 'rollup-plugin-postcss';
import typescript from '@rollup/plugin-typescript';
import svg from 'rollup-plugin-svg';

import { LESS_VARIABLES } from './variables-esm';

export default {
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
    format: 'es',
    sourcemap: true,
  },
};
