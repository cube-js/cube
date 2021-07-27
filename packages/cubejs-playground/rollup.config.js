import commonjs from '@rollup/plugin-commonjs';
import localResolve from 'rollup-plugin-local-resolve';
import postcss from 'rollup-plugin-postcss';
import typescript from '@rollup/plugin-typescript';
import svg from 'rollup-plugin-svg';

import { LESS_VARIABLES } from './variables-esm';

const bundle = (name, globalName, { globals = {}, ...baseConfig }) => {
  return {
    ...baseConfig,
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
      commonjs(),
      typescript({
        tsconfig: 'tsconfig.json',
      }),
      localResolve(),
      svg(),
    ],
    output: {
      dir: 'lib',
      format: 'es',
      sourcemap: true,
      globals,
    },
  };
};

export default bundle('cubejs-playground', 'cubejsPlayground', {
  input: './src/playground/index.ts'
});
