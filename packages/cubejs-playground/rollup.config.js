import commonjs from '@rollup/plugin-commonjs';
import localResolve from 'rollup-plugin-local-resolve';
import postcss from 'rollup-plugin-postcss';
import typescript from 'rollup-plugin-typescript2';
import { uglify } from 'rollup-plugin-uglify';

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
          minimize: true,
        }),
        commonjs(),
        typescript({
          tsconfigOverride: {
            include: ['src/playground/**/*'],
          },
        }),
        localResolve(),
        uglify()
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
  input: './src/playground/index.ts',
  external: [
    'react',
    'react-dom',
    'react-router',
    'prop-types',
    'styled-components',
  ],
});
