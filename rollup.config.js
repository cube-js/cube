import babel from '@rollup/plugin-babel';
import resolve from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';
import alias from '@rollup/plugin-alias';
import peerDepsExternal from 'rollup-plugin-peer-deps-external';

const extensions = ['.js', '.jsx', '.ts', '.tsx'];
const basePlugins = [
  resolve({
    extensions,
    mainFields: ['browser', 'module', 'main'],
  }),
  commonjs({
    include: /node_modules/,
  }),
  babel({
    extensions,
    exclude: '**/node_modules/**',
    babelHelpers: 'runtime',
    presets: [
      '@babel/preset-react',
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
];

const bundle = (name, globalName, { globals = {}, ...baseConfig }, umdConfig) => {
  const baseUmdConfig = {
    ...(umdConfig || baseConfig),
    plugins: [
      ...basePlugins,
      alias({
        entries: {
          '@cubejs-client/core': '../cubejs-client-core/src/index.js',
        },
      }),
    ],
  };

  return [
    // browser-friendly UMD build
    {
      ...baseUmdConfig,
      output: [
        {
          file: `packages/${name}/dist/${name}.umd.js`,
          format: 'umd',
          name: globalName,
          exports: 'named',
          sourcemap: true,
          globals,
        },
      ],
    },

    // minified browser-friendly UMD build
    /* {
      ...BASE_UMD_CONFIG,
      output: [{ file: pkg.browserMin, format: "umd", name: "cubejs" }],
      plugins: [...BASE_UMD_CONFIG.plugins, uglify()]
    }, */

    // // ES module (for bundlers) build.
    {
      ...baseConfig,
      plugins: [
        peerDepsExternal(),
        ...basePlugins,
      ],
      output: [
        {
          file: `packages/${name}/dist/${name}.js`,
          format: 'cjs',
          exports: 'named',
          sourcemap: true,
        }
      ],
    },
    // // ES module (for bundlers) build.
    {
      ...baseConfig,
      plugins: [
        peerDepsExternal(),
        ...basePlugins,
      ],
      output: [
        {
          file: `packages/${name}/dist/${name}.esm.js`,
          format: 'es',
          sourcemap: true,
          globals,
        },
      ],
    },
  ];
};

export default bundle(
  'cubejs-client-core',
  'cubejs',
  {
    input: 'packages/cubejs-client-core/src/index.js',
  },
  {
    input: 'packages/cubejs-client-core/src/index.umd.js',
  }
)
  .concat(
    bundle('cubejs-client-ws-transport', 'CubejsWebSocketTransport', {
      input: 'packages/cubejs-client-ws-transport/src/index.ts',
    })
  )
  .concat(
    bundle('cubejs-client-react', 'cubejsReact', {
      input: 'packages/cubejs-client-react/src/index.ts',
      external: ['react', 'prop-types'],
      globals: {
        react: 'React',
      },
    })
  )
  .concat(
    bundle('cubejs-client-vue', 'cubejsVue', {
      input: 'packages/cubejs-client-vue/src/index.js',
      external: ['vue'],
      globals: {
        vue: 'Vue',
      },
    })
  )
  .concat(
    bundle('cubejs-client-vue3', 'cubejsVue3', {
      input: 'packages/cubejs-client-vue3/src/index.js',
      external: ['vue'],
      globals: {
        vue: 'Vue',
      },
    })
  );
