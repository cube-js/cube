import babel from 'rollup-plugin-babel';
import resolve from 'rollup-plugin-node-resolve';
import commonjs from 'rollup-plugin-commonjs';
import replace from 'rollup-plugin-replace';
import alias from 'rollup-plugin-alias';

const bundle = (name, globalName, baseConfig, umdConfig) => {
  baseConfig = {
    plugins: [
      replace({
        'process.env.CUBEJS_API_URL': `"${process.env.CUBEJS_API_URL || 'https://statsbot.co/cubejs-api/v1'}"`
      })
    ],
    ...baseConfig
  };

  const baseUmdConfig = {
    ...(umdConfig || baseConfig),
    plugins: [
      resolve({
        extensions: ['.ts', '.js', '.json'],
        mainFields: ['browser', 'module', 'main']
      }),
      ...baseConfig.plugins,
      commonjs({
        extensions: ['.js', '.ts']
      }),
      babel({
        exclude: ['node_modules/**', /\/core-js\//],
        runtimeHelpers: true,
        presets: [
          '@babel/preset-react',
          [
            '@babel/preset-env',
            {
              shippedProposals: true,
              useBuiltIns: 'usage',
              corejs: 3
            }
          ]
        ]
      }),
      alias({
        '@cubejs-client/core': '../cubejs-client-core/src/index.js'
      })
    ]
  };

  return [
    // browser-friendly UMD build
    {
      ...baseUmdConfig,
      output: [
        {
          file: `packages/${name}/dist/${name}.umd.js`,
          format: 'umd',
          name: globalName
        }
      ]
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
        ...baseConfig.plugins,
        babel({
          exclude: 'node_modules/**',
          runtimeHelpers: true,
          presets: [
            '@babel/preset-react',
            [
              '@babel/preset-env',
              {
                shippedProposals: true,
                useBuiltIns: 'usage',
                corejs: 3
              }
            ]
          ],
          plugins: [
            [
              '@babel/plugin-transform-runtime',
              {
                corejs: false,
                helpers: true,
                regenerator: true,
                useESModules: false
              }
            ]
          ]
        })
      ],
      output: [{ file: `packages/${name}/dist/${name}.js`, format: 'cjs' }]
    },
    // // ES module (for bundlers) build.
    {
      ...baseConfig,
      plugins: [
        ...baseConfig.plugins,
        babel({
          exclude: 'node_modules/**',
          runtimeHelpers: true,
          presets: [
            '@babel/preset-react',
            [
              '@babel/preset-env',
              {
                shippedProposals: true,
                useBuiltIns: 'usage',
                corejs: 3
              }
            ]
          ],
          plugins: [
            [
              '@babel/plugin-transform-runtime',
              {
                corejs: false,
                helpers: true,
                regenerator: true,
                useESModules: false
              }
            ]
          ]
        })
      ],
      output: [{ file: `packages/${name}/dist/${name}.esm.js`, format: 'es' }]
    }
  ];
};

export default bundle('cubejs-client-core', 'cubejs', {
  input: 'packages/cubejs-client-core/src/index.js',
}, {
  input: 'packages/cubejs-client-core/src/index.umd.js',
}).concat(bundle('cubejs-client-ws-transport', 'CubejsWebSocketTransport', {
  input: 'packages/cubejs-client-ws-transport/src/index.js',
})).concat(bundle('cubejs-client-react', 'cubejsReact', {
  input: 'packages/cubejs-client-react/src/index.js',
  external: [
    'react',
    'prop-types'
  ],
})).concat(bundle('cubejs-client-vue', 'cubejsVue', {
  input: 'packages/cubejs-client-vue/src/index.js',
  external: [
    'vue',
  ],
  globals: {
    vue: 'Vue',
  },
}));
