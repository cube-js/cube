import babel from "rollup-plugin-babel";
import babelrc from "babelrc-rollup";
import resolve from "rollup-plugin-node-resolve";
import commonjs from "rollup-plugin-commonjs";
import replace from "rollup-plugin-replace";
import pkg from "./package.json";
import uglify from "rollup-plugin-uglify";
import alias from 'rollup-plugin-alias';
import typescript from 'rollup-plugin-typescript';

const bundle = (name, globalName, baseConfig) => {
  baseConfig = {
    plugins: [
      replace({
        "process.env.CUBEJS_API_URL": `"${process.env.CUBEJS_API_URL || "https://statsbot.co/cubejs-api/v1"}"`
      })
    ],
    ...baseConfig
  };

  const baseUmdConfig = {
    ...baseConfig,
    plugins: [
      resolve({
        extensions: [ '.ts', '.js', '.json' ],
        mainFields: ['browser', 'module', 'main']
      }),
      ...baseConfig.plugins,
      commonjs({
        extensions: ['.js', '.ts']
      }),
      babel({
        exclude: ['node_modules/**', /\/core-js\//],
        runtimeHelpers: true,
        "presets": [
          '@babel/preset-react',
          [
            "@babel/preset-env",
            {
              shippedProposals: true,
              "useBuiltIns": "usage"
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
          format: "umd",
          name: globalName
        }
      ]
    },

    // minified browser-friendly UMD build
    /*{
      ...BASE_UMD_CONFIG,
      output: [{ file: pkg.browserMin, format: "umd", name: "cubejs" }],
      plugins: [...BASE_UMD_CONFIG.plugins, uglify()]
    },*/

    // // ES module (for bundlers) build.
    {
      ...baseConfig,
      plugins: [
        ...baseConfig.plugins,
        babel({
          exclude: 'node_modules/**',
          runtimeHelpers: true,
          "presets": [
            '@babel/preset-react',
            [
              "@babel/preset-env",
              {
                shippedProposals: true,
                "useBuiltIns": "usage"
              }
            ]
          ],
          "plugins": [
            [
              "@babel/plugin-transform-runtime",
              {
                "corejs": false,
                "helpers": true,
                "regenerator": true,
                "useESModules": false
              }
            ]
          ]
        })
      ],
      output: [{ file: `packages/${name}/dist/${name}.js`, format: "cjs" }]
    },
    // // ES module (for bundlers) build.
    {
      ...baseConfig,
      plugins: [
        ...baseConfig.plugins,
        babel({
          exclude: 'node_modules/**',
          runtimeHelpers: true,
          "presets": [
            '@babel/preset-react',
            [
              "@babel/preset-env",
              {
                shippedProposals: true,
                "useBuiltIns": "usage"
              }
            ]
          ],
          "plugins": [
            [
              "@babel/plugin-transform-runtime",
              {
                "corejs": false,
                "helpers": true,
                "regenerator": true,
                "useESModules": false
              }
            ]
          ]
        })
      ],
      output: [{ file: `packages/${name}/dist/${name}.esm.js`, format: "es" }]
    }
  ]
};

export default bundle('cubejs-client-core', 'cubejs', {
  input: "packages/cubejs-client-core/src/index.js",
}).concat(bundle('cubejs-react', 'cubejsReact', {
  input: "packages/cubejs-react/src/index.js",
  external: [
    'react',
    'prop-types'
  ],
})).concat(bundle('cubejs-vue', 'cubejsVue', {
  input: "packages/cubejs-vue/src/index.js",
  external: [
    'vue',
  ],
  globals: {
    vue: 'Vue',
  },
})).concat(bundle('cubejs-client-ngx', 'cubejsngx', {
  input: "packages/cubejs-client-ngx/index.ts",
  external: [
  ],
  plugins: [
    typescript({
      tsconfig: 'packages/cubejs-client-ngx/tsconfig.json',
      typescript: require('typescript')
    })
  ]
}));
