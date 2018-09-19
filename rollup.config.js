import babel from "rollup-plugin-babel";
import babelrc from "babelrc-rollup";
import resolve from "rollup-plugin-node-resolve";
import commonjs from "rollup-plugin-commonjs";
import replace from "rollup-plugin-replace";
import pkg from "./package.json";
import uglify from "rollup-plugin-uglify";
import alias from 'rollup-plugin-alias';

const bundle = (name, globalName, baseConfig) => {
  baseConfig = {
    ...baseConfig,
    plugins: [
      replace({
        "process.env.CUBEJS_API_URL": `"${process.env.CUBEJS_API_URL || "https://statsbot.co/cubejs-api/v1"}"`
      }),
    ]
  };

  const baseUmdConfig = {
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
        ]
      }),
      resolve({
        module: true
      }),
      alias({
        'cubejs-client': 'src/index.js'
      }),
      commonjs()
    ]
  };

  return [
    // browser-friendly UMD build
    {
      ...baseUmdConfig,
      output: [
        {
          file: `dist/${name}.umd.js`,
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
      output: [{ file: `dist/${name}.js`, format: "es" }]
    }
  ]
};

export default bundle('cubejs-client', 'cubejs', {
  input: "src/index.js",
}).concat(bundle('cubejs-chartjs-client', 'cubejs', {
  input: "packages/cubejs-chartjs/src/index.js",
})).concat(bundle('cubejs-react', 'cubejsReact', {
  input: "packages/cubejs-react/src/index.js",
  external: [
    'react',
    'prop-types'
  ],
}));
