const TerserPlugin = require('terser-webpack-plugin');
const webpack = require('webpack');
const { addLessLoader } = require('customize-cra');
const VARIABLES = require('./src/variables');

const LESS_VARIABLES = {};

// Create LESS variable map.
Object.keys(VARIABLES)
  .forEach((key) => {
    LESS_VARIABLES[`@${key}`] = VARIABLES[key];
  });

module.exports = function override(config, env) {
  config.optimization = {
    minimizer: [
      new TerserPlugin({
        cache: true,
        parallel: 2
      })
    ],
    splitChunks: {
      chunks: 'all',
      minSize: 30000,
      maxSize: 0,
      minChunks: 1,
      maxAsyncRequests: 5,
      maxInitialRequests: 3,
      automaticNameDelimiter: '~',
      name: true,
      cacheGroups: {
        babel: {
          test: /babel/,
          priority: -5
        },
        vendors: {
          test: /[\\/]node_modules[\\/]/,
          priority: -10
        },
        default: {
          priority: -20
        }
      }
    }
  };
  config.stats = 'verbose';
  config.plugins = config.plugins.concat([
    new webpack.ProgressPlugin()
  ]);
  if (env === 'production') {
    config.devtool = false;
  }
  config = addLessLoader({
    javascriptEnabled: true,
    modifyVars: LESS_VARIABLES,
  })(config);
  return config;
};
