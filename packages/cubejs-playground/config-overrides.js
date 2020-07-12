const TerserPlugin = require('terser-webpack-plugin');
const webpack = require('webpack');
const { addLessLoader } = require('customize-cra');

module.exports = function override(config, env) {
  config.optimization = {
    minimizer: [
      new TerserPlugin({
        cache: true,
        parallel: true,
        chunkFilter: (chunk) => chunk.name.indexOf('babel') === -1 && chunk.name.indexOf('vendors') === -1
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
    javascriptEnabled: true
  })(config);
  return config;
};
