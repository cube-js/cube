const TerserPlugin = require('terser-webpack-plugin');
const webpack = require('webpack');
const { addLessLoader } = require('customize-cra');

module.exports = function override(config, env) {
  config.optimization = {
    minimizer: [
      new TerserPlugin({
        terserOptions: {
          parse: {
            ecma: 8,
          },
          compress: {
            ecma: 5,
            warnings: false,
            comparisons: false,
            inline: 2,
            drop_console: true,
          },
          mangle: {
            safari10: true,
          },
          output: {
            ecma: 5,
            comments: false,
            ascii_only: true,
          },
        },
        parallel: 2,
        cache: true,
        sourceMap: false,
        extractComments: false,
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
