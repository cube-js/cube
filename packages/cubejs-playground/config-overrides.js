const TerserPlugin = require('terser-webpack-plugin');
const webpack = require('webpack');
const { addLessLoader } = require('customize-cra');
const MiniCssExtractPlugin = require('mini-css-extract-plugin');
const rewireYarnWorkspaces = require('react-app-rewire-yarn-workspaces');

const { LESS_VARIABLES } = require('./src/variables');

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
    new webpack.ProgressPlugin(),
    // to fix No module factory available for dependency type: CssDependency
    new MiniCssExtractPlugin(),
  ]);
  if (env === 'production') {
    config.devtool = false;
  }
  config = addLessLoader({
    lessOptions: {
      modifyVars: LESS_VARIABLES,
      javascriptEnabled: true,
    },
  })(config);

  return rewireYarnWorkspaces(config, env);
};
