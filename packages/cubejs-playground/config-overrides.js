const webpack = require('webpack');
// const { addLessLoader } = require('customize-cra');
// const MiniCssExtractPlugin = require('mini-css-extract-plugin');
const rewireYarnWorkspaces = require('react-app-rewire-yarn-workspaces');

// const { LESS_VARIABLES } = require('./variables');

module.exports = function override(config, env) {
  config.stats = 'verbose';
  config.plugins = config.plugins.concat([
    new webpack.ProgressPlugin(),
    // new MiniCssExtractPlugin(),
  ]);
  if (env === 'production') {
    config.devtool = false;
  }
  // config = addLessLoader({
  //     lessOptions: {
  //       modifyVars: LESS_VARIABLES,
  //       javascriptEnabled: true,
  //     },
  // })(config);

  return rewireYarnWorkspaces(config, env);
};
