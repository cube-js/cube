const path = require('path');
const CopyPlugin = require('copy-webpack-plugin');

module.exports = (config) => {
  if (!config.plugins) config.plugins = [];

  config.plugins.push(
    new CopyPlugin({
      patterns: [{ from: path.resolve('node_modules', '@chartshq/muze/dist') }],
    })
  );

  return config;
};
