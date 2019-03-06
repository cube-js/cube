const TerserPlugin = require('terser-webpack-plugin');

module.exports = function override(config, env) {
  config.optimization.minimizer = [new TerserPlugin({
    parallel: true,
    terserOptions: {
      ecma: 6,
    },
    exclude: /@babel/
  })];
  return config;
};