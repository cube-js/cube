module.exports = function override(config, env) {
  config.optimization = { minimize: false };
  return config;
};
