// For jest only.
// See https://stackoverflow.com/a/64223627 and https://stackoverflow.com/a/67227427
module.exports = {
  presets: ['@babel/preset-env'],
  env: {
    test: {
      plugins: ['@babel/plugin-transform-runtime']
    }
  }
};
