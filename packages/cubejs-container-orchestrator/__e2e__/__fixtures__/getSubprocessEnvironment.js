// Grab NODE_ENV and FILTER_REGEXP environment variables and prepare them to be injected in the
// env of the suprocess.
const NO_MATCH_REGEXP = /.^/;

function getSubprocessEnvironment(ENV_FILTER_REGEXP = NO_MATCH_REGEXP, overrideEnv = {}) {
  const processEnv = Object.keys(process.env)
    .filter(key => ENV_FILTER_REGEXP.test(key))
    .reduce(
      (env, key) => {
        env[key] = process.env[key];
        return env;
      },
      {
        // Useful for determining whether we’re running in production mode.
        // Most importantly, it switches React into the correct mode.
        NODE_ENV: process.env.NODE_ENV || 'development',
        PATH: process.env.PATH,
        // Useful for resolving the correct path to static assets in `public`.
        // For example, <img src={process.env.PUBLIC_URL + '/img/logo.png'} />.
        // This should only be used as an escape hatch. Normally you would put
        // images into the `src` and `import` them in code to get their paths.
      }
    );
  return {
    ...processEnv,
    ...overrideEnv,
  };
}

module.exports = getSubprocessEnvironment;
