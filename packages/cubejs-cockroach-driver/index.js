const fromExports = require('./dist/src');
const { CockroachDriver } = require('./dist/src/CockroachDriver');

/**
 * After 5 years working with TypeScript, now I know
 * that commonjs and nodejs require is not compatibility with using export default
 */
const toExport = CockroachDriver;

// eslint-disable-next-line no-restricted-syntax
for (const [key, module] of Object.entries(fromExports)) {
  toExport[key] = module;
}

module.exports = toExport;
