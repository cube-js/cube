const fromExports = require('./dist/src');
const { AthenaDriver } = require('./dist/src/AthenaDriver');

/**
 * commonjs and nodejs require is not compatible with using export default
 */
const toExport = AthenaDriver;

// eslint-disable-next-line no-restricted-syntax
for (const [key, module] of Object.entries(fromExports)) {
  toExport[key] = module;
}

module.exports = toExport;
