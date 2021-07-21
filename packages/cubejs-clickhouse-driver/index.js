const fromExports = require('./dist/src');
const { ClickHouseDriver } = require('./dist/src/ClickHouseDriver');

/**
 * After 5 years working with TypeScript, now I know
 * that commonjs and nodejs require is not compatibility with using export default
 */
const toExport = ClickHouseDriver;

// eslint-disable-next-line no-restricted-syntax
for (const [key, module] of Object.entries(fromExports)) {
  toExport[key] = module;
}

module.exports = toExport;
