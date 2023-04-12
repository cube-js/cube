const fromExports = require('./dist/src');
const { DuckDBDriver } = require('./dist/src/DuckDBDriver');

const toExport = DuckDBDriver;

// eslint-disable-next-line no-restricted-syntax
for (const [key, module] of Object.entries(fromExports)) {
  toExport[key] = module;
}

module.exports = toExport;
