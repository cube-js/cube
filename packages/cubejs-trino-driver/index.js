const fromExports = require('./dist/src');
const { TrinoDriver } = require('./dist/src/TrinoDriver');

const toExport = TrinoDriver;

// eslint-disable-next-line no-restricted-syntax
for (const [key, module] of Object.entries(fromExports)) {
  toExport[key] = module;
}

module.exports = toExport;
