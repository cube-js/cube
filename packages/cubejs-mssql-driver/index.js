const fromExports = require('./dist/src');
const { MSSqlDriver } = require('./dist/src/MSSqlDriver');

const toExport = MSSqlDriver;

// eslint-disable-next-line no-restricted-syntax
for (const [key, module] of Object.entries(fromExports)) {
  toExport[key] = module;
}

module.exports = toExport;
