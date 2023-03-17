const fromExports = require('./dist/src');
const { KsqlDriver } = require('./dist/src/KsqlDriver');

const toExport = KsqlDriver;

// eslint-disable-next-line no-restricted-syntax
for (const [key, module] of Object.entries(fromExports)) {
  toExport[key] = module;
}

module.exports = toExport;
