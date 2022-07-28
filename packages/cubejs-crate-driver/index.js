const fromExports = require('./dist/src');
const { CrateDriver } = require('./dist/src/CrateDriver');

const toExport = CrateDriver;

// eslint-disable-next-line no-restricted-syntax
for (const [key, module] of Object.entries(fromExports)) {
  toExport[key] = module;
}

module.exports = toExport;
