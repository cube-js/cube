const fromExports = require('./dist/src');
const { SapHanaDriver } = require('./dist/src/SapHanaDriver');

const toExport = SapHanaDriver;

// eslint-disable-next-line no-restricted-syntax
for (const [key, module] of Object.entries(fromExports)) {
  toExport[key] = module;
}

module.exports = toExport;
