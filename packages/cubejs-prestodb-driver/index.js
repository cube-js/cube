const fromExports = require('./dist/src');
const { PrestoDriver } = require('./dist/src/PrestoDriver');

const toExport = PrestoDriver;

// eslint-disable-next-line no-restricted-syntax
for (const [key, module] of Object.entries(fromExports)) {
  toExport[key] = module;
}

module.exports = toExport;
