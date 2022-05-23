const fromExports = require('./dist/src');
const { FireboltDriver } = require('./dist/src/FireboltDriver');

const toExport = FireboltDriver;

// eslint-disable-next-line no-restricted-syntax
for (const [key, module] of Object.entries(fromExports)) {
  toExport[key] = module;
}

module.exports = toExport;
