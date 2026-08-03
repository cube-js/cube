const fromExports = require('./dist/src');
const { PinotDriver } = require('./dist/src/PinotDriver');

const toExport = PinotDriver;

// eslint-disable-next-line no-restricted-syntax
for (const [key, module] of Object.entries(fromExports)) {
  toExport[key] = module;
}

module.exports = toExport;
