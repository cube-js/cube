const fromExports = require('./dist/src');
const { DucksDBDriver } = require('./dist/src/DucksDBDriver');

const toExport = DucksDBDriver;

// eslint-disable-next-line no-restricted-syntax
for (const [key, module] of Object.entries(fromExports)) {
  toExport[key] = module;
}

module.exports = toExport;
