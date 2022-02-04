const fromExports = require('./dist/src');
const { QuestDriver } = require('./dist/src/QuestDriver');

const toExport = QuestDriver;

// eslint-disable-next-line no-restricted-syntax
for (const [key, module] of Object.entries(fromExports)) {
  toExport[key] = module;
}

module.exports = toExport;
