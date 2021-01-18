const { ParamAllocator } = require('../dist/src/adapter/ParamAllocator');

process.emitWarning(
  'Using absolute import with @cubejs-backend/schema-compiler is deprecated',
  'DeprecationWarning'
);

module.exports = ParamAllocator;
