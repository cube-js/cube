const { QueryCache } = require('../dist/src/adapter/QueryCache');

process.emitWarning(
  'Using absolute import with @cubejs-backend/schema-compiler is deprecated',
  'DeprecationWarning'
);

module.exports = QueryCache;
