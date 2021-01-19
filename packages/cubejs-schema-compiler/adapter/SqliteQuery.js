const { SqliteQuery } = require('../dist/src/adapter/SqliteQuery');

process.emitWarning(
  'Using absolute import with @cubejs-backend/schema-compiler is deprecated',
  'DeprecationWarning'
);

module.exports = SqliteQuery;
