const { PostgresQuery } = require('../dist/src/adapter/PostgresQuery');

process.emitWarning(
  'Using absolute import with @cubejs-backend/schema-compiler is deprecated',
  'DeprecationWarning'
);

module.exports = PostgresQuery;
