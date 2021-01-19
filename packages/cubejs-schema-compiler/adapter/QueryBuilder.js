const { QueryBuilder } = require('../dist/src/adapter/QueryBuilder');

process.emitWarning(
  'Using absolute import with @cubejs-backend/schema-compiler is deprecated',
  'DeprecationWarning'
);

module.exports = QueryBuilder;
