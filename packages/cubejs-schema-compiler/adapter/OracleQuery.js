const { OracleQuery } = require('../dist/src/adapter/OracleQuery');

process.emitWarning(
  'Using absolute import with @cubejs-backend/schema-compiler is deprecated',
  'DeprecationWarning'
);

module.exports = OracleQuery;
