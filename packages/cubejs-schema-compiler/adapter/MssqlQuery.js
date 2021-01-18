const { MssqlQuery } = require('../dist/src/adapter/MssqlQuery');

process.emitWarning(
  'Using absolute import with @cubejs-backend/schema-compiler is deprecated',
  'DeprecationWarning'
);

module.exports = MssqlQuery;
