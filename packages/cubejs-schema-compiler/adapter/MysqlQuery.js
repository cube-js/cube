const { MysqlQuery } = require('../dist/src/adapter/MysqlQuery');

process.emitWarning(
  'Using absolute import with @cubejs-backend/schema-compiler is deprecated',
  'DeprecationWarning'
);

module.exports = MysqlQuery;
