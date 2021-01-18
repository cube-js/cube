const { BaseQuery } = require('../dist/src/adapter/BaseQuery');

process.emitWarning(
  'Using absolute import with @cubejs-backend/schema-compiler is deprecated',
  'DeprecationWarning'
);

module.exports = BaseQuery;
