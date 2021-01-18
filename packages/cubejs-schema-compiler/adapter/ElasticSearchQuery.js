const { ElasticSearchQuery } = require('../dist/src/adapter/ElasticSearchQuery');

process.emitWarning(
  'Using absolute import with @cubejs-backend/schema-compiler is deprecated',
  'DeprecationWarning'
);

module.exports = ElasticSearchQuery;
