const { AWSElasticSearchQuery } = require('../dist/src/adapter/AWSElasticSearchQuery');

process.emitWarning(
  'Using absolute import with @cubejs-backend/schema-compiler is deprecated',
  'DeprecationWarning'
);

module.exports = AWSElasticSearchQuery;
