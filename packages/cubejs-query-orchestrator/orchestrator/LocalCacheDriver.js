const { LocalCacheDriver } = require('../dist/src/orchestrator/LocalCacheDriver');

process.emitWarning(
  'Using absolute import with @cubejs-backend/query-orchestrator is deprecated',
  'DeprecationWarning'
);

module.exports = LocalCacheDriver;
