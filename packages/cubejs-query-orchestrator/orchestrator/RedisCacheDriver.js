const { RedisCacheDriver } = require('../dist/src/orchestrator/RedisCacheDriver');

process.emitWarning(
  'Using absolute import with @cubejs-backend/query-orchestrator is deprecated',
  'DeprecationWarning'
);

module.exports = RedisCacheDriver;
