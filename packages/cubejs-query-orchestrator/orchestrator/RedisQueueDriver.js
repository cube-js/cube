const { RedisQueueDriver } = require('../dist/src/orchestrator/RedisQueueDriver');

process.emitWarning(
  'Using absolute import with @cubejs-backend/query-orchestrator is deprecated',
  'DeprecationWarning'
);

module.exports = RedisQueueDriver;
