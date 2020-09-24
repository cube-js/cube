const { createRedisClient } = require('../dist/src/orchestrator/RedisFactory');

process.emitWarning(
  'Using absolute import with @cubejs-backend/query-orchestrator is deprecated',
  'DeprecationWarning'
);

module.exports = createRedisClient;
