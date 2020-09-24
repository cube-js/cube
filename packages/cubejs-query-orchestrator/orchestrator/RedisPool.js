const { RedisPool } = require('../dist/src/orchestrator/RedisPool');

process.emitWarning(
  'Using absolute import with @cubejs-backend/query-orchestrator is deprecated',
  'DeprecationWarning'
);

module.exports = RedisPool;
