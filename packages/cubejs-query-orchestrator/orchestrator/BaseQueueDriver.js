const { BaseQueueDriver } = require('../dist/src/orchestrator/BaseQueueDriver');

process.emitWarning(
  'Using absolute import with @cubejs-backend/query-orchestrator is deprecated',
  'DeprecationWarning'
);

module.exports = BaseQueueDriver;
