const { LocalQueueDriver } = require('../dist/src/orchestrator/LocalQueueDriver');

process.emitWarning(
  'Using absolute import with @cubejs-backend/query-orchestrator is deprecated',
  'DeprecationWarning'
);

module.exports = LocalQueueDriver;
