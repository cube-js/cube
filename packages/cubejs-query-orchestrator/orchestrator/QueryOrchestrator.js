const { QueryOrchestrator } = require('../dist/src/orchestrator/QueryOrchestrator');

process.emitWarning(
  'Using absolute import with @cubejs-backend/query-orchestrator is deprecated',
  'DeprecationWarning'
);

module.exports = QueryOrchestrator;
