const { QueryQueue } = require('../dist/src/orchestrator/QueryQueue');

process.emitWarning(
  'Using absolute import with @cubejs-backend/query-orchestrator is deprecated',
  'DeprecationWarning'
);

module.exports = QueryQueue;
