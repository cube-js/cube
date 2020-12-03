const { QueryCache } = require('../dist/src/orchestrator/QueryCache');

process.emitWarning(
  'Using absolute import with @cubejs-backend/query-orchestrator is deprecated',
  'DeprecationWarning'
);

module.exports = QueryCache;
