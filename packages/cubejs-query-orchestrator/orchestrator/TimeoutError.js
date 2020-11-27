const { TimeoutError } = require('../dist/src/orchestrator/TimeoutError');

process.emitWarning(
  'Using absolute import with @cubejs-backend/query-orchestrator is deprecated',
  'DeprecationWarning'
);

module.exports = TimeoutError;
