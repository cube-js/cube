const { ContinueWaitError } = require('../dist/src/orchestrator/ContinueWaitError');

process.emitWarning(
  'Using absolute import with @cubejs-backend/query-orchestrator is deprecated',
  'DeprecationWarning'
);

module.exports = ContinueWaitError;
