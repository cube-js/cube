const { cancelCombinator } = require('../dist/src/driver/utils');

process.emitWarning(
  'Using absolute import with @cubejs-backend/query-orchestrator is deprecated',
  'DeprecationWarning'
);

module.exports = cancelCombinator;
