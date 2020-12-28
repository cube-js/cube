const { OrchestratorApi } = require('../dist/src/core/OrchestratorApi');

process.emitWarning(
  'Using absolute import with @cubejs-backend/server-core is deprecated',
  'DeprecationWarning'
);

module.exports = OrchestratorApi;
