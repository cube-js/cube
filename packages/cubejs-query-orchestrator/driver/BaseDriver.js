const { BaseDriver } = require('../dist/src/driver/BaseDriver');

process.emitWarning(
  'Using absolute import with @cubejs-backend/query-orchestrator is deprecated',
  'DeprecationWarning'
);

module.exports = BaseDriver;
