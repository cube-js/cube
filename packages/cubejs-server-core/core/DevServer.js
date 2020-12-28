const { DevServer } = require('../dist/src/core/DevServer');

process.emitWarning(
  'Using absolute import with @cubejs-backend/server-core is deprecated',
  'DeprecationWarning'
);

module.exports = DevServer;
