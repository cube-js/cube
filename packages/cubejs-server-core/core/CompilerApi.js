const { CompilerApi } = require('../dist/src/core/CompilerApi');

process.emitWarning(
  'Using absolute import with @cubejs-backend/server-core is deprecated',
  'DeprecationWarning'
);

module.exports = CompilerApi;
