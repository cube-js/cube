const { FileRepository } = require('../dist/src/core/FileRepository');

process.emitWarning(
  'Using absolute import with @cubejs-backend/server-core is deprecated',
  'DeprecationWarning'
);

module.exports = FileRepository;
