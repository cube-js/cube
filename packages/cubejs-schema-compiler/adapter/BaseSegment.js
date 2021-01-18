const { BaseSegment } = require('../dist/src/adapter/BaseSegment');

process.emitWarning(
  'Using absolute import with @cubejs-backend/schema-compiler is deprecated',
  'DeprecationWarning'
);

module.exports = BaseSegment;
