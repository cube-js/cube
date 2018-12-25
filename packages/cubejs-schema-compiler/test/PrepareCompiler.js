const PrepareCompiler = require('../compiler/PrepareCompiler');

exports.prepareCompiler = (content) => {
  return PrepareCompiler.prepareCompiler({
    dataSchemaFiles: () => Promise.resolve([
      { fileName: "main.js", content }
    ])
  }, { adapter: 'postgres' });
};