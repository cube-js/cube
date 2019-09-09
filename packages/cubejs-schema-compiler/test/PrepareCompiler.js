const PrepareCompiler = require('../compiler/PrepareCompiler');

exports.prepareCompiler = (content, options) => PrepareCompiler.prepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: "main.js", content }
  ])
}, { adapter: 'postgres', ...options });
