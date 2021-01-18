import { prepareCompiler as originalPrepareCompiler } from '../../src/compiler/PrepareCompiler';

export const prepareCompiler = (content, options) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'main.js', content }
  ])
}, { adapter: 'postgres', ...options });
