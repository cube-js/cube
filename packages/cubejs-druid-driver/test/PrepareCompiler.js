import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';

export const prepareCompiler = (content, options) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'main.js', content }
  ])
}, { adapter: 'druid', ...options });
