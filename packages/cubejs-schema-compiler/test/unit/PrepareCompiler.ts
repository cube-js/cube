import { prepareCompiler as originalPrepareCompiler } from '../../src/compiler/PrepareCompiler';

export type CompileContent = {
  content: string;
  fileName: string;
};

export const prepareCompiler = (content: CompileContent | CompileContent[], options = {}) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve(Array.isArray(content) ? content : [content]),
}, { adapter: 'postgres', ...options });

export const prepareJsCompiler = (content, options = {}) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'main.js', content: Array.isArray(content) ? content.join('\r\n') : content }
  ])
}, { adapter: 'postgres', ...options });

export const prepareYamlCompiler = (content, options = {}) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'main.yml', content }
  ])
}, { adapter: 'postgres', ...options });

export const prepareCube = (cubeName, cube, options = {}) => {
  const fileName = `${cubeName}.js`;
  const content = `cube(${JSON.stringify(cubeName)}, ${JSON.stringify(cube).replace(/"([^"]+)":/g, '$1:')});`;

  return originalPrepareCompiler({
    localPath: () => __dirname,
    dataSchemaFiles: () => Promise.resolve([{ fileName, content }])
  }, { adapter: 'postgres', ...options });
};
