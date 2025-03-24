import { parse } from '@babel/parser';
import babelGenerator from '@babel/generator';
import babelTraverse from '@babel/traverse';

import { prepareJsCompiler } from './PrepareCompiler';
import { ImportExportTranspiler } from '../../src/compiler/transpilers';
import { ErrorReporter } from '../../src/compiler/ErrorReporter';

describe('Transpilers', () => {
  it('CubeCheckDuplicatePropTranspiler', async () => {
    try {
      const { compiler } = prepareJsCompiler(`
        cube(\`Test\`, {
          sql: 'select * from test',
          dimensions: {
            test1: {
              sql: 'test_1',
              type: 'number'
            },
            'test1': {
              sql: 'test_1',
              type: 'number'
            },
            test2: {
              sql: 'test_2',
              type: 'number'
            },
          }
        })
      `);

      await compiler.compile();

      throw new Error('Compile should thrown an error');
    } catch (e: any) {
      expect(e.message).toMatch(/Duplicate property parsing test1/);
    }
  });

  it('CubePropContextTranspiler', async () => {
    const { compiler } = prepareJsCompiler(`
        let { securityContext } = COMPILE_CONTEXT;

        cube(\`Test\`, {
          sql_table: 'public.user_\${securityContext.tenantId}',
          dimensions: {}
        })
    `);

    await compiler.compile();
  });

  it('ImportExportTranspiler', async () => {
    const ieTranspiler = new ImportExportTranspiler();
    const errorsReport = new ErrorReporter();
    const code = `
      export const helperFunction = () => 'hello'
      export { helperFunction as alias }
      export default helperFunction
    `;
    const ast = parse(
      code,
      {
        sourceFilename: 'code.js',
        sourceType: 'module',
        plugins: ['objectRestSpread'],
      },
    );

    babelTraverse(ast, ieTranspiler.traverseObject(errorsReport));
    const content = babelGenerator(ast, {}, code).code;

    expect(content).toEqual(`const helperFunction = () => 'hello';
addExport({
  helperFunction: helperFunction
})
addExport({
  alias: helperFunction
});
setExport(helperFunction);`);

    errorsReport.throwIfAny(); // should not throw
  });
});
