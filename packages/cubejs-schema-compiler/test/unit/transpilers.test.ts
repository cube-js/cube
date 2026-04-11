import { parse } from '@babel/parser';
import babelGenerator from '@babel/generator';
import babelTraverse from '@babel/traverse';

import { prepareJsCompiler } from './PrepareCompiler';
import { ImportExportTranspiler } from '../../src/compiler/transpilers';
import { ErrorReporter } from '../../src/compiler/ErrorReporter';
import { PostgresQuery } from '../../src';

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

  it('CubePropContextTranspiler with full path to userAttributes should work normally', async () => {
    const { cubeEvaluator, compiler } = prepareJsCompiler(`
        cube(\`Test\`, {
          sql: 'SELECT * FROM users',
          dimensions: {
            userId: {
              sql: \`userId\`,
              type: 'string'
            }
          },
          accessPolicy: [
            {
              role: \`*\`,
              rowLevel: {
                filters: [
                  {
                    member: \`userId\`,
                    operator: \`equals\`,
                    values: [ securityContext.cubeCloud.userAttributes.userId ]
                  }
                ]
              }
            }
          ]
        })
    `);

    await compiler.compile();

    const transpiledValues = cubeEvaluator.cubeFromPath('Test').accessPolicy?.[0].rowLevel?.filters?.[0].values;
    expect(transpiledValues.toString()).toMatch('securityContext.cubeCloud.userAttributes.userId');
  });

  it('CubePropContextTranspiler with full path to user_attributes should work normally', async () => {
    const { cubeEvaluator, compiler } = prepareJsCompiler(`
        cube(\`Test\`, {
          sql: 'SELECT * FROM users',
          dimensions: {
            userId: {
              sql: \`userId\`,
              type: 'string'
            }
          },
          accessPolicy: [
            {
              role: \`*\`,
              rowLevel: {
                filters: [
                  {
                    member: \`userId\`,
                    operator: \`equals\`,
                    values: [ securityContext.cubeCloud.user_attributes.userId ]
                  }
                ]
              }
            }
          ]
        })
    `);

    await compiler.compile();

    const transpiledValues = cubeEvaluator.cubeFromPath('Test').accessPolicy?.[0].rowLevel?.filters?.[0].values;
    expect(transpiledValues.toString()).toMatch('securityContext.cubeCloud.userAttributes.userId');
  });

  it('CubePropContextTranspiler with shorthand userAttributes should work normally', async () => {
    const { cubeEvaluator, compiler } = prepareJsCompiler(`
        cube(\`Test\`, {
          sql: 'SELECT * FROM users',
          dimensions: {
            userId: {
              sql: \`userId\`,
              type: 'string'
            }
          },
          accessPolicy: [
            {
              role: \`*\`,
              rowLevel: {
                filters: [
                  {
                    member: \`userId\`,
                    operator: \`equals\`,
                    values: [ userAttributes.userId ]
                  }
                ]
              }
            }
          ]
        })
    `);

    await compiler.compile();

    const transpiledValues = cubeEvaluator.cubeFromPath('Test').accessPolicy?.[0].rowLevel?.filters?.[0].values;
    expect(transpiledValues.toString()).toMatch('securityContext.cubeCloud.userAttributes.userId');
  });

  it('CubePropContextTranspiler with shorthand user_attributes should work normally', async () => {
    const { cubeEvaluator, compiler } = prepareJsCompiler(`
        cube(\`Test\`, {
          sql: 'SELECT * FROM users',
          dimensions: {
            userId: {
              sql: \`userId\`,
              type: 'string'
            }
          },
          accessPolicy: [
            {
              role: \`*\`,
              rowLevel: {
                filters: [
                  {
                    member: \`userId\`,
                    operator: \`equals\`,
                    values: [ user_attributes.userId ]
                  }
                ]
              }
            }
          ]
        })
    `);

    await compiler.compile();

    const transpiledValues = cubeEvaluator.cubeFromPath('Test').accessPolicy?.[0].rowLevel?.filters?.[0].values;
    expect(transpiledValues.toString()).toMatch('securityContext.cubeCloud.userAttributes.userId');
  });

  it('CubePropContextTranspiler with shorthand groups in values should transpile to securityContext.cubeCloud.groups', async () => {
    const { cubeEvaluator, compiler } = prepareJsCompiler(`
        cube(\`Test\`, {
          sql: 'SELECT * FROM users',
          dimensions: {
            userId: {
              sql: \`userId\`,
              type: 'string'
            }
          },
          accessPolicy: [
            {
              role: \`*\`,
              rowLevel: {
                filters: [
                  {
                    member: \`userId\`,
                    operator: \`equals\`,
                    values: [ groups ]
                  }
                ]
              }
            }
          ]
        })
    `);

    await compiler.compile();

    const transpiledValues = cubeEvaluator.cubeFromPath('Test').accessPolicy?.[0].rowLevel?.filters?.[0].values;
    expect(transpiledValues.toString()).toMatch('securityContext.cubeCloud.groups');
  });

  it('CubePropContextTranspiler with bare shorthand groups (no array wrap) should transpile to securityContext.cubeCloud.groups', async () => {
    const { cubeEvaluator, compiler } = prepareJsCompiler(`
        cube(\`Test\`, {
          sql: 'SELECT * FROM users',
          dimensions: {
            userId: {
              sql: \`userId\`,
              type: 'string'
            }
          },
          accessPolicy: [
            {
              role: \`*\`,
              rowLevel: {
                filters: [
                  {
                    member: \`userId\`,
                    operator: \`equals\`,
                    values: groups
                  }
                ]
              }
            }
          ]
        })
    `);

    await compiler.compile();

    const transpiledValues = cubeEvaluator.cubeFromPath('Test').accessPolicy?.[0].rowLevel?.filters?.[0].values;
    expect(transpiledValues.toString()).toMatch('securityContext.cubeCloud.groups');
  });

  it('CubePropContextTranspiler with shorthand groups member access should transpile to securityContext.cubeCloud.groups', async () => {
    const { cubeEvaluator, compiler } = prepareJsCompiler(`
        cube(\`Test\`, {
          sql: 'SELECT * FROM users',
          dimensions: {
            userId: {
              sql: \`userId\`,
              type: 'string'
            }
          },
          accessPolicy: [
            {
              role: \`*\`,
              rowLevel: {
                filters: [
                  {
                    member: \`userId\`,
                    operator: \`equals\`,
                    values: [ groups.someProperty ]
                  }
                ]
              }
            }
          ]
        })
    `);

    await compiler.compile();

    const transpiledValues = cubeEvaluator.cubeFromPath('Test').accessPolicy?.[0].rowLevel?.filters?.[0].values;
    expect(transpiledValues.toString()).toMatch('securityContext.cubeCloud.groups.someProperty');
  });

  it('CubePropContextTranspiler with full path to groups should work normally', async () => {
    const { cubeEvaluator, compiler } = prepareJsCompiler(`
        cube(\`Test\`, {
          sql: 'SELECT * FROM users',
          dimensions: {
            userId: {
              sql: \`userId\`,
              type: 'string'
            }
          },
          accessPolicy: [
            {
              role: \`*\`,
              rowLevel: {
                filters: [
                  {
                    member: \`userId\`,
                    operator: \`equals\`,
                    values: [ securityContext.cubeCloud.groups ]
                  }
                ]
              }
            }
          ]
        })
    `);

    await compiler.compile();

    const transpiledValues = cubeEvaluator.cubeFromPath('Test').accessPolicy?.[0].rowLevel?.filters?.[0].values;
    expect(transpiledValues.toString()).toMatch('securityContext.cubeCloud.groups');
  });

  it('CubePropContextTranspiler with groups shorthand in sql template should transpile to SECURITY_CONTEXT.cubeCloud.groups', async () => {
    const { cubeEvaluator, compiler } = prepareJsCompiler(`
        cube(\`Test\`, {
          sql: \`SELECT * FROM users WHERE tenant_id = \${groups}\`,
          dimensions: {
            userId: {
              sql: \`userId\`,
              type: 'string'
            }
          }
        })
    `);

    await compiler.compile();

    const transpiledSql = cubeEvaluator.cubeFromPath('Test').sql;
    expect(transpiledSql!.toString()).toMatch('SECURITY_CONTEXT.cubeCloud.groups');
  });

  it('CubePropContextTranspiler with userAttributes shorthand in dimension sql should transpile to SECURITY_CONTEXT', async () => {
    const { cubeEvaluator, compiler } = prepareJsCompiler(`
        cube(\`Test\`, {
          sql: 'SELECT * FROM users',
          dimensions: {
            userId: {
              sql: \`\${userAttributes.region}\`,
              type: 'string'
            }
          }
        })
    `);

    await compiler.compile();

    const transpiledSql = cubeEvaluator.cubeFromPath('Test').dimensions.userId.sql;
    expect(transpiledSql!.toString()).toMatch('SECURITY_CONTEXT.cubeCloud.userAttributes');
  });

  it('CubePropContextTranspiler with userAttributes shorthand in mask.sql should transpile to SECURITY_CONTEXT', async () => {
    const { cubeEvaluator, compiler } = prepareJsCompiler(`
        cube(\`Test\`, {
          sql: 'SELECT * FROM users',
          dimensions: {
            userId: {
              sql: \`userId\`,
              type: 'string'
            },
            masked_dim: {
              sql: \`price\`,
              type: 'number',
              mask: {
                sql: \`CAST(\${userAttributes.tenantId} AS INTEGER)\`,
              }
            }
          }
        })
    `);

    await compiler.compile();

    const transpiledMaskSql = (cubeEvaluator.cubeFromPath('Test').dimensions.masked_dim as any).mask.sql;
    expect(transpiledMaskSql!.toString()).toMatch('SECURITY_CONTEXT.cubeCloud.userAttributes');
  });

  it('CubePropContextTranspiler mask.sql with CUBE reference should resolve correctly', async () => {
    const compilers = prepareJsCompiler(`
        cube(\`Test\`, {
          sql_table: 'public.test',
          dimensions: {
            id: {
              sql: \`id\`,
              type: 'number',
              primary_key: true,
            },
            secret: {
              sql: \`secret_val\`,
              type: 'string',
              mask: {
                sql: \`CONCAT('***', RIGHT(CAST(\${CUBE}.secret_val AS TEXT), 2))\`,
              }
            }
          },
          measures: {
            count: { type: 'count' }
          }
        })
    `);

    await compilers.compiler.compile();

    const query = new PostgresQuery(
      compilers,
      {
        measures: ['Test.count'],
        dimensions: ['Test.secret'],
        maskedMembers: ['Test.secret'],
      }
    );
    const sql = query.buildSqlAndParams();
    expect(sql[0]).toContain('"test".secret_val');
  });

  it('CubePropContextTranspiler should not transform groups shorthand when a cube member named groups exists', async () => {
    const { cubeEvaluator, compiler } = prepareJsCompiler(`
        cube(\`Test\`, {
          sql: 'SELECT * FROM users',
          dimensions: {
            groups: {
              sql: \`groups_col\`,
              type: 'string'
            },
            filtered: {
              sql: \`\${groups}\`,
              type: 'string'
            }
          }
        })
    `);

    await compiler.compile();

    const transpiledSql = cubeEvaluator.cubeFromPath('Test').dimensions.filtered.sql;
    expect(transpiledSql!.toString()).not.toMatch('SECURITY_CONTEXT');
    expect(transpiledSql!.toString()).not.toMatch('securityContext');
    expect(transpiledSql!.toString()).toMatch('groups');
  });

  it('ImportExportTranspiler', async () => {
    const ieTranspiler = new ImportExportTranspiler();
    const errorsReport = new ErrorReporter();
    const code = `
      export const helperFunction = () => 'hello'
      export { helperFunction as alias }
      export default helperFunction
      export function requireFilterParam() {
        return 'required';
      }
      export const someVar = 42
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
});
addExport({
  alias: helperFunction
});
setExport(helperFunction);
function requireFilterParam() {
  return 'required';
}
addExport({
  requireFilterParam: requireFilterParam
});
const someVar = 42;
addExport({
  someVar: someVar
});`);

    errorsReport.throwIfAny(); // should not throw
  });
});
