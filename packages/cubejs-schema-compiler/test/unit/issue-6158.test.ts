import { prepareJsCompiler } from './PrepareCompiler';
import { PostgresQuery } from '../../src/adapter/PostgresQuery';

// https://github.com/cube-js/cube/issues/6158
// A `case` dimension with an empty `when` list compiles, but produces
// `CASE ELSE 'Unknown' END`, which is invalid SQL.
describe('Issue #6158: zero-option case dimension', () => {
  it('either fails compilation or generates valid SQL', async () => {
    const { compiler, cubeEvaluator, joinGraph } = prepareJsCompiler(`
      cube('web_sessions', {
        sql: 'SELECT 1 AS id',
        measures: {
          count: { type: 'count' },
        },
        dimensions: {
          id: { sql: 'id', type: 'number', primaryKey: true },
          area: {
            case: {
              when: [],
              else: { label: 'Unknown' },
            },
            type: 'string',
          },
        },
      });
    `);

    let compileError: any = null;

    try {
      await compiler.compile();
      compiler.throwIfAnyErrors();
    } catch (e) {
      compileError = e;
    }

    if (compileError) {
      // Rejecting the dimension at compile time is an acceptable fix
      return;
    }

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      dimensions: ['web_sessions.area'],
      measures: ['web_sessions.count'],
    });
    const [sql] = query.buildSqlAndParams();
    expect(sql).not.toMatch(/CASE\s+ELSE/i);
  });
});
