import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// https://github.com/cube-js/cube/issues/6158
// A `case` dimension with an empty `when` list and only an `else` label passes
// validation, but renders `CASE ELSE 'Unknown' END`, which Postgres rejects with
// `syntax error at or near "ELSE"`. Either the model must fail validation, or the
// dimension must render valid SQL that evaluates to the `else` label.
describe('Issue #6158: zero-option case dimension', () => {
  jest.setTimeout(200000);

  const compilers = prepareJsCompiler(`
    cube('web_sessions', {
      sql: \`SELECT 1 AS id UNION ALL SELECT 2 AS id\`,
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

  it('is rejected at compile time or renders valid SQL', async () => {
    let compileError: Error | null = null;

    try {
      await compilers.compiler.compile();
    } catch (e: any) {
      compileError = e;
    }

    if (compileError) {
      // Rejecting the model is an acceptable fix, as long as it names the member.
      expect(compileError.message).toMatch(/area/);
      return;
    }

    const query = new PostgresQuery(compilers, {
      measures: ['web_sessions.count'],
      dimensions: ['web_sessions.area'],
      timezone: 'UTC',
    });

    const res = await dbRunner.testQuery(query.buildSqlAndParams());
    expect(res).toEqual([{ web_sessions__area: 'Unknown', web_sessions__count: '2' }]);
  });
});
