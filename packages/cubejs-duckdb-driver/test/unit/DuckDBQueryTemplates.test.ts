import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';
import { DuckDBQuery } from '../../src/DuckDBQuery';

const prepareCompiler = (content: string) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'main.js', content }
  ])
}, { adapter: 'postgres' });

describe('DuckDBQuery SQL templates', () => {
  // DuckDB has no default LIKE escape character - the `default_escape` gate on
  // its `expressions.like`/`ilike` templates is the repo's own record of that.
  // The native planner escapes `%`, `_` and `\` in the filter value (BaseQuery's
  // `like_escape_char`), so the statement has to carry the clause that
  // interprets that escaping; without one a user searching for a literal `%`
  // matches nothing instead of the rows containing a percent sign.
  it.each([['legacy', false], ['tesseract', true]])(
    'escapes LIKE wildcards in filter values on the %s planner',
    async (_name, useNativeSqlPlanner) => {
      const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(`
        cube('orders', {
          sql_table: 'orders',

          measures: {
            count: {
              type: 'count',
            },
          },

          dimensions: {
            id: {
              sql: 'id',
              type: 'number',
              primary_key: true,
            },
            status: {
              sql: 'status',
              type: 'string',
            },
          },
        });
      `);

      await compiler.compile();

      const query = new DuckDBQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders.count'],
        filters: [{ member: 'orders.status', operator: 'contains', values: ['%'] }],
        useNativeSqlPlanner,
      });

      const [sql, params] = query.buildSqlAndParams();

      expect(params).toEqual(['\\%']);

      // Only the native planner emits the clause: the legacy path relies on
      // DuckDB reading a bare backslash as the escape character, which is the
      // behaviour it has always had here.
      if (useNativeSqlPlanner) {
        // eslint-disable-next-line quotes -- double quotes keep the SQL readable
        expect(sql).toContain("ESCAPE '\\'");
      } else {
        expect(sql).not.toContain('ESCAPE');
      }
    }
  );
});
