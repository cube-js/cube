import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';
import { PinotQuery } from '../../src/PinotQuery';

const prepareCompiler = (content: string) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'main.js', content }
  ])
}, { adapter: 'postgres' });

describe('PinotQuery SQL templates', () => {
  it('renders Tesseract sql_table queries with a prepared FROM source', async () => {
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
        },
      });
    `);

    await compiler.compile();

    const query = new PinotQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['orders.count'],
      timeDimensions: [],
      filters: [],
      rowLimit: 10,
      offset: 5,
      useNativeSqlPlanner: true,
    });

    const [sql] = query.buildSqlAndParams();

    expect(sql).toMatch(/FROM\s+orders\b/);
    expect(sql).not.toMatch(/FROM\s*\(\s*\)\s+AS\b/);
    expect(sql.indexOf('LIMIT 10')).toBeGreaterThan(-1);
    // Pinot expects LIMIT before OFFSET.
    expect(sql.indexOf('LIMIT 10')).toBeLessThan(sql.indexOf('OFFSET 5'));
  });

  // Pinot has no default LIKE escape character. The native planner escapes `%`,
  // `_` and `\` in the filter value (BaseQuery's `like_escape_char`), so the
  // statement has to carry the clause that interprets that escaping - otherwise
  // a user searching for a literal `%` gets a wildcard and matches every row.
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

      const query = new PinotQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders.count'],
        filters: [{ member: 'orders.status', operator: 'contains', values: ['%'] }],
        useNativeSqlPlanner,
      });

      const [sql, params] = query.buildSqlAndParams();

      expect(params).toEqual(['\\%']);
      // eslint-disable-next-line quotes -- double quotes keep the SQL readable
      expect(sql).toContain("ESCAPE '\\'");
    }
  );
});
