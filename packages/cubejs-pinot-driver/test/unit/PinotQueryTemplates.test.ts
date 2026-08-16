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

  // Pinot reads backslash as the LIKE escape character by default and rejects an
  // explicit ESCAPE clause - `LIKE '%\_%' ESCAPE '\'` fails at execution, while
  // the same pattern without the clause matches a literal underscore. So the
  // value must still be escaped (BaseQuery's `like_escape_char`), but the
  // statement must NOT carry a clause. Both halves are pinned here: dropping the
  // escaping would make `%` a wildcard again, and adding the clause back would
  // make every LIKE filter error.
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
      expect(sql).not.toContain('ESCAPE');
    }
  );
});
