import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';
import { PinotQuery } from '../../src/PinotQuery';

const prepareCompiler = (content: string) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'main.js', content }
  ])
}, { adapter: 'postgres' });

const FILTER_MODEL = `
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
`;

const buildFilter = async (operator: string, useNativeSqlPlanner: boolean) => {
  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(FILTER_MODEL);

  await compiler.compile();

  const query = new PinotQuery({ joinGraph, cubeEvaluator, compiler }, {
    measures: ['orders.count'],
    filters: [{ member: 'orders.status', operator, values: ['%'] }],
    useNativeSqlPlanner,
  });

  const [sql, params] = query.buildSqlAndParams();

  return { sql: sql.replace(/\s+/g, ' '), params };
};

// Pinot has no default LIKE escape character, so the value escaping both
// planners apply is only meaningful if the clause that interprets it is
// attached to the predicate - which is why these pin the whole predicate.
/* eslint-disable quotes -- double quotes keep the expected SQL readable */
const PREDICATES: [string, string, boolean, string][] = [
  ['contains', 'legacy', false, "LOWER(\"orders\".status) LIKE CONCAT('%', LOWER(?) , '%') ESCAPE '\\'"],
  ['notContains', 'legacy', false, "LOWER(\"orders\".status) NOT LIKE CONCAT('%', LOWER(?) , '%') ESCAPE '\\'"],
  ['startsWith', 'legacy', false, "LOWER(\"orders\".status) LIKE CONCAT('', LOWER(?) , '%') ESCAPE '\\'"],
  ['endsWith', 'legacy', false, "LOWER(\"orders\".status) LIKE CONCAT('%', LOWER(?) , '') ESCAPE '\\'"],
  ['contains', 'tesseract', true, "LOWER(\"orders\".status) LIKE CONCAT('%', LOWER(?), '%') ESCAPE '\\'"],
  ['notContains', 'tesseract', true, "LOWER(\"orders\".status) NOT LIKE CONCAT('%', LOWER(?), '%') ESCAPE '\\'"],
  ['startsWith', 'tesseract', true, "LOWER(\"orders\".status) LIKE CONCAT('', LOWER(?), '%') ESCAPE '\\'"],
  ['endsWith', 'tesseract', true, "LOWER(\"orders\".status) LIKE CONCAT('%', LOWER(?), '') ESCAPE '\\'"],
];
/* eslint-enable quotes */

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

  it.each(PREDICATES)(
    'escapes and interprets LIKE wildcards for %s on the %s planner',
    async (operator, _name, useNativeSqlPlanner, predicate) => {
      const { sql, params } = await buildFilter(operator, useNativeSqlPlanner);

      expect(params).toEqual(['\\%']);
      expect(sql).toContain(predicate);
    }
  );
});
