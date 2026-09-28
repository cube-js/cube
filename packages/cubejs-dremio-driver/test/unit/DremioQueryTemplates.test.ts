import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';

const DremioQuery = require('../../../driver/DremioQuery');

const prepareCompiler = (content: string) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([{ fileName: 'main.js', content }]),
}, { adapter: 'postgres' });

const MODEL = `
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

const buildFilter = async (operator: string) => {
  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(MODEL);

  await compiler.compile();

  const query = new DremioQuery({ joinGraph, cubeEvaluator, compiler }, {
    measures: ['orders.count'],
    filters: [{ member: 'orders.status', operator, values: ['%'] }],
    useNativeSqlPlanner: true,
  });

  const [sql, params] = query.buildSqlAndParams();

  return { sql: sql.replace(/\s+/g, ' '), params };
};

// Dremio has no default LIKE escape character, so escaping the value is only
// meaningful if the clause that interprets it is attached to the predicate -
// which is why these pin the whole predicate.
/* eslint-disable quotes -- double quotes keep the expected SQL readable */
const PREDICATES: [string, string][] = [
  ['contains', "LOWER(\"orders\".status) LIKE LOWER('%' || ?|| '%') ESCAPE '\\'"],
  ['notContains', "LOWER(\"orders\".status) NOT LIKE LOWER('%' || ?|| '%') ESCAPE '\\'"],
  ['startsWith', "LOWER(\"orders\".status) LIKE LOWER(?|| '%') ESCAPE '\\'"],
  ['endsWith', "LOWER(\"orders\".status) LIKE LOWER('%' || ?) ESCAPE '\\'"],
];
/* eslint-enable quotes */

// Building a query loads the native planner, which on a cold run - the unit job
// starts one right after installing - takes longer than jest's default budget.
const COLD_START_TIMEOUT = 60 * 1000;

describe('DremioQuery SQL templates', () => {
  it.each(PREDICATES)(
    'escapes and interprets LIKE wildcards for %s',
    async (operator, predicate) => {
      const { sql, params } = await buildFilter(operator);

      expect(params).toEqual(['\\%']);
      expect(sql).toContain(predicate);
    },
    COLD_START_TIMEOUT
  );

  // Dremio's ILIKE is a function taking no escape argument, so it cannot be used
  // and still say how the value was escaped. The predicates above pin the
  // replacement; this pins the operator staying gone.
  it('does not render ILIKE', async () => {
    const { sql } = await buildFilter('contains');

    expect(sql).not.toMatch(/ILIKE/i);
  }, COLD_START_TIMEOUT);
});
