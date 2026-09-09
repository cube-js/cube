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

const buildFilter = async (operator: string, useNativeSqlPlanner: boolean) => {
  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(MODEL);

  await compiler.compile();

  const query = new DremioQuery({ joinGraph, cubeEvaluator, compiler }, {
    measures: ['orders.count'],
    filters: [{ member: 'orders.status', operator, values: ['%'] }],
    useNativeSqlPlanner,
  });

  const [sql, params] = query.buildSqlAndParams();

  return { sql: sql.replace(/\s+/g, ' '), params };
};

// Dremio has no default LIKE escape character, so the value escaping both
// planners apply is only meaningful if the clause that interprets it is
// attached to the predicate - which is why these pin the whole predicate.
/* eslint-disable quotes -- double quotes keep the expected SQL readable */
const PREDICATES: [string, string, boolean, string][] = [
  ['contains', 'legacy', false, "LOWER(\"orders\".status) LIKE LOWER(CONCAT('%', ?, '%')) ESCAPE '\\'"],
  ['notContains', 'legacy', false, "LOWER(\"orders\".status) NOT LIKE LOWER(CONCAT('%', ?, '%')) ESCAPE '\\'"],
  ['startsWith', 'legacy', false, "LOWER(\"orders\".status) LIKE LOWER(CONCAT('', ?, '%')) ESCAPE '\\'"],
  ['endsWith', 'legacy', false, "LOWER(\"orders\".status) LIKE LOWER(CONCAT('%', ?, '')) ESCAPE '\\'"],
  ['contains', 'tesseract', true, "LOWER(\"orders\".status) LIKE LOWER('%' || ?|| '%') ESCAPE '\\'"],
  ['notContains', 'tesseract', true, "LOWER(\"orders\".status) NOT LIKE LOWER('%' || ?|| '%') ESCAPE '\\'"],
  ['startsWith', 'tesseract', true, "LOWER(\"orders\".status) LIKE LOWER(?|| '%') ESCAPE '\\'"],
  ['endsWith', 'tesseract', true, "LOWER(\"orders\".status) LIKE LOWER('%' || ?) ESCAPE '\\'"],
];
/* eslint-enable quotes */

describe('DremioQuery SQL templates', () => {
  it.each(PREDICATES)(
    'escapes and interprets LIKE wildcards for %s on the %s planner',
    async (operator, _name, useNativeSqlPlanner, predicate) => {
      const { sql, params } = await buildFilter(operator, useNativeSqlPlanner);

      expect(params).toEqual(['\\%']);
      expect(sql).toContain(predicate);
    }
  );

  // Dremio's ILIKE is a function taking no escape argument, so neither planner
  // can use it and still say how the value was escaped. The predicates above
  // pin the replacement; this pins the operator staying gone.
  it.each([['legacy', false], ['tesseract', true]] as [string, boolean][])(
    'does not render ILIKE on the %s planner',
    async (_name, useNativeSqlPlanner) => {
      const { sql } = await buildFilter('contains', useNativeSqlPlanner);

      expect(sql).not.toMatch(/ILIKE/i);
    }
  );
});
