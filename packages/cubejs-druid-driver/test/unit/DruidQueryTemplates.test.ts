import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';
import { DruidQuery } from '../../src/DruidQuery';

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

const buildQuery = async (query: Record<string, unknown> = {}) => {
  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(MODEL);

  await compiler.compile();

  return new DruidQuery({ joinGraph, cubeEvaluator, compiler }, {
    measures: ['orders.count'],
    ...query,
  });
};

const buildFilter = async (operator: string, useNativeSqlPlanner: boolean) => {
  const query = await buildQuery({
    filters: [{ member: 'orders.status', operator, values: ['%'] }],
    useNativeSqlPlanner,
  });

  const [sql, params] = query.buildSqlAndParams();

  return { sql: sql.replace(/\s+/g, ' '), params };
};

// Druid's LIKE has no default escape character, so the value escaping both
// planners apply is only meaningful if the clause that interprets it is
// attached to the predicate - which is why these pin the whole predicate.
// Both planners emit the same one, so the expected SQL is shared.
/* eslint-disable quotes -- double quotes keep the expected SQL readable */
const PREDICATES: [string, string][] = [
  ['contains', "LOWER(\"orders\".status) LIKE CONCAT('%', LOWER(?), '%') ESCAPE '\\'"],
  ['notContains', "LOWER(\"orders\".status) NOT LIKE CONCAT('%', LOWER(?), '%') ESCAPE '\\'"],
  ['startsWith', "LOWER(\"orders\".status) LIKE CONCAT('', LOWER(?), '%') ESCAPE '\\'"],
  ['endsWith', "LOWER(\"orders\".status) LIKE CONCAT('%', LOWER(?), '') ESCAPE '\\'"],
];
/* eslint-enable quotes */

const CASES: [string, string, boolean, string][] = PREDICATES.flatMap(
  ([operator, predicate]) => ([
    [operator, 'legacy', false, predicate],
    [operator, 'tesseract', true, predicate],
  ] as [string, string, boolean, string][])
);

// Building a query loads the native planner, which on a cold run - the unit job
// starts one right after installing - takes longer than jest's default budget.
const COLD_START_TIMEOUT = 60 * 1000;

describe('DruidQuery SQL templates', () => {
  it.each(CASES)(
    'escapes and interprets LIKE wildcards for %s on the %s planner',
    async (operator, _name, useNativeSqlPlanner, predicate) => {
      const { sql, params } = await buildFilter(operator, useNativeSqlPlanner);

      expect(params).toEqual(['\\%']);
      expect(sql).toContain(predicate);
    },
    COLD_START_TIMEOUT
  );

  // The SQL API push-down renders `expressions.like` / `expressions.ilike` in Rust, so the
  // rendering cannot be exercised from here - these pin the gate the rendering reads.
  // `default_escape` is set when the pushed-down LIKE carried no ESCAPE of its own and so
  // still means Postgres' backslash; Druid has no default escape character, so dropping
  // the gate sends the escaping on with nothing to interpret it.
  it.each(['like', 'ilike'])('gates an ESCAPE clause on default_escape in expressions.%s', async (key) => {
    const templates = (await buildQuery()).sqlTemplates();

    // eslint-disable-next-line quotes -- double quotes keep the escaping readable
    expect(templates.expressions[key]).toContain("{% if default_escape %} ESCAPE '\\'{% endif %}");
  }, COLD_START_TIMEOUT);
});
