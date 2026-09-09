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

const PLANNERS: [string, boolean][] = [['legacy', false], ['tesseract', true]];

describe('DremioQuery SQL templates', () => {
  // Dremio has no default LIKE escape character - the `default_escape` gate on
  // its `expressions.like` template is the repo's own record of that. Both
  // planners escape `%`, `_` and `\` in the filter value (BaseQuery's
  // `like_escape_char`), so the statement has to carry the clause that
  // interprets that escaping; without one a user searching for a literal `%`
  // matches nothing instead of the rows containing a percent sign.
  it.each(PLANNERS)(
    'escapes LIKE wildcards in filter values and interprets them on the %s planner',
    async (_name, useNativeSqlPlanner) => {
      const { sql, params } = await buildFilter('contains', useNativeSqlPlanner);

      expect(params).toEqual(['\\%']);
      // eslint-disable-next-line quotes -- double quotes keep the SQL readable
      expect(sql).toContain("ESCAPE '\\'");
    }
  );

  // Dremio spells case-insensitive matching as the `ILIKE(expr, pattern)`
  // function, not as an infix operator - `sqlTemplates` deleting
  // `expressions.ilike` is what records that here. The function also takes no
  // escape argument, so neither planner can use it and still say how the value
  // was escaped.
  it.each(PLANNERS)('does not render ILIKE on the %s planner', async (_name, useNativeSqlPlanner) => {
    const { sql } = await buildFilter('contains', useNativeSqlPlanner);

    expect(sql).not.toMatch(/ILIKE/i);
  });

  // A negation belongs beside the operator. Spliced into the first argument of
  // a function call instead, it is a parse error rather than a filter.
  it.each(PLANNERS)('negates beside the operator on the %s planner', async (_name, useNativeSqlPlanner) => {
    const { sql } = await buildFilter('notContains', useNativeSqlPlanner);

    expect(sql).toContain('NOT LIKE');
    expect(sql).not.toMatch(/"orders"\.status\s+NOT\s*[,)]/i);
  });
});
