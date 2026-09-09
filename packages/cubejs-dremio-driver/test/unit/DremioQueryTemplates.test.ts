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

const buildContainsFilter = async (useNativeSqlPlanner: boolean) => {
  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(MODEL);

  await compiler.compile();

  const query = new DremioQuery({ joinGraph, cubeEvaluator, compiler }, {
    measures: ['orders.count'],
    filters: [{ member: 'orders.status', operator: 'contains', values: ['%'] }],
    useNativeSqlPlanner,
  });

  const [sql, params] = query.buildSqlAndParams();

  return { sql: sql.replace(/\s+/g, ' '), params };
};

describe('DremioQuery SQL templates', () => {
  // Dremio has no default LIKE escape character - the `default_escape` gate on
  // its `expressions.like` template is the repo's own record of that. Both
  // planners escape `%`, `_` and `\` in the filter value (BaseQuery's
  // `like_escape_char`), so the statement has to carry the clause that
  // interprets that escaping; without one a user searching for a literal `%`
  // matches nothing instead of the rows containing a percent sign.
  it('escapes LIKE wildcards in filter values on both planners', async () => {
    expect((await buildContainsFilter(false)).params).toEqual(['\\%']);
    expect((await buildContainsFilter(true)).params).toEqual(['\\%']);
  });

  it('interprets that escaping with an explicit ESCAPE clause on the native planner', async () => {
    const { sql } = await buildContainsFilter(true);

    // eslint-disable-next-line quotes -- double quotes keep the SQL readable
    expect(sql).toContain("ESCAPE '\\'");
  });

  // Dremio spells case-insensitive matching as the `ILIKE(expr, pattern)`
  // function, not as an infix operator - `sqlTemplates` deleting
  // `expressions.ilike` is what records that here. The native filter path
  // renders `tesseract.ilike`, which is a separate template from the one that
  // delete covers, so it has to avoid the operator on its own.
  it('does not render ILIKE as an infix operator on the native planner', async () => {
    const { sql } = await buildContainsFilter(true);

    expect(sql).not.toMatch(/"orders"\.status\s+(NOT\s+)?ILIKE/i);
  });
});
