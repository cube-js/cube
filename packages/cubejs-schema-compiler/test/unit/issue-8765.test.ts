import { prepareJsCompiler } from './PrepareCompiler';
import { PostgresQuery } from '../../src/adapter/PostgresQuery';

// https://github.com/cube-js/cube/issues/8765
// `SELECT COUNT(*) FROM orders_view` over the SQL API, when the view has no public `count`
// measure (here it is `public: false` and the view uses `prefix: true`), is pushed down by
// CubeSQL as the member expression `COUNT(*)` with `cubeName: orders_view` and no other
// members. Building SQL for it fails:
// - Tesseract: "Can't resolve the cube to query for 'expr:orders_view.count_uint8_1__': the
//   member references no members of 'orders_view', and neither the rest of the query nor the
//   join map of 'orders_view' gives a cube to join from" (the view joins a single cube).
// - Legacy planner: "TypeError: Cannot read properties of null (reading 'joins')", for the view
//   and for the cube itself.
describe('Issue #8765: COUNT(*) member expression without a count measure', () => {
  const { compiler, cubeEvaluator, joinGraph } = prepareJsCompiler(`
cube('Orders', {
  sql: \`
  select 1 as id, 100 as amount, 'new' status
  UNION ALL
  select 2 as id, 200 as amount, 'new' status
  \`,
  measures: {
    count: {
      type: 'count',
      public: false,
    },
    totalAmount: {
      sql: 'amount',
      type: 'sum',
    },
    toRemove: {
      type: 'count',
    },
  },
  dimensions: {
    status: {
      sql: 'status',
      type: 'string',
    },
  },
});

view('orders_view', {
  cubes: [{
    join_path: Orders,
    prefix: true,
    includes: '*',
  }],
});
`);

  // The shape the API gateway builds from CubeSQL's
  // {"cubeName":"orders_view","alias":"count_uint8_1__","expr":{"type":"SqlFunction","cubeParams":["orders_view"],"sql":"COUNT(*)"}}
  const countStar = (cubeName: string) => ({
    // eslint-disable-next-line no-new-func
    expression: new Function(cubeName, 'return `COUNT(*)`'),
    name: 'count_uint8_1__',
    expressionName: 'count_uint8_1__',
    definition: 'COUNT(*)',
    cubeName,
  });

  const buildSql = async (cubeName: string, dimensions: string[]) => {
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [countStar(cubeName)],
      dimensions,
      timezone: 'UTC',
    } as any);
    const [sql] = query.buildSqlAndParams();
    return sql;
  };

  it('SELECT COUNT(*) FROM orders_view', async () => {
    const sql = await buildSql('orders_view', []);
    expect(sql).toMatch(/COUNT\(\*\)/);
    expect(sql).not.toMatch(/GROUP BY/);
  });

  it('SELECT Orders_status, COUNT(*) FROM orders_view GROUP BY 1', async () => {
    const sql = await buildSql('orders_view', ['orders_view.Orders_status']);
    expect(sql).toMatch(/COUNT\(\*\)/);
  });

  it('SELECT COUNT(*) FROM Orders', async () => {
    const sql = await buildSql('Orders', []);
    expect(sql).toMatch(/COUNT\(\*\)/);
  });
});
