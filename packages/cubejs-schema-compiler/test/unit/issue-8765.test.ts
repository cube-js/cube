// @link https://github.com/cube-js/cube/issues/8765
//
// `SELECT COUNT(*) FROM some_view` over the SQL API fails when the view has no
// member named exactly `count` (e.g. the cube's count measure is exposed with
// `prefix: true` as `Orders_count`, or the cube has no count measure at all).
// cubesql then pushes `COUNT(*)` down as an ungrouped member expression
// `{ cubeName: 'orders_view', expr: { type: 'SqlFunction', cubeParams: ['orders_view'], sql: 'COUNT(*)' } }`
// that references no members. The planner can't pick a cube for it on a view and
// throws:
//  - Tesseract: "Can't resolve the cube to query for 'expr:orders_view.count_uint8_1__':
//    the member references no members of 'orders_view', ..."
//  - legacy planner: "TypeError: Cannot read properties of null (reading 'joins')"
// The same query works on the cube itself, on the view when a dimension is also
// selected, and on a non-prefixed view with a public `count` measure.
// Reproduced end to end on Cube v1.7.49 (Postgres) with both planners.
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from './PrepareCompiler';

describe('issue #8765: COUNT(*) member expression on a view', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube('Orders', {
      sql: \`SELECT 1 AS id, 100 AS amount, 'new' AS status UNION ALL SELECT 2, 200, 'shipped'\`,
      measures: {
        count: { type: 'count' },
        totalAmount: { sql: 'amount', type: 'sum' },
      },
      dimensions: {
        id: { sql: 'id', type: 'number', primaryKey: true },
        status: { sql: 'status', type: 'string' },
      },
    });

    view('orders_view', {
      cubes: [{ join_path: Orders, prefix: true, includes: '*' }],
    });
  `);

  beforeAll(() => compiler.compile());

  // Same shape the API gateway builds from cubesql's pushed-down `COUNT(*)`.
  const countStar = (cubeName: string) => ({
    // eslint-disable-next-line no-new-func
    expression: new Function(cubeName, 'return `COUNT(*)`;'),
    name: `${cubeName}.count_uint8_1__`,
    expressionName: 'count_uint8_1__',
    definition: 'COUNT(*)',
    cubeName,
  });

  it('builds SQL for an ungrouped COUNT(*) on the cube', () => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [countStar('Orders')],
    });
    const [sql] = query.buildSqlAndParams();
    expect(sql).toMatch(/COUNT\(\*\)/i);
  });

  it('builds SQL for an ungrouped COUNT(*) on a single-cube view', () => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [countStar('orders_view')],
    });
    const [sql] = query.buildSqlAndParams();
    expect(sql).toMatch(/COUNT\(\*\)/i);
  });
});
