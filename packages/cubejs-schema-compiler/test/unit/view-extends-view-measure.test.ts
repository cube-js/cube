import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// A view that `extends` another view gets the parent's `cubes` includes and
// also the parent's own measures, dimensions and folders. So a measure that
// is defined on a view, such as a ratio over two fact cubes that do not join
// to each other, can be defined once on a base view and reused by every view
// that extends it.
const cubes = `
cubes:
  - name: customers
    sql: "SELECT 1 AS id, 'Alice' AS name, 'NYC' AS city"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: name
        sql: name
        type: string
      - name: city
        sql: city
        type: string

  - name: orders
    sql: "SELECT 1 AS id, 1 AS customer_id, 'completed' AS status, 100 AS amount"
    joins:
      - name: customers
        relationship: many_to_one
        sql: "{orders}.customer_id = {customers.id}"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: status
        sql: status
        type: string
    measures:
      - name: count
        type: count
      - name: total_amount
        type: sum
        sql: amount

  - name: returns
    sql: "SELECT 1 AS id, 1 AS customer_id, 10 AS refund_amount"
    joins:
      - name: customers
        relationship: many_to_one
        sql: "{returns}.customer_id = {customers.id}"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: count
        type: count
      - name: total_refund
        type: sum
        sql: refund_amount
`;

const baseView = `
views:
  - name: customer_metrics
    public: false
    cubes:
      - join_path: orders
        prefix: true
        includes:
          - count
          - total_amount
      - join_path: returns
        prefix: true
        includes:
          - total_refund
      - join_path: customers
        includes:
          - city
    measures:
      - name: refund_rate
        type: number
        multi_stage: true
        sql: "{CUBE.returns_total_refund} / NULLIF({CUBE.orders_total_amount}, 0)"
      - name: average_order_value
        type: number
        sql: "{CUBE.orders_total_amount} / NULLIF({CUBE.orders_count}, 0)"
    dimensions:
      - name: city_upper
        type: string
        sql: "UPPER({CUBE.city})"
    folders:
      - name: Amounts
        includes:
          - orders_total_amount
          - returns_total_refund
`;

const childViews = `
  - name: sales_overview
    extends: customer_metrics
    public: true
    cubes:
      - join_path: orders
        prefix: true
        includes:
          - status
    folders:
      - name: Orders
        includes:
          - orders_count
          - orders_status

  - name: hidden_child
    extends: customer_metrics

  - name: excluding_child
    extends: customer_metrics
    cubes:
      - join_path: orders
        prefix: true
        includes: "*"
        excludes:
          - total_amount
          - refund_rate

  - name: overriding_child
    extends: customer_metrics
    measures:
      - name: refund_rate
        type: number
        multi_stage: true
        sql: "100.0 * {CUBE.returns_total_refund} / NULLIF({CUBE.orders_total_amount}, 0)"
`;

const compile = async (model: string) => {
  const compilers = prepareYamlCompiler(model);
  await compilers.compiler.compile();
  return compilers;
};

const RATIO = /"returns__total_refund" \/ NULLIF\("[^"]+"\."orders__total_amount", 0\) "(\w+)__refund_rate"/;

describe('View that extends a view with its own members', () => {
  let compilers: any;

  beforeAll(async () => {
    compilers = await compile(cubes + baseView + childViews);
  });

  const buildSql = (query: any, useNativeSqlPlanner = true) => new PostgresQuery(compilers, {
    timezone: 'UTC',
    useNativeSqlPlanner,
    ...query,
  }).buildSqlAndParams()[0];

  const meta = (view: string) => {
    const config = compilers.metaTransformer.cubes
      .map((def: any) => def.config)
      .find((def: any) => def.name === view);

    return {
      public: config.public,
      measures: config.measures.map((m: any) => m.name.split('.')[1]).sort(),
      dimensions: config.dimensions.map((d: any) => d.name.split('.')[1]).sort(),
      folders: config.folders.map((f: any) => `${f.name}: ${f.members.map((m: any) => m.split('.')[1]).join(', ')}`),
    };
  };

  it('gets the parent includes, own members and folders next to its own', () => {
    expect(meta('sales_overview')).toEqual({
      public: true,
      measures: ['average_order_value', 'orders_count', 'orders_total_amount', 'refund_rate', 'returns_total_refund'],
      dimensions: ['city', 'city_upper', 'orders_status'],
      folders: ['Amounts: orders_total_amount, returns_total_refund', 'Orders: orders_count, orders_status'],
    });
  });

  it('plans the inherited multi-fact measure the same way as the parent', () => {
    const child = buildSql({ measures: ['sales_overview.refund_rate'], dimensions: ['sales_overview.city'] });
    const parent = buildSql({ measures: ['customer_metrics.refund_rate'], dimensions: ['customer_metrics.city'] });

    expect(child).toMatch(RATIO);
    expect(child).toMatch(/sum\("orders"\.amount\)/);
    expect(child).toMatch(/sum\("returns"\.refund_amount\)/);
    expect(child.replace(/sales_overview__/g, 'customer_metrics__')).toEqual(parent);
  });

  it('fails the inherited multi-fact measure on the legacy planner the same way as the parent', () => {
    expect(() => buildSql({ measures: ['sales_overview.refund_rate'], dimensions: ['sales_overview.city'] }, false))
      .toThrow(/Can't find join path to join/);
    expect(() => buildSql({ measures: ['customer_metrics.refund_rate'], dimensions: ['customer_metrics.city'] }, false))
      .toThrow(/Can't find join path to join/);
  });

  it('plans inherited single-fact members on both planners', () => {
    for (const useNativeSqlPlanner of [true, false]) {
      const sql = buildSql({
        measures: ['sales_overview.average_order_value'],
        dimensions: ['sales_overview.city_upper'],
      }, useNativeSqlPlanner);

      expect(sql).toMatch(/sum\("orders"\.amount\) \/ NULLIF\(count\("orders"\.id\), 0\)/);
      expect(sql).toContain('UPPER("customers".city)');
    }
  });

  it('inherits `public` when it does not set its own', () => {
    expect(meta('customer_metrics').public).toBe(false);
    expect(meta('hidden_child').public).toBe(false);
  });

  // Current behavior, pinned: `excludes` applies only to the include it is
  // written in, so it cannot remove a member the parent includes or defines.
  // Excluded names are not validated, so nothing reports it.
  it('keeps inherited members that its own `excludes` names', () => {
    const { measures } = meta('excluding_child');

    expect(measures).toContain('orders_total_amount');
    expect(measures).toContain('refund_rate');
    expect(meta('excluding_child').dimensions).toContain('orders_status');
  });

  it('replaces an inherited own member that it redefines', () => {
    const child = buildSql({ measures: ['overriding_child.refund_rate'], dimensions: ['overriding_child.city'] });
    const parent = buildSql({ measures: ['customer_metrics.refund_rate'], dimensions: ['customer_metrics.city'] });

    expect(child).toMatch(/100\.0 \* "[^"]+"\."returns__total_refund"/);
    expect(parent).not.toContain('100.0 *');
  });

  it('rejects a member that has the name of an inherited included member', async () => {
    await expect(compile(`${cubes}${baseView}
  - name: conflicting_child
    extends: customer_metrics
    measures:
      - name: orders_total_amount
        type: number
        sql: "{CUBE.returns_total_refund}"
`)).rejects.toThrow(/Included member 'orders_total_amount' conflicts with existing member of 'conflicting_child'/);
  });

  it('rejects a folder that has the name of an inherited folder', async () => {
    await expect(compile(`${cubes}${baseView}
  - name: folder_child
    extends: customer_metrics
    folders:
      - name: Amounts
        includes:
          - orders_total_amount
`)).rejects.toThrow(/Found duplicate folder 'Amounts' in view 'folder_child'/);
  });
});
