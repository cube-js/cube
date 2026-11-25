import { PostgresQuery } from '../../src';
import { prepareJsCompiler, prepareYamlCompiler } from './PrepareCompiler';

// A member reference in a measure's `filters[].sql` is checked at compile time,
// the same as one in its `sql`, instead of failing only when the measure is queried.
const yamlModel = (filterSql: string) => `
cubes:
  - name: orders
    sql_table: orders
    joins:
      - name: customers
        sql: "{CUBE}.customer_id = {customers.id}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: order_status
        sql: status
        type: string
    measures:
      - name: completed_orders
        type: count
        filters:
          - sql: "${filterSql}"

  - name: customers
    sql_table: customers
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: country
        sql: country
        type: string

  - name: orders_ext
    extends: orders
    sql_table: orders_ext

views:
  - name: orders_view
    cubes:
      - join_path: orders
        includes: "*"
`;

// `filterSql` is written with YAML-style `{member}` references
// `filterSql` uses YAML-style `{member}` references; they become `${member}` here
const jsModel = (filterSql: string) => `
cube('orders', {
  sql_table: 'orders',
  dimensions: {
    id: { sql: 'id', type: 'number', primary_key: true },
    order_status: { sql: 'status', type: 'string' },
  },
  measures: {
    completed_orders: {
      type: 'count',
      filters: [{ sql: \`${filterSql.replace(/\{/g, '$$$&')}\` }],
    },
  },
});
`;

describe('Measure filter references', () => {
  it('fails compile on an unresolved reference in a YAML measure filter', async () => {
    const { compiler } = prepareYamlCompiler(yamlModel("{status} = 'completed'"));
    await expect(compiler.compile()).rejects.toThrow(/orders\.completed_orders' filter 1: status is not defined/);
  });

  it('fails compile on an unresolved reference in a JS measure filter', async () => {
    const { compiler } = prepareJsCompiler(jsModel("{status} = 'completed'"));
    await expect(compiler.compile()).rejects.toThrow(/status is not defined/);
  });

  it('fails compile on an unresolved CUBE member in a measure filter', async () => {
    const { compiler } = prepareYamlCompiler(yamlModel("{CUBE.status} = 'completed'"));
    await expect(compiler.compile()).rejects.toThrow(/status/);
  });

  const allMeasures = ['orders.completed_orders', 'orders_ext.completed_orders', 'orders_view.completed_orders'];
  // `orders_ext` inherits the filter, and `orders` is not joined to it
  const ownCubeMeasures = ['orders.completed_orders', 'orders_view.completed_orders'];

  it.each([
    ["{order_status} = 'completed'", allMeasures],
    ["{CUBE.order_status} = 'completed'", allMeasures],
    ["{CUBE}.status = 'completed'", allMeasures],
    ["{orders.order_status} = 'completed'", ownCubeMeasures],
    ["{customers.country} = 'US'", ownCubeMeasures],
    ["{FILTER_PARAMS.orders.order_status.filter('status')}", allMeasures],
    ["{SECURITY_CONTEXT.tenant.filter('tenant_id')}", allMeasures],
  ])('compiles and queries a valid YAML measure filter: %s', async (filterSql, measures) => {
    const compilers = prepareYamlCompiler(yamlModel(filterSql));
    await compilers.compiler.compile();

    for (const measure of measures) {
      const query = new PostgresQuery(compilers, { measures: [measure] });
      expect(() => query.buildSqlAndParams()).not.toThrow();
    }
  });

  it('compiles a valid JS measure filter', async () => {
    const { compiler } = prepareJsCompiler(jsModel("{CUBE}.status = 'completed' AND {order_status} IS NOT NULL"));
    await expect(compiler.compile()).resolves.not.toThrow();
  });
});
