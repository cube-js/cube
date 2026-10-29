import { getEnv } from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';
import type { BaseQueryOptions } from '../../../src/adapter/BaseQuery';

// A member follows its own view's join path, not the paths of other views in the
// query, so each view query returns the rows of the same query over the cubes.
const describeTesseract = getEnv('nativeSqlPlanner') ? describe : describe.skip;

describeTesseract('Multi-view queries with nested view join paths', () => {
  jest.setTimeout(200000);

  const cubes = `
cubes:
  - name: customers
    sql: >
      SELECT 1 AS id, 'A' AS city, 20 AS age UNION ALL
      SELECT 2 AS id, 'A' AS city, 30 AS age UNION ALL
      SELECT 3 AS id, 'B' AS city, 40 AS age UNION ALL
      SELECT 4 AS id, 'B' AS city, 60 AS age UNION ALL
      SELECT 5 AS id, 'C' AS city, 10 AS age UNION ALL
      SELECT 6 AS id, 'C' AS city, 50 AS age UNION ALL
      SELECT 7 AS id, 'D' AS city, 25 AS age UNION ALL
      SELECT 8 AS id, 'D' AS city, 35 AS age UNION ALL
      SELECT 9 AS id, NULL::text AS city, 70 AS age UNION ALL
      SELECT 10 AS id, NULL::text AS city, 80 AS age
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: city
        sql: city
        type: string
    measures:
      - name: avg_age
        sql: age
        type: avg

  - name: orders
    sql: >
      SELECT 1 AS id, 3 AS customer_id, 100 AS amount UNION ALL
      SELECT 2 AS id, 3 AS customer_id, 50 AS amount UNION ALL
      SELECT 3 AS id, 7 AS customer_id, 10 AS amount UNION ALL
      SELECT 4 AS id, 9 AS customer_id, 7 AS amount
    joins:
      - name: customers
        relationship: many_to_one
        sql: "{CUBE}.customer_id = {customers.id}"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: revenue
        sql: amount
        type: sum

  - name: returns
    sql: >
      SELECT 1 AS id, 5 AS customer_id, 5 AS refund UNION ALL
      SELECT 2 AS id, 8 AS customer_id, 3 AS refund UNION ALL
      SELECT 3 AS id, 10 AS customer_id, 1 AS refund
    joins:
      - name: customers
        relationship: many_to_one
        sql: "{CUBE}.customer_id = {customers.id}"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: refunds
        sql: refund
        type: sum
`;

  const nestedViews = `
views:
  - name: customers_view
    cubes:
      - join_path: customers
        includes: [city, avg_age]

  - name: orders_view
    cubes:
      - join_path: orders
        includes: [revenue]
      - join_path: orders.customers
        includes: [city]

  - name: returns_view
    cubes:
      - join_path: returns
        includes: [refunds]
      - join_path: returns.customers
        includes: [city]
`;

  // The documented layout: every fact view includes `customers` at a root-level
  // join path.
  const rootLevelViews = `
views:
  - name: customers_view
    cubes:
      - join_path: customers
        includes: [city, avg_age]

  - name: orders_view
    cubes:
      - join_path: orders
        includes: [revenue]
      - join_path: customers
        includes: [city]

  - name: returns_view
    cubes:
      - join_path: returns
        includes: [refunds]
      - join_path: customers
        includes: [city]

  - name: customer_overview
    cubes:
      - join_path: orders
        includes: [revenue]
      - join_path: returns
        includes: [refunds]
      - join_path: customers
        includes: [city, avg_age]
`;

  const nested = prepareYamlCompiler(cubes + nestedViews);
  const rootLevel = prepareYamlCompiler(cubes + rootLevelViews);

  // Rows as value lists: a view query and the cube query it must match alias
  // their columns differently.
  async function runQuery(compilers: ReturnType<typeof prepareYamlCompiler>, q: BaseQueryOptions) {
    await compilers.compile();
    const query = compilers.createQuery(PostgresQuery, q);
    const rows = await dbRunner.testQuery(query.buildSqlAndParams());
    return rows.map((row: Record<string, unknown>) => Object.values(row));
  }

  const avgAgeByCityWithRevenue = [
    ['A', '25.0000000000000000', null],
    ['B', '50.0000000000000000', '150'],
    ['C', '30.0000000000000000', null],
    ['D', '30.0000000000000000', '10'],
    [null, '75.0000000000000000', '7'],
  ];

  const refundsByCityWithAvgAge = [
    ['A', null, '25.0000000000000000'],
    ['B', null, '50.0000000000000000'],
    ['C', '5', '30.0000000000000000'],
    ['D', '3', '30.0000000000000000'],
    [null, '1', '75.0000000000000000'],
  ];

  const revenueAndRefundsByCity = [
    ['B', '150', null],
    ['C', null, '5'],
    ['D', '10', '3'],
    [null, '7', '1'],
  ];

  it('cube queries give the reference rows', async () => {
    expect(await runQuery(nested, {
      dimensions: ['customers.city'],
      measures: ['customers.avg_age', 'orders.revenue'],
      order: [{ id: 'customers.city' }],
    })).toEqual(avgAgeByCityWithRevenue);

    expect(await runQuery(nested, {
      dimensions: ['customers.city'],
      measures: ['returns.refunds', 'customers.avg_age'],
      order: [{ id: 'customers.city' }],
    })).toEqual(refundsByCityWithAvgAge);

    expect(await runQuery(nested, {
      dimensions: ['customers.city'],
      measures: ['orders.revenue', 'returns.refunds'],
      order: [{ id: 'customers.city' }],
    })).toEqual(revenueAndRefundsByCity);
  });

  describe('nested view join paths', () => {
    it('measure of a dimension view next to a measure of a fact view', async () => {
      // `customers_view.avg_age` must be averaged over every customer, not
      // only over those reached through `orders`.
      expect(await runQuery(nested, {
        dimensions: ['customers_view.city'],
        measures: ['customers_view.avg_age', 'orders_view.revenue'],
        order: [{ id: 'customers_view.city' }],
      })).toEqual(avgAgeByCityWithRevenue);
    });

    it('measure of a fact view next to a measure of a dimension view', async () => {
      expect(await runQuery(nested, {
        dimensions: ['customers_view.city'],
        measures: ['returns_view.refunds', 'customers_view.avg_age'],
        order: [{ id: 'customers_view.city' }],
      })).toEqual(refundsByCityWithAvgAge);
    });

    it('cube measure next to a measure of a fact view', async () => {
      expect(await runQuery(nested, {
        dimensions: ['customers.city'],
        measures: ['customers.avg_age', 'orders_view.revenue'],
        order: [{ id: 'customers.city' }],
      })).toEqual(avgAgeByCityWithRevenue);
    });

    it('measures of two fact views by a dimension of one of them', async () => {
      // `orders_view.city` is `customers.city` as reached from `orders`, which
      // `returns` has no join to.
      await expect(runQuery(nested, {
        dimensions: ['orders_view.city'],
        measures: ['orders_view.revenue', 'returns_view.refunds'],
        order: [{ id: 'orders_view.city' }],
      })).rejects.toThrow(
        "Can't find join path to join 'orders,customers', 'returns': " +
        "'orders_view.city' is joined through 'orders', which 'returns_view.refunds' has no join to"
      );
    });

    it('measures of two fact views by a dimension of a third view', async () => {
      expect(await runQuery(nested, {
        dimensions: ['customers_view.city'],
        measures: ['orders_view.revenue', 'returns_view.refunds'],
        order: [{ id: 'customers_view.city' }],
      })).toEqual(revenueAndRefundsByCity);
    });
  });

  describe('root-level view join paths', () => {
    it('measure of a dimension view next to a measure of a fact view', async () => {
      expect(await runQuery(rootLevel, {
        dimensions: ['customers_view.city'],
        measures: ['customers_view.avg_age', 'orders_view.revenue'],
        order: [{ id: 'customers_view.city' }],
      })).toEqual(avgAgeByCityWithRevenue);
    });

    it('measures of two fact views by a dimension of one of them', async () => {
      expect(await runQuery(rootLevel, {
        dimensions: ['orders_view.city'],
        measures: ['orders_view.revenue', 'returns_view.refunds'],
        order: [{ id: 'orders_view.city' }],
      })).toEqual(revenueAndRefundsByCity);
    });

    it('multi-fact view', async () => {
      expect(await runQuery(rootLevel, {
        dimensions: ['customer_overview.city'],
        measures: ['customer_overview.avg_age', 'customer_overview.revenue'],
        order: [{ id: 'customer_overview.city' }],
      })).toEqual(avgAgeByCityWithRevenue);
    });
  });
});
