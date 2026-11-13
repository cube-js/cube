import { getEnv } from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';
import type { BaseQueryOptions } from '../../../src/adapter/BaseQuery';

// How measures of several facts are computed: customers -< orders -< returns, with
// a return of no order, and payments joined into orders.
const describeTesseract = getEnv('nativeSqlPlanner') ? describe : describe.skip;

describeTesseract('Measures over several facts', () => {
  jest.setTimeout(200000);

  const compilers = prepareYamlCompiler(`
cubes:
  - name: regions
    sql: >
      SELECT 1 AS id, 'East' AS name UNION ALL
      SELECT 2 AS id, 'Midwest' AS name
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: name
        sql: name
        type: string

  - name: customers
    sql: >
      SELECT 1 AS id, 'New York' AS city, 1 AS region_id UNION ALL
      SELECT 2 AS id, 'Boston' AS city, 1 AS region_id UNION ALL
      SELECT 3 AS id, 'Chicago' AS city, 2 AS region_id UNION ALL
      SELECT 4 AS id, 'New York' AS city, 1 AS region_id
    joins:
      - name: regions
        relationship: many_to_one
        sql: "{customers}.region_id = {regions}.id"
      - name: orders
        relationship: one_to_many
        sql: "{customers}.id = {orders}.customer_id"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: city
        sql: city
        type: string
    measures:
      - name: count
        type: count
      - name: net_per_customer
        type: number
        sql: "{orders.amount_minus_refund} / NULLIF({customers.count}, 0)"

  - name: orders
    sql: >
      SELECT 1 AS id, 1 AS customer_id, 'completed' AS status, 100.00::numeric(10, 2) AS amount UNION ALL
      SELECT 2 AS id, 1 AS customer_id, 'completed' AS status, 200.00::numeric(10, 2) AS amount UNION ALL
      SELECT 3 AS id, 1 AS customer_id, 'pending' AS status, 40.00::numeric(10, 2) AS amount UNION ALL
      SELECT 4 AS id, 2 AS customer_id, 'completed' AS status, 300.00::numeric(10, 2) AS amount UNION ALL
      SELECT 5 AS id, 2 AS customer_id, 'pending' AS status, 1000.00::numeric(10, 2) AS amount UNION ALL
      SELECT 6 AS id, 4 AS customer_id, 'completed' AS status, 400.00::numeric(10, 2) AS amount
    joins:
      - name: returns
        relationship: one_to_many
        sql: "{orders}.id = {returns}.order_id"
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
      - name: min_amount
        type: min
        sql: amount
      - name: max_amount
        type: max
        sql: amount
      - name: unique_customers
        type: count_distinct
        sql: customer_id
      - name: amount_minus_refund
        type: number
        sql: "{orders.total_amount} - {returns.total_refund}"
      - name: amount_minus_refund_plus_one
        type: number
        sql: "{orders.amount_minus_refund} + 1"
      - name: amount_minus_refund_plus_count
        type: number
        sql: "{orders.amount_minus_refund} + {orders.count}"
      - name: returns_proxy
        type: number
        sql: "{returns.count}"
      - name: count_minus_returns
        type: number
        sql: "{orders.count} - {returns.count}"
      - name: unpaid
        type: number
        sql: "{orders.total_amount} - {payments.total_paid}"

  - name: returns
    sql: >
      SELECT 1 AS id, 1 AS order_id, 'defective' AS reason, 30.00::numeric(10, 2) AS refund UNION ALL
      SELECT 2 AS id, 1 AS order_id, 'wrong_item' AS reason, 20.00::numeric(10, 2) AS refund UNION ALL
      SELECT 3 AS id, 4 AS order_id, 'defective' AS reason, 50.00::numeric(10, 2) AS refund UNION ALL
      SELECT 4 AS id, NULL::int AS order_id, 'lost' AS reason, 7.00::numeric(10, 2) AS refund UNION ALL
      SELECT 5 AS id, 6 AS order_id, 'wrong_item' AS reason, 400.00::numeric(10, 2) AS refund
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: reason
        sql: reason
        type: string
    measures:
      - name: count
        type: count
      - name: total_refund
        type: sum
        sql: refund

  - name: payments
    sql: >
      SELECT 1 AS id, 1 AS order_id, 60.00::numeric(10, 2) AS amount UNION ALL
      SELECT 2 AS id, 1 AS order_id, 40.00::numeric(10, 2) AS amount UNION ALL
      SELECT 3 AS id, 3 AS order_id, 150.00::numeric(10, 2) AS amount UNION ALL
      SELECT 4 AS id, 4 AS order_id, 300.00::numeric(10, 2) AS amount UNION ALL
      SELECT 5 AS id, NULL::int AS order_id, 9.00::numeric(10, 2) AS amount
    joins:
      - name: orders
        relationship: many_to_one
        sql: "{payments}.order_id = {orders}.id"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: total_paid
        type: sum
        sql: amount

views:
  - name: sales
    measures:
      - name: net_amount
        type: number
        sql: "{CUBE.total_amount} - {CUBE.total_refund}"
    cubes:
      - join_path: customers
        includes:
          - city
      - join_path: customers.orders
        includes:
          - count
          - total_amount
      - join_path: customers.orders.returns
        includes:
          - total_refund
`);

  async function runQuery(q: BaseQueryOptions) {
    await compilers.compile();
    const query = compilers.createQuery(PostgresQuery, { timezone: 'UTC', ...q });
    return dbRunner.testQuery(query.buildSqlAndParams());
  }

  const byCity = { dimensions: ['customers.city'], order: [{ id: 'customers.city' }] };

  it('calculated measure across facts', async () => {
    expect(await runQuery({ measures: ['orders.amount_minus_refund'], ...byCity })).toEqual([
      { customers__city: 'Boston', orders__amount_minus_refund: '1250.00' },
      { customers__city: 'Chicago', orders__amount_minus_refund: null },
      { customers__city: 'New York', orders__amount_minus_refund: '290.00' },
    ]);
  });

  it('calculated measure across facts with its leaves', async () => {
    expect(await runQuery({
      measures: ['orders.amount_minus_refund', 'orders.total_amount', 'returns.total_refund'],
      ...byCity,
    })).toEqual([
      { customers__city: 'Boston', orders__amount_minus_refund: '1250.00', orders__total_amount: '1300.00', returns__total_refund: '50.00' },
      { customers__city: 'Chicago', orders__amount_minus_refund: null, orders__total_amount: null, returns__total_refund: null },
      { customers__city: 'New York', orders__amount_minus_refund: '290.00', orders__total_amount: '740.00', returns__total_refund: '450.00' },
    ]);
  });

  it('calculated measure over a calculated measure across facts', async () => {
    expect(await runQuery({ measures: ['orders.amount_minus_refund_plus_one'], ...byCity })).toEqual([
      { customers__city: 'Boston', orders__amount_minus_refund_plus_one: '1251.00' },
      { customers__city: 'Chicago', orders__amount_minus_refund_plus_one: null },
      { customers__city: 'New York', orders__amount_minus_refund_plus_one: '291.00' },
    ]);
  });

  it('calculated measure over a calculated measure across facts and a sibling', async () => {
    expect(await runQuery({ measures: ['orders.amount_minus_refund_plus_count'], ...byCity })).toEqual([
      { customers__city: 'Boston', orders__amount_minus_refund_plus_count: '1252.00' },
      { customers__city: 'Chicago', orders__amount_minus_refund_plus_count: null },
      { customers__city: 'New York', orders__amount_minus_refund_plus_count: '294.00' },
    ]);
  });

  // Read directly, the return of no order counts; read through orders, it does not.
  it('one measure read over two joins', async () => {
    expect(await runQuery({
      measures: ['returns.total_refund', 'orders.amount_minus_refund'],
      dimensions: ['returns.reason'],
      order: [{ id: 'returns.reason' }],
    })).toEqual([
      { returns__reason: 'defective', returns__total_refund: '80.00', orders__amount_minus_refund: '320.00' },
      { returns__reason: 'lost', returns__total_refund: '7.00', orders__amount_minus_refund: null },
      { returns__reason: 'wrong_item', returns__total_refund: '420.00', orders__amount_minus_refund: '80.00' },
      { returns__reason: null, returns__total_refund: null, orders__amount_minus_refund: null },
    ]);
  });

  // Returns are counted through orders: the return of no order is not.
  it('calculated measure across facts, grand total', async () => {
    expect(await runQuery({ measures: ['orders.count_minus_returns'] })).toEqual([
      { orders__count_minus_returns: '2' },
    ]);
  });

  it('proxy of a measure of another cube, grand total', async () => {
    expect(await runQuery({ measures: ['orders.returns_proxy'] })).toEqual([
      { orders__returns_proxy: '4' },
    ]);
  });

  it('calculated measure two levels up the chain', async () => {
    expect(await runQuery({
      measures: ['customers.net_per_customer'],
      dimensions: ['regions.name'],
      order: [{ id: 'regions.name' }],
    })).toEqual([
      { regions__name: 'East', customers__net_per_customer: '513.3333333333333333' },
      { regions__name: 'Midwest', customers__net_per_customer: null },
    ]);
  });

  // Orders repeat once per return; each is still counted once.
  it('aggregates of orders by a dimension of returns', async () => {
    expect(await runQuery({
      measures: ['orders.count', 'orders.total_amount', 'orders.min_amount', 'orders.max_amount', 'orders.unique_customers'],
      dimensions: ['returns.reason'],
      order: [{ id: 'returns.reason' }],
    })).toEqual([
      { returns__reason: 'defective', orders__count: '2', orders__total_amount: '400.00', orders__min_amount: '100.00', orders__max_amount: '300.00', orders__unique_customers: '2' },
      { returns__reason: 'wrong_item', orders__count: '2', orders__total_amount: '500.00', orders__min_amount: '100.00', orders__max_amount: '400.00', orders__unique_customers: '2' },
      { returns__reason: null, orders__count: '3', orders__total_amount: '1240.00', orders__min_amount: '40.00', orders__max_amount: '1000.00', orders__unique_customers: '2' },
    ]);
  });

  it('measure filter, order, limit and offset on a calculated measure across facts', async () => {
    expect(await runQuery({
      measures: ['orders.amount_minus_refund'],
      dimensions: ['customers.city'],
      filters: [{ member: 'orders.amount_minus_refund', operator: 'gt', values: ['100'] }],
      order: [{ id: 'orders.amount_minus_refund', desc: true }],
      limit: 1,
      offset: 1,
    })).toEqual([
      { customers__city: 'New York', orders__amount_minus_refund: '290.00' },
    ]);
  });

  it('calculated measure of a view across facts', async () => {
    expect(await runQuery({
      measures: ['sales.net_amount', 'sales.count'],
      dimensions: ['sales.city'],
      order: [{ id: 'sales.city' }],
    })).toEqual([
      { sales__city: 'Boston', sales__net_amount: '1250.00', sales__count: '2' },
      { sales__city: 'Chicago', sales__net_amount: null, sales__count: '0' },
      { sales__city: 'New York', sales__net_amount: '290.00', sales__count: '4' },
    ]);
  });

  // Payments are joined into orders: an order without payments is counted, the
  // payment of no order lands in the NULL status.
  it('calculated measure over a fact joined into its cube', async () => {
    expect(await runQuery({
      measures: ['orders.unpaid'],
      dimensions: ['orders.status'],
      order: [{ id: 'orders.status' }],
    })).toEqual([
      { orders__status: 'completed', orders__unpaid: '600.00' },
      { orders__status: 'pending', orders__unpaid: '890.00' },
      { orders__status: null, orders__unpaid: null },
    ]);
  });
});
