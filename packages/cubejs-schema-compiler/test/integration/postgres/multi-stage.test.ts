import {
  getEnv,
} from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Multi-Stage', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: >
      SELECT 10 as ID, 'complited' as STATUS, '2024-01-12T20:00:00.000Z'::timestamptz as CREATED_AT
    joins:
      - name: line_items
        sql: "{CUBE}.ID = {line_items}.order_id"
        relationship: many_to_one

    dimensions:
      - name: id
        sql: ID
        type: number
        primary_key: true

      - name: status
        sql: STATUS
        type: string

      - name: date
        sql: CREATED_AT
        type: time

      - name: amount
        sql: '{line_items.total_amount}'
        type: number
        sub_query: true

    measures:
      - name: count
        type: count

      - name: completed_count
        type: count
        filters:
          - sql: "{CUBE}.STATUS = 'completed'"

      - name: returned_count
        type: count
        filters:
          - sql: "{CUBE}.STATUS = 'returned'"

      - name: return_rate
        type: number
        sql: "({returned_count} / NULLIF({completed_count}, 0)) * 100.0"
        description: "Percentage of returned orders out of completed, exclude just placed orders."
        format: percent

      - name: total_amount
        sql: '{CUBE.amount}'
        type: sum

      - name: revenue
        sql: "CASE WHEN {CUBE}.status = 'completed' THEN {CUBE.amount} END"
        type: sum
        format: currency

      - name: average_order_value
        sql: '{CUBE.amount}'
        type: avg

      - name: revenue_1_y_ago
        sql: "{revenue}"
        multi_stage: true
        type: number
        format: currency
        time_shift:
          - time_dimension: date
            interval: 1 year
            type: prior
          - time_dimension: orders_view.date
            interval: 1 year
            type: prior

      - name: cagr_1_y
        sql: "(({revenue} / {revenue_1_y_ago}) - 1)"
        type: number
        format: percent
        description: "Annual CAGR, year over year growth in revenue"

  - name: line_items
    sql: >
      SELECT 10 as ID, '2024-01-12T20:00:00.000Z'::timestamptz as CREATED_AT, 10 as ORDER_ID, 10 as PRODUCT_ID
    public: false

    joins:
      - name: products
        sql: "{CUBE}.PRODUCT_ID = {products}.ID"
        relationship: many_to_one

    dimensions:
      - name: id
        sql: ID
        type: number
        primary_key: true

      - name: created_at
        sql: CREATED_AT
        type: time

      - name: price
        sql: "{products.price}"
        type: number

    measures:
      - name: count
        type: count

      - name: total_amount
        sql: "{price}"
        type: sum

  - name: products
    sql: >
      SELECT 10 as ID, 'some category' as PRODUCT_CATEGORY, 'some name' as NAME, 10 as PRICE
    public: false
    description: >
      Products and categories in our e-commerce store.

    dimensions:
      - name: id
        sql: ID
        type: number
        primary_key: true

      - name: product_category
        sql: PRODUCT_CATEGORY
        type: string

      - name: name
        sql: NAME
        type: string

      - name: price
        sql: PRICE
        type: number

    measures:
      - name: count
        type: count
views:
  - name: orders_view

    cubes:
      - join_path: orders
        includes:
          - date
          - revenue
          - cagr_1_y
          - return_rate

      - join_path: line_items.products
        prefix: true
        includes:
          - product_category

    `);

  async function runQueryTest(q, expectedResult) {
    if (!getEnv('nativeSqlPlanner')) {
      return;
    }
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, q);

    console.log(query.buildSqlAndParams());

    const res = await dbRunner.testQuery(query.buildSqlAndParams());
    console.log(JSON.stringify(res));

    expect(res).toEqual(
      expectedResult
    );
  }

  it('multi stage over sub query', async () => runQueryTest({
    measures: ['orders.revenue', 'orders.revenue_1_y_ago'],
    timeDimensions: [
      {
        dimension: 'orders.date',
        granularity: 'year'
      }
    ],
    dimensions: [
      'orders.status',
      'orders.amount'
    ],
    timezone: 'UTC'
  }, []));
});
