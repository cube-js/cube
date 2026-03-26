import {
  getEnv,
} from '@cubejs-backend/shared';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Multi-Stage median type', () => {
  jest.setTimeout(200000);

  // Test data:
  //   client A, date 2024-01-01: policy_ids 1,2,3 → count_distinct = 3
  //   client B, date 2024-01-01: policy_id 1      → count_distinct = 1
  //   median of [3,1] = PERCENTILE_CONT(0.5) = 2
  //   filtered to client A only: median of [3] = 3
  const { compiler: medianCompiler, joinGraph: medianJoinGraph, cubeEvaluator: medianCubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: policies
    sql: >
      SELECT 'A' as client_id, '2024-01-01' as collection_date, 1 as policy_id
      UNION ALL SELECT 'A', '2024-01-01', 2
      UNION ALL SELECT 'A', '2024-01-01', 3
      UNION ALL SELECT 'B', '2024-01-01', 1

    dimensions:
      - name: client_id
        sql: client_id
        type: string
        primary_key: true

      - name: collection_date
        sql: collection_date
        type: string

    measures:
      - name: _inner_count_distinct_policy_id
        sql: policy_id
        type: count_distinct
        public: false

      - name: median_policies
        sql: "{_inner_count_distinct_policy_id}"
        type: median
        multi_stage: true
        group_by:
          - client_id
          - collection_date
  `);

  if (getEnv('nativeSqlPlanner')) {
    it('median multi_stage returns correct unfiltered result', async () => dbRunner.runQueryTest({
      measures: ['policies.median_policies'],
    }, [
      { policies__median_policies: '2' },
    ],
    { joinGraph: medianJoinGraph, cubeEvaluator: medianCubeEvaluator, compiler: medianCompiler }));

    it('median multi_stage is filter-aware', async () => dbRunner.runQueryTest({
      measures: ['policies.median_policies'],
      filters: [{ member: 'policies.client_id', operator: 'equals', values: ['A'] }],
    }, [
      { policies__median_policies: '3' },
    ],
    { joinGraph: medianJoinGraph, cubeEvaluator: medianCubeEvaluator, compiler: medianCompiler }));
  } else {
    test.skip('median multi_stage returns correct unfiltered result', () => { expect(1).toBe(1); });
    test.skip('median multi_stage is filter-aware', () => { expect(1).toBe(1); });
  }
});

describe('Multi-Stage', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: >
      SELECT 9 as ID, 'completed' as STATUS, '2022-01-12T20:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 10 as ID, 'completed' as STATUS, '2023-01-12T20:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 11 as ID, 'completed' as STATUS, '2024-01-14T20:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 12 as ID, 'completed' as STATUS, '2024-02-14T20:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 13 as ID, 'completed' as STATUS, '2025-03-14T20:00:00.000Z'::timestamptz as CREATED_AT
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
      SELECT 9 as ID, '2024-01-12T20:00:00.000Z'::timestamptz as CREATED_AT, 9 as ORDER_ID, 11 as PRODUCT_ID
      union all
      SELECT 10 as ID, '2024-01-12T20:00:00.000Z'::timestamptz as CREATED_AT, 10 as ORDER_ID, 10 as PRODUCT_ID
      union all
      SELECT 11 as ID, '2024-01-12T20:00:00.000Z'::timestamptz as CREATED_AT, 10 as ORDER_ID, 11 as PRODUCT_ID
      union all
      SELECT 12 as ID, '2024-01-12T20:00:00.000Z'::timestamptz as CREATED_AT, 11 as ORDER_ID, 10 as PRODUCT_ID
      union all
      SELECT 13 as ID, '2024-01-12T20:00:00.000Z'::timestamptz as CREATED_AT, 11 as ORDER_ID, 10 as PRODUCT_ID
      union all
      SELECT 14 as ID, '2024-01-12T20:00:00.000Z'::timestamptz as CREATED_AT, 12 as ORDER_ID, 10 as PRODUCT_ID
      union all
      SELECT 15 as ID, '2024-01-12T20:00:00.000Z'::timestamptz as CREATED_AT, 13 as ORDER_ID, 11 as PRODUCT_ID
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
      union all
      SELECT 11 as ID, 'some category' as PRODUCT_CATEGORY, 'some name' as NAME, 5 as PRICE
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

  if (getEnv('nativeSqlPlanner')) {
    it('multi stage over sub query', async () => dbRunner.runQueryTest({
      measures: ['orders.revenue', 'orders.revenue_1_y_ago', 'orders.cagr_1_y'],
      timeDimensions: [
        {
          dimension: 'orders.date',
          granularity: 'year'
        }
      ],
      timezone: 'UTC'
    }, [
      {
        orders__cagr_1_y: null,
        orders__date_year: '2022-01-01T00:00:00.000Z',
        orders__revenue: '5',
        orders__revenue_1_y_ago: null,
      },
      {
        orders__date_year: '2023-01-01T00:00:00.000Z',
        orders__revenue: '15',
        orders__revenue_1_y_ago: '5',
        orders__cagr_1_y: '2.0000000000000000'
      },
      {
        orders__date_year: '2024-01-01T00:00:00.000Z',
        orders__revenue: '30',
        orders__revenue_1_y_ago: '15',
        orders__cagr_1_y: '1.0000000000000000'
      },
      {
        orders__date_year: '2025-01-01T00:00:00.000Z',
        orders__revenue: '5',
        orders__revenue_1_y_ago: '30',
        orders__cagr_1_y: '-0.83333333333333333333'
      },

      {
        orders__cagr_1_y: null,
        orders__date_year: '2026-01-01T00:00:00.000Z',
        orders__revenue: null,
        orders__revenue_1_y_ago: '5',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }));
    it('multi stage measure filter', async () => dbRunner.runQueryTest({
      dimensions: ['orders.status'],
      timeDimensions: [
        {
          dimension: 'orders.date',
          granularity: 'year'
        }
      ],
      filters: [
        { member: 'orders.cagr_1_y', operator: 'gt', values: ['1.5'] }
      ],
      timezone: 'UTC'
    }, [
      {
        orders__date_year: '2023-01-01T00:00:00.000Z',
        orders__status: 'completed',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }));
  } else {
    // This test is working only in tesseract
    test.skip('multi stage over sub query', () => { expect(1).toBe(1); });
  }
});
