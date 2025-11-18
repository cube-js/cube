import {
  getEnv,
} from '@cubejs-backend/shared';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Multi-Stage Bucketing', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: >
      SELECT  1 AS id, '2023-03-01T00:00:00Z'::timestamptz AS createdAt, 1 AS customerId, 1000 AS revenue UNION ALL
      SELECT  2 AS id, '2023-09-01T00:00:00Z'::timestamptz AS createdAt, 1 AS customerId, 1100 AS revenue UNION ALL
      SELECT  3 AS id, '2024-03-01T00:00:00Z'::timestamptz AS createdAt, 1 AS customerId, 1300 AS revenue UNION ALL
      SELECT  4 AS id, '2024-09-01T00:00:00Z'::timestamptz AS createdAt, 1 AS customerId, 1400 AS revenue UNION ALL
      SELECT  5 AS id, '2025-03-01T00:00:00Z'::timestamptz AS createdAt, 1 AS customerId, 1600 AS revenue UNION ALL
      SELECT  6 AS id, '2025-09-01T00:00:00Z'::timestamptz AS createdAt, 1 AS customerId, 1700 AS revenue UNION ALL

      SELECT  7 AS id, '2023-03-01T00:00:00Z'::timestamptz AS createdAt, 2 AS customerId, 2000 AS revenue UNION ALL
      SELECT  8 AS id, '2023-09-01T00:00:00Z'::timestamptz AS createdAt, 2 AS customerId, 2100 AS revenue UNION ALL
      SELECT  9 AS id, '2024-03-01T00:00:00Z'::timestamptz AS createdAt, 2 AS customerId, 2300 AS revenue UNION ALL
      SELECT 10 AS id, '2024-09-01T00:00:00Z'::timestamptz AS createdAt, 2 AS customerId, 2500 AS revenue UNION ALL
      SELECT 11 AS id, '2025-03-01T00:00:00Z'::timestamptz AS createdAt, 2 AS customerId, 2700 AS revenue UNION ALL
      SELECT 12 AS id, '2025-09-01T00:00:00Z'::timestamptz AS createdAt, 2 AS customerId, 2900 AS revenue UNION ALL

      SELECT 13 AS id, '2023-03-01T00:00:00Z'::timestamptz AS createdAt, 3 AS customerId, 3000 AS revenue UNION ALL
      SELECT 14 AS id, '2023-09-01T00:00:00Z'::timestamptz AS createdAt, 3 AS customerId, 2800 AS revenue UNION ALL
      SELECT 15 AS id, '2024-03-01T00:00:00Z'::timestamptz AS createdAt, 3 AS customerId, 2500 AS revenue UNION ALL
      SELECT 16 AS id, '2024-09-01T00:00:00Z'::timestamptz AS createdAt, 3 AS customerId, 2300 AS revenue UNION ALL
      SELECT 17 AS id, '2025-03-01T00:00:00Z'::timestamptz AS createdAt, 3 AS customerId, 2100 AS revenue UNION ALL
      SELECT 18 AS id, '2025-09-01T00:00:00Z'::timestamptz AS createdAt, 3 AS customerId, 1900 AS revenue UNION ALL

      SELECT 19 AS id, '2023-03-01T00:00:00Z'::timestamptz AS createdAt, 4 AS customerId, 4000 AS revenue UNION ALL
      SELECT 20 AS id, '2023-09-01T00:00:00Z'::timestamptz AS createdAt, 4 AS customerId, 4200 AS revenue UNION ALL
      SELECT 21 AS id, '2024-03-01T00:00:00Z'::timestamptz AS createdAt, 4 AS customerId, 3900 AS revenue UNION ALL
      SELECT 22 AS id, '2024-09-01T00:00:00Z'::timestamptz AS createdAt, 4 AS customerId, 3700 AS revenue UNION ALL
      SELECT 23 AS id, '2025-03-01T00:00:00Z'::timestamptz AS createdAt, 4 AS customerId, 3400 AS revenue UNION ALL
      SELECT 24 AS id, '2025-09-01T00:00:00Z'::timestamptz AS createdAt, 4 AS customerId, 3200 AS revenue UNION ALL

      SELECT 25 AS id, '2023-03-01T00:00:00Z'::timestamptz AS createdAt, 5 AS customerId, 1500 AS revenue UNION ALL
      SELECT 26 AS id, '2023-09-01T00:00:00Z'::timestamptz AS createdAt, 5 AS customerId, 1700 AS revenue UNION ALL
      SELECT 27 AS id, '2024-03-01T00:00:00Z'::timestamptz AS createdAt, 5 AS customerId, 2000 AS revenue UNION ALL
      SELECT 28 AS id, '2024-09-01T00:00:00Z'::timestamptz AS createdAt, 5 AS customerId, 2200 AS revenue UNION ALL
      SELECT 29 AS id, '2025-03-01T00:00:00Z'::timestamptz AS createdAt, 5 AS customerId, 2500 AS revenue UNION ALL
      SELECT 30 AS id, '2025-09-01T00:00:00Z'::timestamptz AS createdAt, 5 AS customerId, 2700 AS revenue UNION ALL

      SELECT 31 AS id, '2023-03-01T00:00:00Z'::timestamptz AS createdAt, 6 AS customerId, 4500 AS revenue UNION ALL
      SELECT 32 AS id, '2023-09-01T00:00:00Z'::timestamptz AS createdAt, 6 AS customerId, 4300 AS revenue UNION ALL
      SELECT 33 AS id, '2024-03-01T00:00:00Z'::timestamptz AS createdAt, 6 AS customerId, 4100 AS revenue UNION ALL
      SELECT 34 AS id, '2024-09-01T00:00:00Z'::timestamptz AS createdAt, 6 AS customerId, 3900 AS revenue UNION ALL
      SELECT 35 AS id, '2025-03-01T00:00:00Z'::timestamptz AS createdAt, 6 AS customerId, 3700 AS revenue UNION ALL
      SELECT 36 AS id, '2025-09-01T00:00:00Z'::timestamptz AS createdAt, 6 AS customerId, 3500 AS revenue

    dimensions:
      - name: id
        sql: ID
        type: number
        primary_key: true

      - name: customerId
        sql: customerId
        type: number

      - name: createdAt
        sql: createdAt
        type: time

      - name: changeType
        sql: "CONCAT('Revenue is ', {revenueChangeType})"
        multi_stage: true
        type: string
        add_group_by: [orders.customerId]

      - name: changeTypeComplex
        sql: >
          CASE
            WHEN {revenueYearAgo} IS NULL THEN 'New'
            WHEN {revenue} > {revenueYearAgo} THEN 'Grow'
            ELSE 'Down'
          END
        multi_stage: true
        type: string
        add_group_by: [orders.customerId]

      - name: changeTypeComplexWithJoin
        sql: >
          CASE
            WHEN {revenueYearAgo} IS NULL THEN 'New'
            WHEN {revenue} > {revenueYearAgo} THEN 'Grow'
            ELSE 'Down'
          END
        multi_stage: true
        type: string
        add_group_by: [first_date.customerId]

      - name: changeTypeConcat
        sql: "CONCAT({changeTypeComplex}, '-test')"
        type: string
        multi_stage: true

      - name: twoDimsConcat
        sql: "CONCAT({changeTypeComplex}, '-', {first_date.customerType2})"
        type: string
        multi_stage: true


    measures:
      - name: count
        type: count

      - name: revenue
        sql: revenue
        type: sum

      - name: revenueYearAgo
        sql: "{revenue}"
        multi_stage: true
        type: number
        time_shift:
          - time_dimension: orders.createdAt
            interval: 1 year
            type: prior

      - name: revenueChangeType
        sql: >
          CASE
            WHEN {revenueYearAgo} IS NULL THEN 'New'
            WHEN {revenue} > {revenueYearAgo} THEN 'Grow'
            ELSE 'Down'
          END
        type: string

  - name: first_date
    sql: >
      SELECT  1 AS id, '2023-03-01T00:00:00Z'::timestamptz AS createdAt, 1 AS customerId UNION ALL
      SELECT  8 AS id, '2023-09-01T00:00:00Z'::timestamptz AS createdAt, 2 AS customerId UNION ALL
      SELECT 16 AS id, '2024-09-01T00:00:00Z'::timestamptz AS createdAt, 3 AS customerId UNION ALL
      SELECT 23 AS id, '2025-03-01T00:00:00Z'::timestamptz AS createdAt, 4 AS customerId UNION ALL
      SELECT 29 AS id, '2025-03-01T00:00:00Z'::timestamptz AS createdAt, 5 AS customerId UNION ALL
      SELECT 36 AS id, '2025-09-01T00:00:00Z'::timestamptz AS createdAt, 6 AS customerId

    joins:
    - name: orders
      sql: "{first_date.customerId} = {orders.customerId}"
      relationship: one_to_many

    dimensions:
      - name: customerId
        sql: customerId
        type: number

      - name: createdAt
        sql: createdAt
        type: time

      - name: customerType
        sql: >
          CASE
            WHEN {orders.revenue} < 10000 THEN 'Low'
            WHEN {orders.revenue} < 20000 THEN 'Medium'
            ELSE 'Top'
          END
        multi_stage: true
        type: string
        add_group_by: [first_date.customerId]

      - name: customerType2
        sql: >
          CASE
            WHEN {orders.revenue} < 3000 THEN 'Low'
            ELSE 'Top'
          END
        multi_stage: true
        type: string
        add_group_by: [first_date.customerId]

      - name: customerTypeConcat
        sql: "CONCAT('Customer type: ', {customerType})"
        multi_stage: true
        type: string
        add_group_by: [first_date.customerId]


    `);

  if (getEnv('nativeSqlPlanner')) {
    it('simple bucketing', async () => dbRunner.runQueryTest({
      dimensions: ['orders.changeType'],
      measures: ['orders.count', 'orders.revenue'],
      timeDimensions: [
        {
          dimension: 'orders.createdAt',
          granularity: 'year',
          dateRange: ['2024-01-02T00:00:00', '2026-01-01T00:00:00']
        }
      ],
      timezone: 'UTC',
      order: [{
        id: 'orders.changeType'
      }, { id: 'orders.createdAt' }],
    }, [
      {
        orders__change_type: 'Revenue is Down',
        orders__created_at_year: '2024-01-01T00:00:00.000Z',
        orders__count: '6',
        orders__revenue: '20400'
      },
      {
        orders__change_type: 'Revenue is Down',
        orders__created_at_year: '2025-01-01T00:00:00.000Z',
        orders__count: '6',
        orders__revenue: '17800'
      },
      {
        orders__change_type: 'Revenue is Grow',
        orders__created_at_year: '2024-01-01T00:00:00.000Z',
        orders__count: '6',
        orders__revenue: '11700'
      },
      {
        orders__change_type: 'Revenue is Grow',
        orders__created_at_year: '2025-01-01T00:00:00.000Z',
        orders__count: '6',
        orders__revenue: '14100'
      }
    ],
    { joinGraph, cubeEvaluator, compiler }));

    it('bucketing with multistage measure', async () => dbRunner.runQueryTest({
      dimensions: ['orders.changeType'],
      measures: ['orders.revenue', 'orders.revenueYearAgo'],
      timeDimensions: [
        {
          dimension: 'orders.createdAt',
          granularity: 'year',
          dateRange: ['2024-01-02T00:00:00', '2026-01-01T00:00:00']
        }
      ],
      timezone: 'UTC',
      order: [{
        id: 'orders.changeType'
      }, { id: 'orders.createdAt' }],
    },
    [
      {
        orders__change_type: 'Revenue is Down',
        orders__created_at_year: '2024-01-01T00:00:00.000Z',
        orders__revenue: '20400',
        orders__revenue_year_ago: '22800'
      },
      {
        orders__change_type: 'Revenue is Down',
        orders__created_at_year: '2025-01-01T00:00:00.000Z',
        orders__revenue: '17800',
        orders__revenue_year_ago: '20400'
      },
      {
        orders__change_type: 'Revenue is Grow',
        orders__created_at_year: '2024-01-01T00:00:00.000Z',
        orders__revenue: '11700',
        orders__revenue_year_ago: '9400'
      },
      {
        orders__change_type: 'Revenue is Grow',
        orders__created_at_year: '2025-01-01T00:00:00.000Z',
        orders__revenue: '14100',
        orders__revenue_year_ago: '11700'
      },
    ],
    { joinGraph, cubeEvaluator, compiler }));
    it('bucketing with complex bucket dimension', async () => dbRunner.runQueryTest({
      dimensions: ['orders.changeTypeComplex'],
      measures: ['orders.revenue', 'orders.revenueYearAgo'],
      timeDimensions: [
        {
          dimension: 'orders.createdAt',
          granularity: 'year',
          dateRange: ['2024-01-02T00:00:00', '2026-01-01T00:00:00']
        }
      ],
      timezone: 'UTC',
      order: [{
        id: 'orders.changeTypeComplex'
      }, { id: 'orders.createdAt' }],
    },
    [
      {
        orders__change_type_complex: 'Down',
        orders__created_at_year: '2024-01-01T00:00:00.000Z',
        orders__revenue: '20400',
        orders__revenue_year_ago: '22800'
      },
      {
        orders__change_type_complex: 'Down',
        orders__created_at_year: '2025-01-01T00:00:00.000Z',
        orders__revenue: '17800',
        orders__revenue_year_ago: '20400'
      },
      {
        orders__change_type_complex: 'Grow',
        orders__created_at_year: '2024-01-01T00:00:00.000Z',
        orders__revenue: '11700',
        orders__revenue_year_ago: '9400'
      },
      {
        orders__change_type_complex: 'Grow',
        orders__created_at_year: '2025-01-01T00:00:00.000Z',
        orders__revenue: '14100',
        orders__revenue_year_ago: '11700'
      },
    ],
    { joinGraph, cubeEvaluator, compiler }));
    it('bucketing with dimension over complex dimension', async () => dbRunner.runQueryTest({
      dimensions: ['orders.changeTypeConcat'],
      measures: ['orders.revenue', 'orders.revenueYearAgo'],
      timeDimensions: [
        {
          dimension: 'orders.createdAt',
          granularity: 'year',
          dateRange: ['2024-01-02T00:00:00', '2026-01-01T00:00:00']
        }
      ],
      timezone: 'UTC',
      order: [{
        id: 'orders.changeTypeConcat'
      }, { id: 'orders.createdAt' }],
    },
    [
      {
        orders__change_type_concat: 'Down-test',
        orders__created_at_year: '2024-01-01T00:00:00.000Z',
        orders__revenue: '20400',
        orders__revenue_year_ago: '22800'
      },
      {
        orders__change_type_concat: 'Down-test',
        orders__created_at_year: '2025-01-01T00:00:00.000Z',
        orders__revenue: '17800',
        orders__revenue_year_ago: '20400'
      },
      {
        orders__change_type_concat: 'Grow-test',
        orders__created_at_year: '2024-01-01T00:00:00.000Z',
        orders__revenue: '11700',
        orders__revenue_year_ago: '9400'
      },
      {
        orders__change_type_concat: 'Grow-test',
        orders__created_at_year: '2025-01-01T00:00:00.000Z',
        orders__revenue: '14100',
        orders__revenue_year_ago: '11700'
      },
    ],
    { joinGraph, cubeEvaluator, compiler }));
    it('bucketing with join and bucket dimension', async () => dbRunner.runQueryTest({
      dimensions: ['orders.changeTypeComplexWithJoin'],
      measures: ['orders.revenue', 'orders.revenueYearAgo'],
      timeDimensions: [
        {
          dimension: 'orders.createdAt',
          granularity: 'year',
          dateRange: ['2024-01-02T00:00:00', '2026-01-01T00:00:00']
        }
      ],
      timezone: 'UTC',
      order: [{
        id: 'orders.changeTypeComplexWithJoin'
      }, { id: 'orders.createdAt' }],
    },
    [
      {
        orders__change_type_complex_with_join: 'Down',
        orders__created_at_year: '2024-01-01T00:00:00.000Z',
        orders__revenue: '20400',
        orders__revenue_year_ago: '22800'
      },
      {
        orders__change_type_complex_with_join: 'Down',
        orders__created_at_year: '2025-01-01T00:00:00.000Z',
        orders__revenue: '17800',
        orders__revenue_year_ago: '20400'
      },
      {
        orders__change_type_complex_with_join: 'Grow',
        orders__created_at_year: '2024-01-01T00:00:00.000Z',
        orders__revenue: '11700',
        orders__revenue_year_ago: '9400'
      },
      {
        orders__change_type_complex_with_join: 'Grow',
        orders__created_at_year: '2025-01-01T00:00:00.000Z',
        orders__revenue: '14100',
        orders__revenue_year_ago: '11700'
      },
    ],
    { joinGraph, cubeEvaluator, compiler }));
    it('bucketing dim reference other cube measure', async () => dbRunner.runQueryTest({
      dimensions: ['first_date.customerType'],
      measures: ['orders.revenue'],
      timezone: 'UTC',
      order: [{
        id: 'first_date.customerType'
      }],
    },
    [
      { first_date__customer_type: 'Low', orders__revenue: '8100' },
      { first_date__customer_type: 'Medium', orders__revenue: '41700' },
      { first_date__customer_type: 'Top', orders__revenue: '46400' }
    ],
    { joinGraph, cubeEvaluator, compiler }));
    it('bucketing with two dimensions', async () => dbRunner.runQueryTest({
      dimensions: ['orders.changeTypeConcat', 'first_date.customerType2'],
      measures: ['orders.revenue', 'orders.revenueYearAgo'],
      timeDimensions: [
        {
          dimension: 'orders.createdAt',
          granularity: 'year',
          dateRange: ['2024-01-02T00:00:00', '2026-01-01T00:00:00']
        }
      ],
      timezone: 'UTC',
      order: [{
        id: 'orders.changeTypeConcat'
      }, { id: 'orders.createdAt' }],
    },
    [
      {
        orders__change_type_concat: 'Down-test',
        first_date__customer_type2: 'Top',
        orders__created_at_year: '2024-01-01T00:00:00.000Z',
        orders__revenue: '20400',
        orders__revenue_year_ago: '22800'
      },
      {
        orders__change_type_concat: 'Down-test',
        first_date__customer_type2: 'Top',
        orders__created_at_year: '2025-01-01T00:00:00.000Z',
        orders__revenue: '17800',
        orders__revenue_year_ago: '20400'
      },
      {
        orders__change_type_concat: 'Grow-test',
        first_date__customer_type2: 'Low',
        orders__created_at_year: '2024-01-01T00:00:00.000Z',
        orders__revenue: '2700',
        orders__revenue_year_ago: '2100'
      },
      {
        orders__change_type_concat: 'Grow-test',
        first_date__customer_type2: 'Top',
        orders__created_at_year: '2024-01-01T00:00:00.000Z',
        orders__revenue: '9000',
        orders__revenue_year_ago: '7300'
      },
      {
        orders__change_type_concat: 'Grow-test',
        first_date__customer_type2: 'Top',
        orders__created_at_year: '2025-01-01T00:00:00.000Z',
        orders__revenue: '14100',
        orders__revenue_year_ago: '11700'
      }
    ],
    { joinGraph, cubeEvaluator, compiler }));
    it('bucketing with two dims concacted', async () => dbRunner.runQueryTest({
      dimensions: ['orders.twoDimsConcat'],
      measures: ['orders.revenue', 'orders.revenueYearAgo'],
      timeDimensions: [
        {
          dimension: 'orders.createdAt',
          granularity: 'year',
          dateRange: ['2024-01-02T00:00:00', '2026-01-01T00:00:00']
        }
      ],
      timezone: 'UTC',
      order: [{
        id: 'orders.twoDimsConcat'
      }, { id: 'orders.createdAt' }],
    },
    [
      {
        orders__two_dims_concat: 'Down-Top',
        orders__created_at_year: '2024-01-01T00:00:00.000Z',
        orders__revenue: '20400',
        orders__revenue_year_ago: '22800'
      },
      {
        orders__two_dims_concat: 'Down-Top',
        orders__created_at_year: '2025-01-01T00:00:00.000Z',
        orders__revenue: '17800',
        orders__revenue_year_ago: '20400'
      },
      {
        orders__two_dims_concat: 'Grow-Low',
        orders__created_at_year: '2024-01-01T00:00:00.000Z',
        orders__revenue: '2700',
        orders__revenue_year_ago: '2100'
      },
      {
        orders__two_dims_concat: 'Grow-Top',
        orders__created_at_year: '2024-01-01T00:00:00.000Z',
        orders__revenue: '9000',
        orders__revenue_year_ago: '7300'
      },
      {
        orders__two_dims_concat: 'Grow-Top',
        orders__created_at_year: '2025-01-01T00:00:00.000Z',
        orders__revenue: '14100',
        orders__revenue_year_ago: '11700'
      }
    ],
    { joinGraph, cubeEvaluator, compiler }));
  } else {
    // This test is working only in tesseract
    test.skip('multi stage over sub query', () => { expect(1).toBe(1); });
  }
});
