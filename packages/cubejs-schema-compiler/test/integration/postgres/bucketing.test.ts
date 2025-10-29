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
  } else {
    // This test is working only in tesseract
    test.skip('multi stage over sub query', () => { expect(1).toBe(1); });
  }
});
