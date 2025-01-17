import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Custom Granularities', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
  cubes:
    - name: orders
      sql: >
        SELECT order_id,
               created_at,
               CASE
                   WHEN order_id % 3 = 1 THEN 'processing'
                   WHEN order_id % 3 = 2 THEN 'completed'
                   ELSE 'shipped'
                   END AS status
        FROM (SELECT GENERATE_SERIES(1, 60)                                           AS order_id,
                     GENERATE_SERIES('2024-01-01'::date, '2026-04-10'::date, '2 weeks') AS created_at) AS subquery

      dimensions:
        - name: order_id
          sql: order_id
          type: number
          primary_key: true
          public: true

        - name: status
          sql: status
          type: string

        - name: createdAtHalfYear
          sql: "{createdAt.half_year}"
          type: string

        - name: createdAt
          sql: created_at
          type: time
          granularities:
            - name: half_year
              interval: 6 months
              origin: '2024-01-01' # to keep tests stable across time (year change, etc)
            - name: half_year_by_1st_april
              interval: 6 months
              #offset: 3 months
              origin: '2024-04-01' # to keep tests stable across time (year change, etc)
            - name: two_weeks_by_friday
              interval: 2 weeks
              origin: '2024-08-23'
            - name: one_hour_by_5min_offset
              interval: 1 hour
              offset: 5 minutes
            - name: twenty_five_minutes
              interval: 25 minutes
              origin: '2024-01-01 10:15:00'
            - name: fiscal_year_by_1st_feb
              interval: 1 year
              origin: '2024-02-01'
            - name: fiscal_year_by_15th_march
              interval: 1 year
              origin: '2024-03-15'

      measures:
        - name: count
          type: count

        - name: rollingCountByTrailing3Months
          type: count
          rolling_window:
            trailing: 3 months

        - name: rollingCountByLeading4Months
          type: count
          rolling_window:
            leading: 4 months

        - name: rollingCountByUnbounded
          type: count
          rolling_window:
            trailing: unbounded

  views:
    - name: orders_view
      cubes:
        - join_path: orders
          includes: "*"
  `);

  it('works with half_year custom granularity w/o dimensions query', async () => dbRunner.runQueryTest(
    {
      measures: ['orders.count'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        granularity: 'half_year',
        dateRange: ['2024-01-01', '2025-12-31']
      }],
      dimensions: [],
      filters: [],
      timezone: 'Europe/London'
    },
    [
      {
        orders__count: '13',
        orders__created_at_half_year: '2024-01-01T00:00:00.000Z',
      },
      {
        orders__count: '13',
        orders__created_at_half_year: '2024-07-01T00:00:00.000Z',
      },
      {
        orders__count: '13',
        orders__created_at_half_year: '2025-01-01T00:00:00.000Z',
      },
      {
        orders__count: '13',
        orders__created_at_half_year: '2025-07-01T00:00:00.000Z',
      },
      {
        orders__count: '1',
        orders__created_at_half_year: '2026-01-01T00:00:00.000Z',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  it('works with proxied createdAtHalfYear custom granularity as dimension query', async () => dbRunner.runQueryTest(
    {
      measures: ['orders.count'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        dateRange: ['2024-01-01', '2025-12-31']
      }],
      dimensions: ['orders.createdAtHalfYear'],
      filters: [],
      timezone: 'Europe/London'
    },
    [
      {
        orders__count: '13',
        orders__created_at_half_year: '2024-01-01T00:00:00.000Z',
      },
      {
        orders__count: '13',
        orders__created_at_half_year: '2024-07-01T00:00:00.000Z',
      },
      {
        orders__count: '13',
        orders__created_at_half_year: '2025-01-01T00:00:00.000Z',
      },
      {
        orders__count: '13',
        orders__created_at_half_year: '2025-07-01T00:00:00.000Z',
      },
      {
        orders__count: '1',
        orders__created_at_half_year: '2026-01-01T00:00:00.000Z',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  it('works with half_year custom granularity w/o dimensions querying view', async () => dbRunner.runQueryTest(
    {
      measures: ['orders_view.count'],
      timeDimensions: [{
        dimension: 'orders_view.createdAt',
        granularity: 'half_year',
        dateRange: ['2024-01-01', '2025-12-31']
      }],
      dimensions: [],
      filters: [],
      timezone: 'Europe/London'
    },
    [
      {
        orders_view__count: '13',
        orders_view__created_at_half_year: '2024-01-01T00:00:00.000Z',
      },
      {
        orders_view__count: '13',
        orders_view__created_at_half_year: '2024-07-01T00:00:00.000Z',
      },
      {
        orders_view__count: '13',
        orders_view__created_at_half_year: '2025-01-01T00:00:00.000Z',
      },
      {
        orders_view__count: '13',
        orders_view__created_at_half_year: '2025-07-01T00:00:00.000Z',
      },
      {
        orders_view__count: '1',
        orders_view__created_at_half_year: '2026-01-01T00:00:00.000Z',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  it('works with proxied createdAtHalfYear custom granularity as dimension querying view', async () => dbRunner.runQueryTest(
    {
      measures: ['orders_view.count'],
      timeDimensions: [{
        dimension: 'orders_view.createdAt',
        dateRange: ['2024-01-01', '2025-12-31']
      }],
      dimensions: ['orders_view.createdAtHalfYear'],
      filters: [],
      timezone: 'Europe/London'
    },
    [
      {
        orders_view__count: '13',
        orders_view__created_at_half_year: '2024-01-01T00:00:00.000Z',
      },
      {
        orders_view__count: '13',
        orders_view__created_at_half_year: '2024-07-01T00:00:00.000Z',
      },
      {
        orders_view__count: '13',
        orders_view__created_at_half_year: '2025-01-01T00:00:00.000Z',
      },
      {
        orders_view__count: '13',
        orders_view__created_at_half_year: '2025-07-01T00:00:00.000Z',
      },
      {
        orders_view__count: '1',
        orders_view__created_at_half_year: '2026-01-01T00:00:00.000Z',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  it('works with half_year_by_1st_april custom granularity w/o dimensions query', async () => dbRunner.runQueryTest(
    {
      measures: ['orders.count'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        granularity: 'half_year_by_1st_april',
        dateRange: ['2024-01-01', '2025-12-31']
      }],
      dimensions: [],
      filters: [],
      timezone: 'Europe/London'
    },
    [
      {
        orders__count: '7',
        orders__created_at_half_year_by_1st_april: '2023-10-01T00:00:00.000Z',
      },
      {
        orders__count: '13',
        orders__created_at_half_year_by_1st_april: '2024-04-01T00:00:00.000Z',
      },
      {
        orders__count: '13',
        orders__created_at_half_year_by_1st_april: '2024-10-01T00:00:00.000Z',
      },
      {
        orders__count: '13',
        orders__created_at_half_year_by_1st_april: '2025-04-01T00:00:00.000Z',
      },
      {
        orders__count: '7',
        orders__created_at_half_year_by_1st_april: '2025-10-01T00:00:00.000Z',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  it('works with half_year custom granularity with dimension query', async () => dbRunner.runQueryTest(
    {
      measures: ['orders.count'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        granularity: 'half_year',
        dateRange: ['2024-01-01', '2025-12-31']
      }],
      dimensions: ['orders.status'],
      filters: [],
      order: [{ id: 'orders.createdAt' }, { id: 'orders.status' }],
      timezone: 'Europe/London'
    },
    [
      {
        orders__count: '4',
        orders__created_at_half_year: '2024-01-01T00:00:00.000Z',
        orders__status: 'completed',
      },
      {
        orders__count: '5',
        orders__created_at_half_year: '2024-01-01T00:00:00.000Z',
        orders__status: 'processing',
      },
      {
        orders__count: '4',
        orders__created_at_half_year: '2024-01-01T00:00:00.000Z',
        orders__status: 'shipped',
      },
      {
        orders__count: '5',
        orders__created_at_half_year: '2024-07-01T00:00:00.000Z',
        orders__status: 'completed',
      },
      {
        orders__count: '4',
        orders__created_at_half_year: '2024-07-01T00:00:00.000Z',
        orders__status: 'processing',
      },
      {
        orders__count: '4',
        orders__created_at_half_year: '2024-07-01T00:00:00.000Z',
        orders__status: 'shipped',
      },
      {
        orders__count: '4',
        orders__created_at_half_year: '2025-01-01T00:00:00.000Z',
        orders__status: 'completed',
      },
      {
        orders__count: '4',
        orders__created_at_half_year: '2025-01-01T00:00:00.000Z',
        orders__status: 'processing',
      },
      {
        orders__count: '5',
        orders__created_at_half_year: '2025-01-01T00:00:00.000Z',
        orders__status: 'shipped',
      },
      {
        orders__count: '4',
        orders__created_at_half_year: '2025-07-01T00:00:00.000Z',
        orders__status: 'completed',
      },
      {
        orders__count: '5',
        orders__created_at_half_year: '2025-07-01T00:00:00.000Z',
        orders__status: 'processing',
      },
      {
        orders__count: '4',
        orders__created_at_half_year: '2025-07-01T00:00:00.000Z',
        orders__status: 'shipped',
      },
      {
        orders__count: '1',
        orders__created_at_half_year: '2026-01-01T00:00:00.000Z',
        orders__status: 'completed',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  it('works with half_year_by_1st_april custom with dimension granularity query', async () => dbRunner.runQueryTest(
    {
      measures: ['orders.count'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        granularity: 'half_year_by_1st_april',
        dateRange: ['2024-01-01', '2025-12-31']
      }],
      dimensions: ['orders.status'],
      filters: [],
      order: [{ id: 'orders.createdAt' }, { id: 'orders.status' }],
      timezone: 'Europe/London'
    },
    [
      {
        orders__count: '2',
        orders__created_at_half_year_by_1st_april: '2023-10-01T00:00:00.000Z',
        orders__status: 'completed',
      },
      {
        orders__count: '3',
        orders__created_at_half_year_by_1st_april: '2023-10-01T00:00:00.000Z',
        orders__status: 'processing',
      },
      {
        orders__count: '2',
        orders__created_at_half_year_by_1st_april: '2023-10-01T00:00:00.000Z',
        orders__status: 'shipped',
      },
      {
        orders__count: '5',
        orders__created_at_half_year_by_1st_april: '2024-04-01T00:00:00.000Z',
        orders__status: 'completed',
      },
      {
        orders__count: '4',
        orders__created_at_half_year_by_1st_april: '2024-04-01T00:00:00.000Z',
        orders__status: 'processing',
      },
      {
        orders__count: '4',
        orders__created_at_half_year_by_1st_april: '2024-04-01T00:00:00.000Z',
        orders__status: 'shipped',
      },
      {
        orders__count: '4',
        orders__created_at_half_year_by_1st_april: '2024-10-01T00:00:00.000Z',
        orders__status: 'completed',
      },
      {
        orders__count: '4',
        orders__created_at_half_year_by_1st_april: '2024-10-01T00:00:00.000Z',
        orders__status: 'processing',
      },
      {
        orders__count: '5',
        orders__created_at_half_year_by_1st_april: '2024-10-01T00:00:00.000Z',
        orders__status: 'shipped',
      },
      {
        orders__count: '4',
        orders__created_at_half_year_by_1st_april: '2025-04-01T00:00:00.000Z',
        orders__status: 'completed',
      },
      {
        orders__count: '5',
        orders__created_at_half_year_by_1st_april: '2025-04-01T00:00:00.000Z',
        orders__status: 'processing',
      },
      {
        orders__count: '4',
        orders__created_at_half_year_by_1st_april: '2025-04-01T00:00:00.000Z',
        orders__status: 'shipped',
      },
      {
        orders__count: '3',
        orders__created_at_half_year_by_1st_april: '2025-10-01T00:00:00.000Z',
        orders__status: 'completed',
      },
      {
        orders__count: '2',
        orders__created_at_half_year_by_1st_april: '2025-10-01T00:00:00.000Z',
        orders__status: 'processing',
      },
      {
        orders__count: '2',
        orders__created_at_half_year_by_1st_april: '2025-10-01T00:00:00.000Z',
        orders__status: 'shipped',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  it('works with half_year custom granularity w/o dimensions with unbounded rolling window query', async () => dbRunner.runQueryTest(
    {
      measures: ['orders.rollingCountByUnbounded'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        granularity: 'half_year',
        dateRange: ['2024-01-01', '2025-12-31']
      }],
      dimensions: [],
      filters: [],
      timezone: 'Europe/London'
    },
    [
      {
        orders__created_at_half_year: '2024-01-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '13',
      },
      {
        orders__created_at_half_year: '2024-07-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '27',
      },
      {
        orders__created_at_half_year: '2025-01-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '40',
      },
      {
        orders__created_at_half_year: '2025-07-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '53',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  it('works with half_year custom granularity with dimension with unbounded rolling window query', async () => dbRunner.runQueryTest(
    {
      measures: ['orders.rollingCountByUnbounded'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        granularity: 'half_year',
        dateRange: ['2024-01-01', '2025-12-31']
      }],
      dimensions: ['orders.status'],
      filters: [],
      order: [{ id: 'orders.createdAt' }, { id: 'orders.status' }],
      timezone: 'Europe/London'
    },
    [
      {
        orders__created_at_half_year: '2024-01-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '4',
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: '2024-01-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '5',
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: '2024-01-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '4',
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year: '2024-07-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '9',
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: '2024-07-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '9',
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: '2024-07-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '9',
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year: '2025-01-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '13',
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: '2025-01-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '14',
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: '2025-01-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '13',
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year: '2025-07-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '18',
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: '2025-07-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '18',
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: '2025-07-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '17',
        orders__status: 'shipped',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  it('works with half_year_by_1st_april custom granularity with dimension with unbounded rolling window query', async () => dbRunner.runQueryTest(
    {
      measures: ['orders.rollingCountByUnbounded'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        granularity: 'half_year_by_1st_april',
        dateRange: ['2024-01-01', '2025-12-31']
      }],
      dimensions: ['orders.status'],
      filters: [],
      order: [{ id: 'orders.createdAt' }, { id: 'orders.status' }],
      timezone: 'Europe/London'
    },
    [
      {
        orders__created_at_half_year_by_1st_april: '2023-10-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '2',
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year_by_1st_april: '2023-10-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '3',
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year_by_1st_april: '2023-10-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '2',
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year_by_1st_april: '2024-04-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '7',
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year_by_1st_april: '2024-04-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '7',
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year_by_1st_april: '2024-04-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '6',
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year_by_1st_april: '2024-10-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '11',
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year_by_1st_april: '2024-10-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '11',
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year_by_1st_april: '2024-10-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '11',
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year_by_1st_april: '2025-04-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '15',
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year_by_1st_april: '2025-04-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '16',
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year_by_1st_april: '2025-04-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '15',
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year_by_1st_april: '2025-10-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '20',
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year_by_1st_april: '2025-10-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '20',
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year_by_1st_april: '2025-10-01T00:00:00.000Z',
        orders__rolling_count_by_unbounded: '19',
        orders__status: 'shipped',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  it('works with half_year custom granularity w/o dimensions with trailing rolling window query', async () => dbRunner.runQueryTest(
    {
      measures: ['orders.rollingCountByTrailing3Months'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        granularity: 'half_year',
        dateRange: ['2024-01-01', '2025-12-31']
      }],
      dimensions: [],
      filters: [],
      timezone: 'Europe/London'
    },
    [
      {
        orders__created_at_half_year: '2024-01-01T00:00:00.000Z',
        orders__rolling_count_by_trailing3_months: '6',
      },
      {
        orders__created_at_half_year: '2024-07-01T00:00:00.000Z',
        orders__rolling_count_by_trailing3_months: '7',
      },
      {
        orders__created_at_half_year: '2025-01-01T00:00:00.000Z',
        orders__rolling_count_by_trailing3_months: '7',
      },
      {
        orders__created_at_half_year: '2025-07-01T00:00:00.000Z',
        orders__rolling_count_by_trailing3_months: '7',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  it('works with half_year custom granularity with dimension with trailing rolling window query', async () => dbRunner.runQueryTest(
    {
      measures: ['orders.rollingCountByTrailing3Months'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        granularity: 'half_year',
        dateRange: ['2024-01-01', '2025-12-31']
      }],
      dimensions: ['orders.status'],
      filters: [],
      order: [{ id: 'orders.createdAt' }, { id: 'orders.status' }],
      timezone: 'Europe/London'
    },
    [
      {
        orders__created_at_half_year: '2024-01-01T00:00:00.000Z',
        orders__rolling_count_by_trailing3_months: '2',
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: '2024-01-01T00:00:00.000Z',
        orders__rolling_count_by_trailing3_months: '2',
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: '2024-01-01T00:00:00.000Z',
        orders__rolling_count_by_trailing3_months: '2',
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year: '2024-07-01T00:00:00.000Z',
        orders__rolling_count_by_trailing3_months: '2',
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: '2024-07-01T00:00:00.000Z',
        orders__rolling_count_by_trailing3_months: '2',
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: '2024-07-01T00:00:00.000Z',
        orders__rolling_count_by_trailing3_months: '3',
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year: '2025-01-01T00:00:00.000Z',
        orders__rolling_count_by_trailing3_months: '2',
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: '2025-01-01T00:00:00.000Z',
        orders__rolling_count_by_trailing3_months: '3',
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: '2025-01-01T00:00:00.000Z',
        orders__rolling_count_by_trailing3_months: '2',
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year: '2025-07-01T00:00:00.000Z',
        orders__rolling_count_by_trailing3_months: '3',
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: '2025-07-01T00:00:00.000Z',
        orders__rolling_count_by_trailing3_months: '2',
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: '2025-07-01T00:00:00.000Z',
        orders__rolling_count_by_trailing3_months: '2',
        orders__status: 'shipped',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  it('works with half_year_by_1st_april custom granularity w/o dimensions with trailing rolling window query', async () => dbRunner.runQueryTest(
    {
      measures: ['orders.rollingCountByTrailing3Months'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        granularity: 'half_year_by_1st_april',
        dateRange: ['2024-01-01', '2025-12-31']
      }],
      dimensions: [],
      filters: [],
      timezone: 'Europe/London'
    },
    [
      {
        orders__created_at_half_year_by_1st_april: '2023-10-01T00:00:00.000Z',
        orders__rolling_count_by_trailing3_months: '7',
      },
      {
        orders__created_at_half_year_by_1st_april: '2024-04-01T00:00:00.000Z',
        orders__rolling_count_by_trailing3_months: '7',
      },
      {
        orders__created_at_half_year_by_1st_april: '2024-10-01T00:00:00.000Z',
        orders__rolling_count_by_trailing3_months: '6',
      },
      {
        orders__created_at_half_year_by_1st_april: '2025-04-01T00:00:00.000Z',
        orders__rolling_count_by_trailing3_months: '6',
      },
      {
        orders__created_at_half_year_by_1st_april: '2025-10-01T00:00:00.000Z',
        orders__rolling_count_by_trailing3_months: '6',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  it('works with half_year custom granularity w/o dimensions with leading rolling window query', async () => dbRunner.runQueryTest(
    {
      measures: ['orders.rollingCountByLeading4Months'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        granularity: 'half_year',
        dateRange: ['2024-01-01', '2025-12-31']
      }],
      dimensions: [],
      filters: [],
      timezone: 'Europe/London'
    },
    [
      {
        orders__created_at_half_year: '2024-01-01T00:00:00.000Z',
        orders__rolling_count_by_leading4_months: '9',
      },
      {
        orders__created_at_half_year: '2024-07-01T00:00:00.000Z',
        orders__rolling_count_by_leading4_months: '8',
      },
      {
        orders__created_at_half_year: '2025-01-01T00:00:00.000Z',
        orders__rolling_count_by_leading4_months: '8',
      },
      {
        orders__created_at_half_year: '2025-07-01T00:00:00.000Z',
        orders__rolling_count_by_leading4_months: '7',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  it('works with half_year custom granularity with dimension with leading rolling window query', async () => dbRunner.runQueryTest(
    {
      measures: ['orders.rollingCountByLeading4Months'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        granularity: 'half_year',
        dateRange: ['2024-01-01', '2025-12-31']
      }],
      dimensions: ['orders.status'],
      filters: [],
      order: [{ id: 'orders.createdAt' }, { id: 'orders.status' }],
      timezone: 'Europe/London'
    },
    [
      {
        orders__created_at_half_year: '2024-01-01T00:00:00.000Z',
        orders__rolling_count_by_leading4_months: '3',
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: '2024-01-01T00:00:00.000Z',
        orders__rolling_count_by_leading4_months: '3',
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: '2024-01-01T00:00:00.000Z',
        orders__rolling_count_by_leading4_months: '3',
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year: '2024-07-01T00:00:00.000Z',
        orders__rolling_count_by_leading4_months: '3',
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: '2024-07-01T00:00:00.000Z',
        orders__rolling_count_by_leading4_months: '3',
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: '2024-07-01T00:00:00.000Z',
        orders__rolling_count_by_leading4_months: '2',
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year: '2025-01-01T00:00:00.000Z',
        orders__rolling_count_by_leading4_months: '3',
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: '2025-01-01T00:00:00.000Z',
        orders__rolling_count_by_leading4_months: '2',
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: '2025-01-01T00:00:00.000Z',
        orders__rolling_count_by_leading4_months: '3',
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year: '2025-07-01T00:00:00.000Z',
        orders__rolling_count_by_leading4_months: '2',
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: '2025-07-01T00:00:00.000Z',
        orders__rolling_count_by_leading4_months: '2',
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: '2025-07-01T00:00:00.000Z',
        orders__rolling_count_by_leading4_months: '3',
        orders__status: 'shipped',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  it('works with half_year_by_1st_april custom granularity w/o dimensions with leading rolling window query', async () => dbRunner.runQueryTest(
    {
      measures: ['orders.rollingCountByLeading4Months'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        granularity: 'half_year_by_1st_april',
        dateRange: ['2024-01-01', '2025-12-31']
      }],
      dimensions: [],
      filters: [],
      timezone: 'Europe/London'
    },
    [
      {
        orders__created_at_half_year_by_1st_april: '2023-10-01T00:00:00.000Z',
        orders__rolling_count_by_leading4_months: '9',
      },
      {
        orders__created_at_half_year_by_1st_april: '2024-04-01T00:00:00.000Z',
        orders__rolling_count_by_leading4_months: '9',
      },
      {
        orders__created_at_half_year_by_1st_april: '2024-10-01T00:00:00.000Z',
        orders__rolling_count_by_leading4_months: '9',
      },
      {
        orders__created_at_half_year_by_1st_april: '2025-04-01T00:00:00.000Z',
        orders__rolling_count_by_leading4_months: '9',
      },
      {
        orders__created_at_half_year_by_1st_april: '2025-10-01T00:00:00.000Z',
        orders__rolling_count_by_leading4_months: '1',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));
});
