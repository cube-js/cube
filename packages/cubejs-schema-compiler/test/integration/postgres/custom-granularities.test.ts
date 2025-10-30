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
            - name: one_week_by_friday_by_offset
              interval: 1 week
              offset: 4 days
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

  it('works with one_week_by_friday_by_offset custom granularity w/o dimensions query', async () => dbRunner.runQueryTest(
    {
      measures: ['orders.count'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        granularity: 'one_week_by_friday_by_offset',
        dateRange: ['2024-01-01', '2024-03-01']
      }],
      dimensions: [],
      filters: [],
      timezone: 'Europe/London'
    },
    [
      {
        orders__count: '1',
        orders__created_at_one_week_by_friday_by_offset: '2023-12-29T00:00:00.000Z',
      },
      {
        orders__count: '1',
        orders__created_at_one_week_by_friday_by_offset: '2024-01-12T00:00:00.000Z',
      },
      {
        orders__count: '1',
        orders__created_at_one_week_by_friday_by_offset: '2024-01-26T00:00:00.000Z',
      },
      {
        orders__count: '1',
        orders__created_at_one_week_by_friday_by_offset: '2024-02-09T00:00:00.000Z',
      },
      {
        orders__count: '1',
        orders__created_at_one_week_by_friday_by_offset: '2024-02-23T00:00:00.000Z',
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

  // Query-time offset tests
  
  // Test demonstrating day boundaries shifted to 2am-2am
  // With offset "2 hours", midnight and 1am data get grouped into the previous day's bucket
  it('works with query-time offset shifting day boundary (2am-2am)', async () => {
    const { compiler: offsetCompiler, joinGraph: offsetJoinGraph, cubeEvaluator: offsetCubeEvaluator } = prepareYamlCompiler(`
      cubes:
        - name: events
          sql: >
            SELECT *
            FROM (VALUES
              (1, '2024-01-01 00:00:00'::timestamp),
              (2, '2024-01-01 01:00:00'::timestamp),
              (3, '2024-01-01 01:30:00'::timestamp),
              (4, '2024-01-02 00:00:00'::timestamp),
              (5, '2024-01-02 01:00:00'::timestamp),
              (6, '2024-01-02 01:30:00'::timestamp)
            ) AS t(event_id, event_time)

          dimensions:
            - name: event_id
              sql: event_id
              type: number
              primary_key: true

            - name: eventTime
              sql: event_time
              type: time

          measures:
            - name: count
              type: count
    `);
    
    await dbRunner.runQueryTest(
      {
        measures: ['events.count'],
        timeDimensions: [{
          dimension: 'events.eventTime',
          granularity: 'day',
          offset: '2 hours', // Days run 2am-2am instead of midnight-midnight
          dateRange: ['2024-01-01', '2024-01-03']
        }],
        dimensions: [],
        timezone: 'UTC',
        order: [['events.eventTime', 'asc']],
      },
      [
        {
          // Dec 31 at 2am = bucket for data from Dec 31 2am to Jan 1 2am
          // Contains: Jan 1 00:00, Jan 1 01:00, Jan 1 01:30 (3 records before Jan 1 2am)
          events__event_time_day: '2023-12-31T02:00:00.000Z',
          events__count: '3',
        },
        {
          // Jan 1 at 2am = bucket for data from Jan 1 2am to Jan 2 2am
          // Contains: Jan 2 00:00, Jan 2 01:00, Jan 2 01:30 (3 records before Jan 2 2am)
          events__event_time_day: '2024-01-01T02:00:00.000Z',
          events__count: '3',
        },
      ],
      { joinGraph: offsetJoinGraph, cubeEvaluator: offsetCubeEvaluator, compiler: offsetCompiler }
    );
  });

  // Test demonstrating week boundaries shifted to Wednesday-Wednesday
  // Default week is Monday-based (ISO), offset "2 days" shifts to Wednesday
  it('works with query-time offset shifting week to start on Wednesday', async () => {
    const { compiler: weekCompiler, joinGraph: weekJoinGraph, cubeEvaluator: weekCubeEvaluator } = prepareYamlCompiler(`
      cubes:
        - name: activities
          sql: >
            SELECT *
            FROM (VALUES
              (1, '2024-01-01 10:00:00'::timestamp),
              (2, '2024-01-02 10:00:00'::timestamp),
              (3, '2024-01-03 10:00:00'::timestamp),
              (4, '2024-01-04 10:00:00'::timestamp),
              (5, '2024-01-08 10:00:00'::timestamp),
              (6, '2024-01-10 10:00:00'::timestamp)
            ) AS t(activity_id, activity_time)

          dimensions:
            - name: activity_id
              sql: activity_id
              type: number
              primary_key: true

            - name: activityTime
              sql: activity_time
              type: time

          measures:
            - name: count
              type: count
    `);
    
    await dbRunner.runQueryTest(
      {
        measures: ['activities.count'],
        timeDimensions: [{
          dimension: 'activities.activityTime',
          granularity: 'week',
          offset: '2 days', // Shift from Monday start to Wednesday start
          dateRange: ['2024-01-01', '2024-01-15']
        }],
        dimensions: [],
        timezone: 'UTC',
        order: [['activities.activityTime', 'asc']],
      },
      [
        {
          // Week of Jan 3 (Wednesday) = Wed Jan 3 to Tue Jan 9
          // Contains: Jan 3 (Wed), Jan 4 (Thu), Jan 8 (Mon)
          activities__activity_time_week: '2024-01-03T00:00:00.000Z',
          activities__count: '3',
        },
        {
          // Week of Jan 10 (Wednesday) = Wed Jan 10 to Tue Jan 16
          // Contains: Jan 10 (Wed)
          activities__activity_time_week: '2024-01-10T00:00:00.000Z',
          activities__count: '1',
        },
        {
          // Week of Dec 27 (Wednesday) = Wed Dec 27 to Tue Jan 2
          // Contains: Jan 1 (Mon), Jan 2 (Tue) - both before the Wednesday cutoff
          activities__activity_time_week: '2023-12-27T00:00:00.000Z',
          activities__count: '2',
        },
      ],
      { joinGraph: weekJoinGraph, cubeEvaluator: weekCubeEvaluator, compiler: weekCompiler }
    );
  });

  it('works with query-time offset on predefined hour granularity', async () => dbRunner.runQueryTest(
    {
      measures: ['orders.count'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        granularity: 'hour',
        offset: '15 minutes',
        dateRange: ['2024-01-01T00:00:00.000', '2024-01-01T02:00:00.000']
      }],
      dimensions: [],
      timezone: 'UTC',
      order: [['orders.createdAt', 'asc']],
    },
    [
      {
        orders__created_at_hour: '2023-12-31T23:15:00.000Z',
        orders__count: '1',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  it('rejects query-time offset with custom granularity', async () => {
    await expect(
      dbRunner.runQueryTest(
        {
          measures: ['orders.count'],
          timeDimensions: [{
            dimension: 'orders.createdAt',
            granularity: 'one_week_by_friday_by_offset', // Custom granularity
            offset: '2 days', // Should be rejected
            dateRange: ['2024-01-01', '2024-03-31']
          }],
          dimensions: [],
          timezone: 'UTC',
          order: [['orders.createdAt', 'asc']],
        },
        [],
        { joinGraph, cubeEvaluator, compiler }
      )
    ).rejects.toThrow('Query-time offset parameter cannot be used with custom granularity');
  });

  it('works with negative query-time offset', async () => dbRunner.runQueryTest(
    {
      measures: ['orders.count'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        granularity: 'day',
        offset: '-6 hours',
        dateRange: ['2024-01-01', '2024-01-31']
      }],
      dimensions: [],
      timezone: 'UTC',
      order: [['orders.createdAt', 'asc']],
    },
    [
      {
        orders__created_at_day: '2023-12-31T18:00:00.000Z',
        orders__count: '1',
      },
      {
        orders__created_at_day: '2024-01-14T18:00:00.000Z',
        orders__count: '1',
      },
      {
        orders__created_at_day: '2024-01-28T18:00:00.000Z',
        orders__count: '1',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  it('works with query-time offset with minute precision', async () => dbRunner.runQueryTest(
    {
      measures: ['orders.count'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        granularity: 'hour',
        offset: '-45 minutes',
        dateRange: ['2024-01-01T00:00:00.000', '2024-01-01T02:00:00.000']
      }],
      dimensions: [],
      timezone: 'UTC',
      order: [['orders.createdAt', 'asc']],
    },
    [
      {
        orders__created_at_hour: '2023-12-31T23:15:00.000Z',
        orders__count: '1',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));
});
