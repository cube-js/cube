import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { MSSqlDbRunner } from './MSSqlDbRunner';

describe('Custom Granularities', () => {
  jest.setTimeout(200000);

  const dbRunner = new MSSqlDbRunner();

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
  cubes:
    - name: orders
      sql: >
        SELECT
            num + 1 AS order_id,
            DATEADD(WEEK, num * 2, '2024-01-01T00:00:00.000Z') AS created_at,
            CASE
                WHEN (num + 1) % 3 = 1 THEN 'processing'
                WHEN (num + 1) % 3 = 2 THEN 'completed'
                ELSE 'shipped'
            END AS status
        FROM ##numbers

      dimensions:
        - name: order_id
          sql: order_id
          type: number
          primary_key: true
          public: true

        - name: status
          sql: status
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
            - name: fifteen_days_hours_minutes_seconds
              interval: 15 days 3 hours 25 minutes 40 seconds
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
      timezone: 'UTC'
    },
    [
      {
        orders__count: 13,
        orders__created_at_half_year: new Date('2024-01-01T00:00:00.000Z'),
      },
      {
        orders__count: 14,
        orders__created_at_half_year: new Date('2024-07-01T00:00:00.000Z'),
      },
      {
        orders__count: 13,
        orders__created_at_half_year: new Date('2025-01-01T00:00:00.000Z'),
      },
      {
        orders__count: 13,
        orders__created_at_half_year: new Date('2025-07-01T00:00:00.000Z'),
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
        orders__count: 7,
        orders__created_at_half_year_by_1st_april: new Date('2023-10-01T00:00:00.000Z'),
      },
      {
        orders__count: 13,
        orders__created_at_half_year_by_1st_april: new Date('2024-04-01T00:00:00.000Z'),
      },
      {
        orders__count: 13,
        orders__created_at_half_year_by_1st_april: new Date('2024-10-01T00:00:00.000Z'),
      },
      {
        orders__count: 13,
        orders__created_at_half_year_by_1st_april: new Date('2025-04-01T00:00:00.000Z'),
      },
      {
        orders__count: 7,
        orders__created_at_half_year_by_1st_april: new Date('2025-10-01T00:00:00.000Z'),
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
      timezone: 'UTC'
    },
    [
      {
        orders__count: 4,
        orders__created_at_half_year: new Date('2024-01-01T00:00:00.000Z'),
        orders__status: 'completed',
      },
      {
        orders__count: 5,
        orders__created_at_half_year: new Date('2024-01-01T00:00:00.000Z'),
        orders__status: 'processing',
      },
      {
        orders__count: 4,
        orders__created_at_half_year: new Date('2024-01-01T00:00:00.000Z'),
        orders__status: 'shipped',
      },
      {
        orders__count: 5,
        orders__created_at_half_year: new Date('2024-07-01T00:00:00.000Z'),
        orders__status: 'completed',
      },
      {
        orders__count: 4,
        orders__created_at_half_year: new Date('2024-07-01T00:00:00.000Z'),
        orders__status: 'processing',
      },
      {
        orders__count: 5,
        orders__created_at_half_year: new Date('2024-07-01T00:00:00.000Z'),
        orders__status: 'shipped',
      },
      {
        orders__count: 4,
        orders__created_at_half_year: new Date('2025-01-01T00:00:00.000Z'),
        orders__status: 'completed',
      },
      {
        orders__count: 5,
        orders__created_at_half_year: new Date('2025-01-01T00:00:00.000Z'),
        orders__status: 'processing',
      },
      {
        orders__count: 4,
        orders__created_at_half_year: new Date('2025-01-01T00:00:00.000Z'),
        orders__status: 'shipped',
      },
      {
        orders__count: 5,
        orders__created_at_half_year: new Date('2025-07-01T00:00:00.000Z'),
        orders__status: 'completed',
      },
      {
        orders__count: 4,
        orders__created_at_half_year: new Date('2025-07-01T00:00:00.000Z'),
        orders__status: 'processing',
      },
      {
        orders__count: 4,
        orders__created_at_half_year: new Date('2025-07-01T00:00:00.000Z'),
        orders__status: 'shipped',
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
        orders__count: 2,
        orders__created_at_half_year_by_1st_april: new Date('2023-10-01T00:00:00.000Z'),
        orders__status: 'completed',
      },
      {
        orders__count: 3,
        orders__created_at_half_year_by_1st_april: new Date('2023-10-01T00:00:00.000Z'),
        orders__status: 'processing',
      },
      {
        orders__count: 2,
        orders__created_at_half_year_by_1st_april: new Date('2023-10-01T00:00:00.000Z'),
        orders__status: 'shipped',
      },
      {
        orders__count: 5,
        orders__created_at_half_year_by_1st_april: new Date('2024-04-01T00:00:00.000Z'),
        orders__status: 'completed',
      },
      {
        orders__count: 4,
        orders__created_at_half_year_by_1st_april: new Date('2024-04-01T00:00:00.000Z'),
        orders__status: 'processing',
      },
      {
        orders__count: 4,
        orders__created_at_half_year_by_1st_april: new Date('2024-04-01T00:00:00.000Z'),
        orders__status: 'shipped',
      },
      {
        orders__count: 4,
        orders__created_at_half_year_by_1st_april: new Date('2024-10-01T00:00:00.000Z'),
        orders__status: 'completed',
      },
      {
        orders__count: 4,
        orders__created_at_half_year_by_1st_april: new Date('2024-10-01T00:00:00.000Z'),
        orders__status: 'processing',
      },
      {
        orders__count: 5,
        orders__created_at_half_year_by_1st_april: new Date('2024-10-01T00:00:00.000Z'),
        orders__status: 'shipped',
      },
      {
        orders__count: 4,
        orders__created_at_half_year_by_1st_april: new Date('2025-04-01T00:00:00.000Z'),
        orders__status: 'completed',
      },
      {
        orders__count: 5,
        orders__created_at_half_year_by_1st_april: new Date('2025-04-01T00:00:00.000Z'),
        orders__status: 'processing',
      },
      {
        orders__count: 4,
        orders__created_at_half_year_by_1st_april: new Date('2025-04-01T00:00:00.000Z'),
        orders__status: 'shipped',
      },
      {
        orders__count: 3,
        orders__created_at_half_year_by_1st_april: new Date('2025-10-01T00:00:00.000Z'),
        orders__status: 'completed',
      },
      {
        orders__count: 2,
        orders__created_at_half_year_by_1st_april: new Date('2025-10-01T00:00:00.000Z'),
        orders__status: 'processing',
      },
      {
        orders__count: 2,
        orders__created_at_half_year_by_1st_april: new Date('2025-10-01T00:00:00.000Z'),
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
        orders__created_at_half_year: new Date('2024-01-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 13,
      },
      {
        orders__created_at_half_year: new Date('2024-07-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 27,
      },
      {
        orders__created_at_half_year: new Date('2025-01-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 40,
      },
      {
        orders__created_at_half_year: new Date('2025-07-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 53,
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
        orders__created_at_half_year: new Date('2024-01-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 4,
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: new Date('2024-01-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 5,
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: new Date('2024-01-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 4,
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year: new Date('2024-07-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 9,
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: new Date('2024-07-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 9,
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: new Date('2024-07-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 9,
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year: new Date('2025-01-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 13,
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: new Date('2025-01-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 14,
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: new Date('2025-01-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 13,
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year: new Date('2025-07-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 18,
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: new Date('2025-07-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 18,
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: new Date('2025-07-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 17,
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
        orders__created_at_half_year_by_1st_april: new Date('2023-10-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 2,
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year_by_1st_april: new Date('2023-10-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 3,
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year_by_1st_april: new Date('2023-10-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 2,
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year_by_1st_april: new Date('2024-04-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 7,
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year_by_1st_april: new Date('2024-04-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 7,
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year_by_1st_april: new Date('2024-04-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 6,
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year_by_1st_april: new Date('2024-10-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 11,
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year_by_1st_april: new Date('2024-10-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 11,
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year_by_1st_april: new Date('2024-10-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 11,
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year_by_1st_april: new Date('2025-04-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 15,
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year_by_1st_april: new Date('2025-04-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 16,
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year_by_1st_april: new Date('2025-04-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 15,
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year_by_1st_april: new Date('2025-10-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 20,
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year_by_1st_april: new Date('2025-10-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 20,
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year_by_1st_april: new Date('2025-10-01T00:00:00.000Z'),
        orders__rolling_count_by_unbounded: 19,
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
        orders__created_at_half_year: new Date('2024-01-01T00:00:00.000Z'),
        orders__rolling_count_by_trailing3_months: 6,
      },
      {
        orders__created_at_half_year: new Date('2024-07-01T00:00:00.000Z'),
        orders__rolling_count_by_trailing3_months: 7,
      },
      {
        orders__created_at_half_year: new Date('2025-01-01T00:00:00.000Z'),
        orders__rolling_count_by_trailing3_months: 7,
      },
      {
        orders__created_at_half_year: new Date('2025-07-01T00:00:00.000Z'),
        orders__rolling_count_by_trailing3_months: 7,
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
        orders__created_at_half_year: new Date('2024-01-01T00:00:00.000Z'),
        orders__rolling_count_by_trailing3_months: 2,
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: new Date('2024-01-01T00:00:00.000Z'),
        orders__rolling_count_by_trailing3_months: 2,
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: new Date('2024-01-01T00:00:00.000Z'),
        orders__rolling_count_by_trailing3_months: 2,
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year: new Date('2024-07-01T00:00:00.000Z'),
        orders__rolling_count_by_trailing3_months: 2,
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: new Date('2024-07-01T00:00:00.000Z'),
        orders__rolling_count_by_trailing3_months: 2,
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: new Date('2024-07-01T00:00:00.000Z'),
        orders__rolling_count_by_trailing3_months: 3,
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year: new Date('2025-01-01T00:00:00.000Z'),
        orders__rolling_count_by_trailing3_months: 2,
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: new Date('2025-01-01T00:00:00.000Z'),
        orders__rolling_count_by_trailing3_months: 3,
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: new Date('2025-01-01T00:00:00.000Z'),
        orders__rolling_count_by_trailing3_months: 2,
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year: new Date('2025-07-01T00:00:00.000Z'),
        orders__rolling_count_by_trailing3_months: 3,
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: new Date('2025-07-01T00:00:00.000Z'),
        orders__rolling_count_by_trailing3_months: 2,
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: new Date('2025-07-01T00:00:00.000Z'),
        orders__rolling_count_by_trailing3_months: 2,
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
        orders__created_at_half_year_by_1st_april: new Date('2023-10-01T00:00:00.000Z'),
        orders__rolling_count_by_trailing3_months: 7,
      },
      {
        orders__created_at_half_year_by_1st_april: new Date('2024-04-01T00:00:00.000Z'),
        orders__rolling_count_by_trailing3_months: 7,
      },
      {
        orders__created_at_half_year_by_1st_april: new Date('2024-10-01T00:00:00.000Z'),
        orders__rolling_count_by_trailing3_months: 6,
      },
      {
        orders__created_at_half_year_by_1st_april: new Date('2025-04-01T00:00:00.000Z'),
        orders__rolling_count_by_trailing3_months: 6,
      },
      {
        orders__created_at_half_year_by_1st_april: new Date('2025-10-01T00:00:00.000Z'),
        orders__rolling_count_by_trailing3_months: 6,
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
        orders__created_at_half_year: new Date('2024-01-01T00:00:00.000Z'),
        orders__rolling_count_by_leading4_months: 9,
      },
      {
        orders__created_at_half_year: new Date('2024-07-01T00:00:00.000Z'),
        orders__rolling_count_by_leading4_months: 8,
      },
      {
        orders__created_at_half_year: new Date('2025-01-01T00:00:00.000Z'),
        orders__rolling_count_by_leading4_months: 8,
      },
      {
        orders__created_at_half_year: new Date('2025-07-01T00:00:00.000Z'),
        orders__rolling_count_by_leading4_months: 7,
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
        orders__created_at_half_year: new Date('2024-01-01T00:00:00.000Z'),
        orders__rolling_count_by_leading4_months: 3,
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: new Date('2024-01-01T00:00:00.000Z'),
        orders__rolling_count_by_leading4_months: 3,
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: new Date('2024-01-01T00:00:00.000Z'),
        orders__rolling_count_by_leading4_months: 3,
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year: new Date('2024-07-01T00:00:00.000Z'),
        orders__rolling_count_by_leading4_months: 3,
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: new Date('2024-07-01T00:00:00.000Z'),
        orders__rolling_count_by_leading4_months: 3,
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: new Date('2024-07-01T00:00:00.000Z'),
        orders__rolling_count_by_leading4_months: 2,
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year: new Date('2025-01-01T00:00:00.000Z'),
        orders__rolling_count_by_leading4_months: 3,
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: new Date('2025-01-01T00:00:00.000Z'),
        orders__rolling_count_by_leading4_months: 2,
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: new Date('2025-01-01T00:00:00.000Z'),
        orders__rolling_count_by_leading4_months: 3,
        orders__status: 'shipped',
      },
      {
        orders__created_at_half_year: new Date('2025-07-01T00:00:00.000Z'),
        orders__rolling_count_by_leading4_months: 2,
        orders__status: 'completed',
      },
      {
        orders__created_at_half_year: new Date('2025-07-01T00:00:00.000Z'),
        orders__rolling_count_by_leading4_months: 2,
        orders__status: 'processing',
      },
      {
        orders__created_at_half_year: new Date('2025-07-01T00:00:00.000Z'),
        orders__rolling_count_by_leading4_months: 3,
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
        orders__created_at_half_year_by_1st_april: new Date('2023-10-01T00:00:00.000Z'),
        orders__rolling_count_by_leading4_months: 9,
      },
      {
        orders__created_at_half_year_by_1st_april: new Date('2024-04-01T00:00:00.000Z'),
        orders__rolling_count_by_leading4_months: 9,
      },
      {
        orders__created_at_half_year_by_1st_april: new Date('2024-10-01T00:00:00.000Z'),
        orders__rolling_count_by_leading4_months: 9,
      },
      {
        orders__created_at_half_year_by_1st_april: new Date('2025-04-01T00:00:00.000Z'),
        orders__rolling_count_by_leading4_months: 9,
      },
      {
        orders__created_at_half_year_by_1st_april: new Date('2025-10-01T00:00:00.000Z'),
        orders__rolling_count_by_leading4_months: 1,
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  it('works with fifteen_days_hours_minutes_seconds custom granularity w/o dimensions query', async () => dbRunner.runQueryTest(
    {
      measures: ['orders.count'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        granularity: 'fifteen_days_hours_minutes_seconds',
        dateRange: ['2024-01-01', '2024-02-31']
      }],
      dimensions: [],
      filters: [],
      timezone: 'Europe/London'
    },
    [
      {
        orders__count: 1,
        orders__created_at_fifteen_days_hours_minutes_seconds: new Date('2023-12-17T06:49:20.000Z'),
      },
      {
        orders__count: 1,
        orders__created_at_fifteen_days_hours_minutes_seconds: new Date('2024-01-01T10:15:00.000Z'),
      },
      {
        orders__count: 1,
        orders__created_at_fifteen_days_hours_minutes_seconds: new Date('2024-01-16T13:40:40.000Z'),
      },
      {
        orders__count: 1,
        orders__created_at_fifteen_days_hours_minutes_seconds: new Date('2024-01-31T17:06:20.000Z'),
      },
      {
        orders__count: 1,
        orders__created_at_fifteen_days_hours_minutes_seconds: new Date('2024-02-15T20:32:00.000Z'),
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));
});
