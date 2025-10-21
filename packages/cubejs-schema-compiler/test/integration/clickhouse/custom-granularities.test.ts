import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { ClickHouseDbRunner } from './ClickHouseDbRunner';

describe('Custom Granularities', () => {
  jest.setTimeout(200000);

  const dbRunner = new ClickHouseDbRunner();

  afterAll(async () => {
    await dbRunner.tearDown();
  });

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
  cubes:
    - name: orders
      sql: >
        SELECT
            num + 1 AS order_id,
            DATE_ADD(toDateTime('2024-01-01'), INTERVAL (num * 2) WEEK) AS created_at,
            CASE
                WHEN (num + 1) % 3 = 1 THEN 'processing'
                WHEN (num + 1) % 3 = 2 THEN 'completed'
                ELSE 'shipped'
            END AS status
        FROM numbers

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
            - name: one_week_by_friday_by_offset
              interval: 1 week
              offset: 4 days
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
      timezone: 'Europe/London'
    },
    [
      {
        orders__count: '13',
        orders__created_at_half_year: '2024-01-01T00:00:00.000',
      },
      {
        orders__count: '14',
        orders__created_at_half_year: '2024-07-01T00:00:00.000',
      },
      {
        orders__count: '13',
        orders__created_at_half_year: '2025-01-01T00:00:00.000',
      },
      {
        orders__count: '13',
        orders__created_at_half_year: '2025-07-01T00:00:00.000',
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
        orders__created_at_half_year_by_1st_april: '2023-10-01T00:00:00.000',
      },
      {
        orders__count: '13',
        orders__created_at_half_year_by_1st_april: '2024-04-01T00:00:00.000',
      },
      {
        orders__count: '13',
        orders__created_at_half_year_by_1st_april: '2024-10-01T00:00:00.000',
      },
      {
        orders__count: '13',
        orders__created_at_half_year_by_1st_april: '2025-04-01T00:00:00.000',
      },
      {
        orders__count: '7',
        orders__created_at_half_year_by_1st_april: '2025-10-01T00:00:00.000',
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
        orders__created_at_half_year: '2024-01-01T00:00:00.000',
        orders__status: 'completed',
      },
      {
        orders__count: '5',
        orders__created_at_half_year: '2024-01-01T00:00:00.000',
        orders__status: 'processing',
      },
      {
        orders__count: '4',
        orders__created_at_half_year: '2024-01-01T00:00:00.000',
        orders__status: 'shipped',
      },
      {
        orders__count: '5',
        orders__created_at_half_year: '2024-07-01T00:00:00.000',
        orders__status: 'completed',
      },
      {
        orders__count: '4',
        orders__created_at_half_year: '2024-07-01T00:00:00.000',
        orders__status: 'processing',
      },
      {
        orders__count: '5',
        orders__created_at_half_year: '2024-07-01T00:00:00.000',
        orders__status: 'shipped',
      },
      {
        orders__count: '4',
        orders__created_at_half_year: '2025-01-01T00:00:00.000',
        orders__status: 'completed',
      },
      {
        orders__count: '5',
        orders__created_at_half_year: '2025-01-01T00:00:00.000',
        orders__status: 'processing',
      },
      {
        orders__count: '4',
        orders__created_at_half_year: '2025-01-01T00:00:00.000',
        orders__status: 'shipped',
      },
      {
        orders__count: '5',
        orders__created_at_half_year: '2025-07-01T00:00:00.000',
        orders__status: 'completed',
      },
      {
        orders__count: '4',
        orders__created_at_half_year: '2025-07-01T00:00:00.000',
        orders__status: 'processing',
      },
      {
        orders__count: '4',
        orders__created_at_half_year: '2025-07-01T00:00:00.000',
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
        orders__count: '2',
        orders__created_at_half_year_by_1st_april: '2023-10-01T00:00:00.000',
        orders__status: 'completed',
      },
      {
        orders__count: '3',
        orders__created_at_half_year_by_1st_april: '2023-10-01T00:00:00.000',
        orders__status: 'processing',
      },
      {
        orders__count: '2',
        orders__created_at_half_year_by_1st_april: '2023-10-01T00:00:00.000',
        orders__status: 'shipped',
      },
      {
        orders__count: '5',
        orders__created_at_half_year_by_1st_april: '2024-04-01T00:00:00.000',
        orders__status: 'completed',
      },
      {
        orders__count: '4',
        orders__created_at_half_year_by_1st_april: '2024-04-01T00:00:00.000',
        orders__status: 'processing',
      },
      {
        orders__count: '4',
        orders__created_at_half_year_by_1st_april: '2024-04-01T00:00:00.000',
        orders__status: 'shipped',
      },
      {
        orders__count: '4',
        orders__created_at_half_year_by_1st_april: '2024-10-01T00:00:00.000',
        orders__status: 'completed',
      },
      {
        orders__count: '4',
        orders__created_at_half_year_by_1st_april: '2024-10-01T00:00:00.000',
        orders__status: 'processing',
      },
      {
        orders__count: '5',
        orders__created_at_half_year_by_1st_april: '2024-10-01T00:00:00.000',
        orders__status: 'shipped',
      },
      {
        orders__count: '4',
        orders__created_at_half_year_by_1st_april: '2025-04-01T00:00:00.000',
        orders__status: 'completed',
      },
      {
        orders__count: '5',
        orders__created_at_half_year_by_1st_april: '2025-04-01T00:00:00.000',
        orders__status: 'processing',
      },
      {
        orders__count: '4',
        orders__created_at_half_year_by_1st_april: '2025-04-01T00:00:00.000',
        orders__status: 'shipped',
      },
      {
        orders__count: '3',
        orders__created_at_half_year_by_1st_april: '2025-10-01T00:00:00.000',
        orders__status: 'completed',
      },
      {
        orders__count: '2',
        orders__created_at_half_year_by_1st_april: '2025-10-01T00:00:00.000',
        orders__status: 'processing',
      },
      {
        orders__count: '2',
        orders__created_at_half_year_by_1st_april: '2025-10-01T00:00:00.000',
        orders__status: 'shipped',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  // This requires inequal join condition support, which is available in
  // ClickHouse >= 24.5 (as experimental feature).
  // But it also conflicts with "join_use_nulls: '1'" which is turned on in our runner.
  // @see https://clickhouse.com/docs/en/sql-reference/statements/select/join#experimental-join-with-inequality-conditions-for-columns-from-different-tables
  // it('works with half_year custom granularity w/o dimensions with unbounded rolling window query', async () => dbRunner.runQueryTest(
  //   {
  //     measures: ['orders.rollingCountByUnbounded'],
  //     timeDimensions: [{
  //       dimension: 'orders.createdAt',
  //       granularity: 'half_year',
  //       dateRange: ['2024-01-01', '2025-12-31']
  //     }],
  //     dimensions: [],
  //     filters: [],
  //     timezone: 'Europe/London'
  //   },
  //   [
  //     // TODO Fill
  //   ],
  //   { joinGraph, cubeEvaluator, compiler }
  // ));

  // This requires inequal join condition support, which is available in
  // ClickHouse >= 24.5 (as experimental feature).
  // But it also conflicts with "join_use_nulls: '1'" which is turned on in our runner.
  // @see https://clickhouse.com/docs/en/sql-reference/statements/select/join#experimental-join-with-inequality-conditions-for-columns-from-different-tables
  // it('works with half_year custom granularity with dimension with unbounded rolling window query', async () => dbRunner.runQueryTest(
  //   {
  //     measures: ['orders.rollingCountByUnbounded'],
  //     timeDimensions: [{
  //       dimension: 'orders.createdAt',
  //       granularity: 'half_year',
  //       dateRange: ['2024-01-01', '2025-12-31']
  //     }],
  //     dimensions: ['orders.status'],
  //     filters: [],
  //     order: [{ id: 'orders.createdAt' }, { id: 'orders.status' }],
  //     timezone: 'Europe/London'
  //   },
  //   [
  //     // TODO Fill
  //   ],
  //   { joinGraph, cubeEvaluator, compiler }
  // ));

  // This requires inequal join condition support, which is available in
  // ClickHouse >= 24.5 (as experimental feature).
  // But it also conflicts with "join_use_nulls: '1'" which is turned on in our runner.
  // @see https://clickhouse.com/docs/en/sql-reference/statements/select/join#experimental-join-with-inequality-conditions-for-columns-from-different-tables
  // it('works with half_year_by_1st_april custom granularity with dimension with unbounded rolling window query', async () => dbRunner.runQueryTest(
  //   {
  //     measures: ['orders.rollingCountByUnbounded'],
  //     timeDimensions: [{
  //       dimension: 'orders.createdAt',
  //       granularity: 'half_year_by_1st_april',
  //       dateRange: ['2024-01-01', '2025-12-31']
  //     }],
  //     dimensions: ['orders.status'],
  //     filters: [],
  //     order: [{ id: 'orders.createdAt' }, { id: 'orders.status' }],
  //     timezone: 'Europe/London'
  //   },
  //   [
  //     // TODO Fill
  //   ],
  //   { joinGraph, cubeEvaluator, compiler }
  // ));

  // This requires inequal join condition support, which is available in
  // ClickHouse >= 24.5 (as experimental feature).
  // But it also conflicts with "join_use_nulls: '1'" which is turned on in our runner.
  // @see https://clickhouse.com/docs/en/sql-reference/statements/select/join#experimental-join-with-inequality-conditions-for-columns-from-different-tables
  // it('works with half_year custom granularity w/o dimensions with trailing rolling window query', async () => dbRunner.runQueryTest(
  //   {
  //     measures: ['orders.rollingCountByTrailing3Months'],
  //     timeDimensions: [{
  //       dimension: 'orders.createdAt',
  //       granularity: 'half_year',
  //       dateRange: ['2024-01-01', '2025-12-31']
  //     }],
  //     dimensions: [],
  //     filters: [],
  //     timezone: 'Europe/London'
  //   },
  //   [
  //     // TODO Fill
  //   ],
  //   { joinGraph, cubeEvaluator, compiler }
  // ));

  // This requires inequal join condition support, which is available in
  // ClickHouse >= 24.5 (as experimental feature).
  // But it also conflicts with "join_use_nulls: '1'" which is turned on in our runner.
  // @see https://clickhouse.com/docs/en/sql-reference/statements/select/join#experimental-join-with-inequality-conditions-for-columns-from-different-tables
  // it('works with half_year custom granularity with dimension with trailing rolling window query', async () => dbRunner.runQueryTest(
  //   {
  //     measures: ['orders.rollingCountByTrailing3Months'],
  //     timeDimensions: [{
  //       dimension: 'orders.createdAt',
  //       granularity: 'half_year',
  //       dateRange: ['2024-01-01', '2025-12-31']
  //     }],
  //     dimensions: ['orders.status'],
  //     filters: [],
  //     order: [{ id: 'orders.createdAt' }, { id: 'orders.status' }],
  //     timezone: 'Europe/London'
  //   },
  //   [
  //     // TODO Fill
  //   ],
  //   { joinGraph, cubeEvaluator, compiler }
  // ));

  // This requires inequal join condition support, which is available in
  // ClickHouse >= 24.5 (as experimental feature).
  // But it also conflicts with "join_use_nulls: '1'" which is turned on in our runner.
  // @see https://clickhouse.com/docs/en/sql-reference/statements/select/join#experimental-join-with-inequality-conditions-for-columns-from-different-tables
  // it('works with half_year_by_1st_april custom granularity w/o dimensions with trailing rolling window query', async () => dbRunner.runQueryTest(
  //   {
  //     measures: ['orders.rollingCountByTrailing3Months'],
  //     timeDimensions: [{
  //       dimension: 'orders.createdAt',
  //       granularity: 'half_year_by_1st_april',
  //       dateRange: ['2024-01-01', '2025-12-31']
  //     }],
  //     dimensions: [],
  //     filters: [],
  //     timezone: 'Europe/London'
  //   },
  //   [
  //      // TODO Fill
  //   ],
  //   { joinGraph, cubeEvaluator, compiler }
  // ));

  // This requires inequal join condition support, which is available in
  // ClickHouse >= 24.5 (as experimental feature).
  // But it also conflicts with "join_use_nulls: '1'" which is turned on in our runner.
  // @see https://clickhouse.com/docs/en/sql-reference/statements/select/join#experimental-join-with-inequality-conditions-for-columns-from-different-tables
  // it('works with half_year custom granularity w/o dimensions with leading rolling window query', async () => dbRunner.runQueryTest(
  //   {
  //     measures: ['orders.rollingCountByLeading4Months'],
  //     timeDimensions: [{
  //       dimension: 'orders.createdAt',
  //       granularity: 'half_year',
  //       dateRange: ['2024-01-01', '2025-12-31']
  //     }],
  //     dimensions: [],
  //     filters: [],
  //     timezone: 'Europe/London'
  //   },
  //   [
  //     // TODO Fill
  //   ],
  //   { joinGraph, cubeEvaluator, compiler }
  // ));

  // This requires inequal join condition support, which is available in
  // ClickHouse >= 24.5 (as experimental feature).
  // But it also conflicts with "join_use_nulls: '1'" which is turned on in our runner.
  // @see https://clickhouse.com/docs/en/sql-reference/statements/select/join#experimental-join-with-inequality-conditions-for-columns-from-different-tables
  // it('works with half_year custom granularity with dimension with leading rolling window query', async () => dbRunner.runQueryTest(
  //   {
  //     measures: ['orders.rollingCountByLeading4Months'],
  //     timeDimensions: [{
  //       dimension: 'orders.createdAt',
  //       granularity: 'half_year',
  //       dateRange: ['2024-01-01', '2025-12-31']
  //     }],
  //     dimensions: ['orders.status'],
  //     filters: [],
  //     order: [{ id: 'orders.createdAt' }, { id: 'orders.status' }],
  //     timezone: 'Europe/London'
  //   },
  //   [
  //     // TODO fill
  //   ],
  //   { joinGraph, cubeEvaluator, compiler }
  // ));

  // This requires inequal join condition support, which is available in
  // ClickHouse >= 24.5 (as experimental feature).
  // But it also conflicts with "join_use_nulls: '1'" which is turned on in our runner.
  // @see https://clickhouse.com/docs/en/sql-reference/statements/select/join#experimental-join-with-inequality-conditions-for-columns-from-different-tables
  // it('works with half_year_by_1st_april custom granularity w/o dimensions with leading rolling window query', async () => dbRunner.runQueryTest(
  //   {
  //     measures: ['orders.rollingCountByLeading4Months'],
  //     timeDimensions: [{
  //       dimension: 'orders.createdAt',
  //       granularity: 'half_year_by_1st_april',
  //       dateRange: ['2024-01-01', '2025-12-31']
  //     }],
  //     dimensions: [],
  //     filters: [],
  //     timezone: 'Europe/London'
  //   },
  //   [
  //     // TODO: fill
  //   ],
  //   { joinGraph, cubeEvaluator, compiler }
  // ));

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
        orders__count: '1',
        orders__created_at_fifteen_days_hours_minutes_seconds: '2023-12-17T06:49:20.000',
      },
      {
        orders__count: '1',
        orders__created_at_fifteen_days_hours_minutes_seconds: '2024-01-01T10:15:00.000',
      },
      {
        orders__count: '1',
        orders__created_at_fifteen_days_hours_minutes_seconds: '2024-01-16T13:40:40.000',
      },
      {
        orders__count: '1',
        orders__created_at_fifteen_days_hours_minutes_seconds: '2024-01-31T17:06:20.000',
      },
      {
        orders__count: '1',
        orders__created_at_fifteen_days_hours_minutes_seconds: '2024-02-15T20:32:00.000',
      },
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  // Query-time offset tests
  
  // Test demonstrating day boundaries shifted to 2am-2am
  it('works with query-time offset shifting day boundary (2am-2am)', async () => {
    const { compiler: offsetCompiler, joinGraph: offsetJoinGraph, cubeEvaluator: offsetCubeEvaluator } = prepareYamlCompiler(`
      cubes:
        - name: events
          sql: >
            SELECT *
            FROM (
              SELECT 1 as event_id, toDateTime('2024-01-01 00:00:00') as event_time UNION ALL
              SELECT 2, toDateTime('2024-01-01 01:00:00') UNION ALL
              SELECT 3, toDateTime('2024-01-01 01:30:00') UNION ALL
              SELECT 4, toDateTime('2024-01-02 00:00:00') UNION ALL
              SELECT 5, toDateTime('2024-01-02 01:00:00') UNION ALL
              SELECT 6, toDateTime('2024-01-02 01:30:00')
            )

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
          offset: '2 hours',
          dateRange: ['2024-01-01', '2024-01-03']
        }],
        dimensions: [],
        timezone: 'UTC',
        order: [['events.eventTime', 'asc']],
      },
      [
        {
          events__event_time_day: '2023-12-31T02:00:00.000',
          events__count: '3',
        },
        {
          events__event_time_day: '2024-01-01T02:00:00.000',
          events__count: '3',
        },
      ],
      { joinGraph: offsetJoinGraph, cubeEvaluator: offsetCubeEvaluator, compiler: offsetCompiler }
    );
  });

  // Test demonstrating week boundaries shifted to Wednesday-Wednesday
  it('works with query-time offset shifting week to start on Wednesday', async () => {
    const { compiler: weekCompiler, joinGraph: weekJoinGraph, cubeEvaluator: weekCubeEvaluator } = prepareYamlCompiler(`
      cubes:
        - name: activities
          sql: >
            SELECT *
            FROM (
              SELECT 1 as activity_id, toDateTime('2024-01-01 10:00:00') as activity_time UNION ALL
              SELECT 2, toDateTime('2024-01-02 10:00:00') UNION ALL
              SELECT 3, toDateTime('2024-01-03 10:00:00') UNION ALL
              SELECT 4, toDateTime('2024-01-04 10:00:00') UNION ALL
              SELECT 5, toDateTime('2024-01-08 10:00:00') UNION ALL
              SELECT 6, toDateTime('2024-01-10 10:00:00')
            )

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
          offset: '2 days',
          dateRange: ['2024-01-01', '2024-01-15']
        }],
        dimensions: [],
        timezone: 'UTC',
        order: [['activities.activityTime', 'asc']],
      },
      [
        {
          activities__activity_time_week: '2024-01-03T00:00:00.000',
          activities__count: '3',
        },
        {
          activities__activity_time_week: '2024-01-10T00:00:00.000',
          activities__count: '1',
        },
        {
          activities__activity_time_week: '2023-12-27T00:00:00.000',
          activities__count: '2',
        },
      ],
      { joinGraph: weekJoinGraph, cubeEvaluator: weekCubeEvaluator, compiler: weekCompiler }
    );
  });

  it('rejects query-time offset with custom granularity', async () => {
    await expect(
      dbRunner.runQueryTest(
        {
          measures: ['orders.count'],
          timeDimensions: [{
            dimension: 'orders.createdAt',
            granularity: 'one_week_by_friday_by_offset',
            offset: '2 days',
            dateRange: ['2024-01-01', '2024-02-28']
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
});
