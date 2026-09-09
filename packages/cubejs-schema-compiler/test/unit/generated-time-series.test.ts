/* eslint-disable no-restricted-syntax */
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { SnowflakeQuery } from '../../src/adapter/SnowflakeQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

/**
 * A rolling window asked for at some granularity but with no date range has to
 * derive the bounds of its time series in SQL, from the data itself. The planner
 * can only do that where the dialect defines `generated_time_series_select`;
 * without it there is nowhere to take the bounds from and the query is rejected
 * with "Date range is required for time series".
 */
describe('generated time series', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: events
    sql: "SELECT 1 AS user_id, '2024-01-01' AS invited_at"
    dimensions:
      - name: invited_at
        sql: invited_at
        type: time
        granularities:
          - name: two_weeks
            interval: 2 weeks
            origin: "2024-01-01"
    measures:
      - name: cumulative_users
        sql: user_id
        type: count_distinct
        rolling_window:
          trailing: unbounded
      - name: rolling_30d_users
        sql: user_id
        type: count_distinct
        rolling_window:
          trailing: "30 day"
`);

  const buildSql = async (
    QueryClass: any,
    { granularity = 'month', dateRange, measure = 'events.cumulative_users' }: {
      granularity?: string, dateRange?: [string, string], measure?: string
    } = {}
  ) => {
    await compiler.compile();

    const query = new QueryClass({ joinGraph, cubeEvaluator, compiler }, {
      measures: [measure],
      timeDimensions: [{
        dimension: 'events.invited_at',
        granularity,
        ...(dateRange ? { dateRange } : {}),
      }],
      timezone: 'UTC',
      useNativeSqlPlanner: true,
    });

    return query.buildSqlAndParams()[0];
  };

  // Every dialect that generates the series in SQL has to accept the same query,
  // so the guard is stated once over all of them rather than per dialect. Each
  // is paired with the row generator its template has to reach for, since the
  // `time_series` CTE is named the same on the path that does not generate.
  const GENERATING_DIALECTS: [string, any, string][] = [
    ['Postgres', PostgresQuery, 'generate_series'],
    ['Snowflake', SnowflakeQuery, 'ARRAY_GENERATE_RANGE'],
  ];

  const PREDEFINED_GRANULARITIES = ['second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year'];

  describe.each(GENERATING_DIALECTS)('%s', (_name, QueryClass, generator) => {
    it.each(PREDEFINED_GRANULARITIES)('plans a rolling window at %s granularity with no date range', async (granularity) => {
      const sql = await buildSql(QueryClass, { granularity });

      expect(sql).toContain(generator);
    });

    it('keeps planning a rolling window with an explicit date range', async () => {
      const sql = await buildSql(QueryClass, { dateRange: ['2024-01-01', '2024-12-31'] });

      expect(sql).toContain(generator);
    });

    it('plans a bounded rolling window with no date range', async () => {
      const sql = await buildSql(QueryClass, { measure: 'events.rolling_30d_users' });

      expect(sql).toContain(generator);
    });
  });

  describe('Snowflake', () => {
    it('steps the series by the granularity itself, not by its smallest time unit', async () => {
      const weekly = await buildSql(SnowflakeQuery, { granularity: 'week' });
      const quarterly = await buildSql(SnowflakeQuery, { granularity: 'quarter' });

      expect(weekly).toContain('DATEDIFF(week');
      expect(quarterly).toContain('DATEDIFF(quarter');
    });

    it('names the series columns so that they survive identifier folding', async () => {
      const sql = await buildSql(SnowflakeQuery);

      expect(sql).toContain('"date_from"');
      expect(sql).toContain('"date_to"');
    });

    // A predefined granularity takes the requested range verbatim, so the series
    // can start off the granularity boundary. DATEDIFF counts boundaries crossed
    // rather than whole periods and then hands back one row too many, whose
    // period starts past the end of the range.
    it('ends the series at the end of the range even when the range starts mid-period', async () => {
      const ranged = await buildSql(SnowflakeQuery, { dateRange: ['2024-01-15', '2024-02-05'] });

      expect(ranged).toContain('WHERE series_date <= \'2024-02-05\'::timestamp_ntz');

      const derived = await buildSql(SnowflakeQuery);

      expect(derived).toContain('WHERE series_date <= series_end');
    });

    // Snowflake cannot multiply an arbitrary interval by a row number, so a
    // granularity that is not one whole time unit still needs the range spelled
    // out and the series built outside the database.
    it('still requires a date range for a custom granularity', async () => {
      await expect(buildSql(SnowflakeQuery, { granularity: 'two_weeks' }))
        .rejects.toThrow('Date range is required for time series');

      const sql = await buildSql(SnowflakeQuery, {
        granularity: 'two_weeks',
        dateRange: ['2024-01-01', '2024-12-31'],
      });

      expect(sql).toContain('time_series');
    });
  });
});
