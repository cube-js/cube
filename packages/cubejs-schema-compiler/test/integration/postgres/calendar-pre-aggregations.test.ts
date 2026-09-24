import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

/**
 * A calendar `time_shift` is the join condition, not arithmetic on the time
 * column, so a rollup storing rows joined on the unshifted key cannot apply it.
 * Either everything the shift reads is materialized and the query is answered
 * from rollups, or the whole query goes to the source database.
 */
describe('Calendar cubes and pre-aggregations', () => {
  jest.setTimeout(200000);

  // A 4-5-4 retail calendar: the first year is 53 weeks, the next two 52, so
  // "one year back" is 371 days for some rows and 364 for others. No interval
  // reproduces that — the mapping only exists as a column of the calendar.
  // language=YAML
  const model = (rollups: string) => `
cubes:
  - name: retail_calendar
    calendar: true
    sql: >
      SELECT d.date_val,
             CASE
               WHEN d.off BETWEEN 371 AND 734 THEN d.date_val - INTERVAL '371 day'
               WHEN d.off >= 735 THEN d.date_val - INTERVAL '364 day'
             END AS prev_year_date
      FROM (SELECT (DATE '2024-02-04' + (gs.n - 1))::timestamp AS date_val,
                   gs.n - 1 AS off
            FROM generate_series(1, 1099) AS gs(n)) d

    dimensions:
      - name: date_key
        sql: "{CUBE}.date_val"
        type: time
        primary_key: true
        time_shift:
          - interval: 1 year
            type: prior
            sql: "{CUBE.prev_year_date}"

      - name: prev_year_date
        sql: "{CUBE}.prev_year_date"
        type: time

      - name: retail_date
        sql: "{CUBE}.date_val"
        type: time
        time_shift:
          - interval: 1 year
            type: prior
            sql: "{CUBE.prev_year_date}"

    measures:
      - name: count
        type: count

    pre_aggregations:
      - name: calendar_rollup
        measures:
          - count
        dimensions:
          - prev_year_date
        time_dimensions:
          - dimension: date_key
            granularity: day
          - dimension: retail_date
            granularity: day

  - name: demand
    sql: >
      SELECT gs.n AS id,
             (DATE '2024-02-04' + (gs.n - 1))::timestamp AS demand_date,
             gs.n AS amount
      FROM generate_series(1, 1099) AS gs(n)

    joins:
      - name: retail_calendar
        sql: "{CUBE.demand_date} = {retail_calendar.date_key}"
        relationship: many_to_one

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: demand_date
        sql: demand_date
        type: time

    measures:
      - name: revenue
        sql: amount
        type: sum

      - name: revenue_ly
        type: number
        multi_stage: true
        sql: "{revenue}"
        time_shift:
          - interval: 1 year
            type: prior

    pre_aggregations:
${rollups}
`;

  // Stores the calendar dimension the query groups by, but nothing the shift
  // reads — so it can answer an unshifted query and nothing else.
  const plainRollup = `
      - name: demand_plain
        measures:
          - revenue
        time_dimensions:
          - dimension: retail_calendar.retail_date
            granularity: day
`;

  const rollupJoin = `
      - name: demand_rollup
        measures:
          - revenue
        time_dimension: demand_date
        granularity: day

      - name: demand_with_calendar
        type: rollup_join
        measures:
          - revenue
        dimensions:
          - retail_calendar.prev_year_date
        time_dimension: retail_calendar.retail_date
        granularity: day
        rollups:
          - demand.demand_rollup
          - retail_calendar.calendar_rollup
`;

  const compile = async (rollups: string) => {
    const compilers = prepareYamlCompiler(model(rollups));
    await compilers.compiler.compile();
    return compilers;
  };

  // Both retail years at once: the first maps back 371 days, the second 364,
  // so a single interval cannot produce this answer however it is chosen.
  const shiftedQuery = {
    measures: ['demand.revenue', 'demand.revenue_ly'],
    timeDimensions: [{
      dimension: 'retail_calendar.retail_date',
      granularity: 'day',
      dateRange: ['2025-02-09', '2025-02-10'],
    }],
    order: [{ id: 'retail_calendar.retail_date' }],
    timezone: 'UTC',
    preAggregationsSchema: '',
  };

  const laterYearQuery = {
    ...shiftedQuery,
    timeDimensions: [{
      dimension: 'retail_calendar.retail_date',
      granularity: 'day',
      dateRange: ['2026-02-08', '2026-02-09'],
    }],
  };

  // Revenue of day n is n. 2025-02-09 is day 372 and maps back 371 days to day
  // 1; 2026-02-08 is day 736 and maps back 364 days to day 372.
  const expected = [
    { retail_calendar__retail_date_day: '2025-02-09T00:00:00.000Z', demand__revenue: '372', demand__revenue_ly: '1' },
    { retail_calendar__retail_date_day: '2025-02-10T00:00:00.000Z', demand__revenue: '373', demand__revenue_ly: '2' },
  ];

  const expectedLaterYear = [
    { retail_calendar__retail_date_day: '2026-02-08T00:00:00.000Z', demand__revenue: '736', demand__revenue_ly: '372' },
    { retail_calendar__retail_date_day: '2026-02-09T00:00:00.000Z', demand__revenue: '737', demand__revenue_ly: '373' },
  ];

  it('falls back to the source when the rollup cannot apply the shift', async () => {
    const compilers = await compile(plainRollup);
    const query = new PostgresQuery(compilers, { ...shiftedQuery, useNativeSqlPlanner: true });

    query.buildSqlAndParams();
    expect((<any>query).preAggregations.preAggregationForQuery).toBeUndefined();

    const res = await dbRunner.evaluateQueryWithPreAggregations(query);
    expect(res).toEqual(expected);
  });

  it('serves the shift from rollups when the calendar is materialized too', async () => {
    const compilers = await compile(rollupJoin);

    // The same rollup answers both retail years, whose shifts are 371 and 364
    // days — reading the mapping, not applying an interval.
    for (const [options, rows] of [[shiftedQuery, expected], [laterYearQuery, expectedLaterYear]] as const) {
      const query = new PostgresQuery(compilers, { ...options, useNativeSqlPlanner: true });

      query.buildSqlAndParams();
      expect((<any>query).preAggregations.preAggregationForQuery.canUsePreAggregation).toBe(true);

      const res = await dbRunner.evaluateQueryWithPreAggregations(query);
      expect(res).toEqual(rows);
    }
  });

  it('keeps serving an unshifted query from the rollup', async () => {
    const compilers = await compile(plainRollup);
    const query = new PostgresQuery(compilers, {
      ...shiftedQuery,
      measures: ['demand.revenue'],
      useNativeSqlPlanner: true,
    });

    query.buildSqlAndParams();
    expect((<any>query).preAggregations.preAggregationForQuery.canUsePreAggregation).toBe(true);

    const res = await dbRunner.evaluateQueryWithPreAggregations(query);
    expect(res).toEqual(expected.map(({ demand__revenue_ly, ...rest }) => rest));
  });
});
