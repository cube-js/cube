import { getEnv } from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Calendar cube to-date rolling window', () => {
  jest.setTimeout(200000);

  // Fiscal weeks start on Sunday 2023-12-17 and run for 7 days, so they line up
  // with neither the ISO week nor any interval anchored at the start of a year.
  // language=YAML
  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: fiscal_calendar
    calendar: true
    sql: >
      SELECT (DATE '2023-12-17' + (gs.n - 1))::date AS cal_date,
             (DATE '2023-12-17' + ((gs.n - 1) / 7) * 7)::date AS wk_start_dt,
             CASE
               WHEN gs.n - 1 < 28 THEN DATE '2023-12-17'
               WHEN gs.n - 1 < 63 THEN DATE '2024-01-14'
               ELSE DATE '2024-02-18'
             END::date AS mo_start_dt
      FROM generate_series(1, 91) AS gs(n)
    dimensions:
      - name: date_key
        sql: cal_date
        type: time
        primary_key: true
      - name: date
        sql: cal_date
        type: time
        granularities:
          - name: week
            sql: "{CUBE}.wk_start_dt"
          - name: month
            sql: "{CUBE}.mo_start_dt"

  - name: sales
    sql: >
      SELECT gs.n::int AS id,
             (DATE '2023-12-17' + (gs.n - 1))::date AS date,
             10 AS amount
      FROM generate_series(1, 91) AS gs(n)
    joins:
      - name: fiscal_calendar
        sql: "{CUBE}.date = {fiscal_calendar.date_key}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: wtd_amount
        sql: amount
        type: sum
        rolling_window:
          type: to_date
          granularity: week

      - name: mtd_amount
        sql: amount
        type: sum
        rolling_window:
          type: to_date
          granularity: month

      - name: trailing_amount
        sql: amount
        type: sum
        rolling_window:
          trailing: 3 day
          offset: end
`);

  async function runQueryTest(q: any, expectedResult: any) {
    // Calendars are working only with Tesseract SQL planner
    if (!getEnv('nativeSqlPlanner')) {
      return;
    }

    await compiler.compile();
    const query = new PostgresQuery(
      { joinGraph, cubeEvaluator, compiler },
      { ...q, timezone: 'UTC', preAggregationsSchema: '' }
    );

    const res = await dbRunner.testQuery(query.buildSqlAndParams());

    expect(res).toEqual(expectedResult);
  }

  it('accumulates within the calendar week, not within a natural one', async () => runQueryTest({
    measures: ['sales.wtd_amount'],
    timeDimensions: [{
      dimension: 'fiscal_calendar.date',
      granularity: 'day',
      dateRange: ['2023-12-19', '2023-12-26'],
    }],
    order: [{ id: 'fiscal_calendar.date' }],
  }, [
    // Fiscal week of 2023-12-17 accumulates through 2023-12-23...
    { fiscal_calendar__date_day: '2023-12-19T00:00:00.000Z', sales__wtd_amount: '30' },
    { fiscal_calendar__date_day: '2023-12-20T00:00:00.000Z', sales__wtd_amount: '40' },
    { fiscal_calendar__date_day: '2023-12-21T00:00:00.000Z', sales__wtd_amount: '50' },
    { fiscal_calendar__date_day: '2023-12-22T00:00:00.000Z', sales__wtd_amount: '60' },
    { fiscal_calendar__date_day: '2023-12-23T00:00:00.000Z', sales__wtd_amount: '70' },
    // ...and resets on 2023-12-24, where the next fiscal week starts.
    { fiscal_calendar__date_day: '2023-12-24T00:00:00.000Z', sales__wtd_amount: '10' },
    { fiscal_calendar__date_day: '2023-12-25T00:00:00.000Z', sales__wtd_amount: '20' },
    { fiscal_calendar__date_day: '2023-12-26T00:00:00.000Z', sales__wtd_amount: '30' },
  ]));

  it('bounds each window by its own calendar period', async () => runQueryTest({
    measures: ['sales.wtd_amount', 'sales.mtd_amount'],
    timeDimensions: [{
      dimension: 'fiscal_calendar.date',
      granularity: 'day',
      dateRange: ['2023-12-23', '2023-12-25'],
    }],
    order: [{ id: 'fiscal_calendar.date' }],
  }, [
    // Fiscal weeks are 7 days; the fiscal month running from 2023-12-17 is 28.
    { fiscal_calendar__date_day: '2023-12-23T00:00:00.000Z', sales__wtd_amount: '70', sales__mtd_amount: '70' },
    { fiscal_calendar__date_day: '2023-12-24T00:00:00.000Z', sales__wtd_amount: '10', sales__mtd_amount: '80' },
    { fiscal_calendar__date_day: '2023-12-25T00:00:00.000Z', sales__wtd_amount: '20', sales__mtd_amount: '90' },
  ]));

  it('ends a period where the calendar ends it, not one nominal interval later', async () => runQueryTest({
    measures: ['sales.mtd_amount'],
    timeDimensions: [{
      dimension: 'fiscal_calendar.date',
      granularity: 'month',
      dateRange: ['2023-12-17', '2024-03-16'],
    }],
    order: [{ id: 'fiscal_calendar.date' }],
  }, [
    // 28, 35 and 28 days at 10 a day. A nominal `1 month` upper bound would
    // reach past the 28-day period and fold the next one into it.
    { fiscal_calendar__date_month: '2023-12-17T00:00:00.000Z', sales__mtd_amount: '280' },
    { fiscal_calendar__date_month: '2024-01-14T00:00:00.000Z', sales__mtd_amount: '350' },
    { fiscal_calendar__date_month: '2024-02-18T00:00:00.000Z', sales__mtd_amount: '280' },
  ]));

  it('leaves a regular window on the same series alone', async () => runQueryTest({
    measures: ['sales.trailing_amount', 'sales.mtd_amount'],
    timeDimensions: [{
      dimension: 'fiscal_calendar.date',
      granularity: 'day',
      dateRange: ['2023-12-22', '2023-12-25'],
    }],
    order: [{ id: 'fiscal_calendar.date' }],
  }, [
    // The trailing window keeps counting across the fiscal boundary the
    // to-date window resets on.
    { fiscal_calendar__date_day: '2023-12-22T00:00:00.000Z', sales__trailing_amount: '30', sales__mtd_amount: '60' },
    { fiscal_calendar__date_day: '2023-12-23T00:00:00.000Z', sales__trailing_amount: '30', sales__mtd_amount: '70' },
    { fiscal_calendar__date_day: '2023-12-24T00:00:00.000Z', sales__trailing_amount: '30', sales__mtd_amount: '80' },
    { fiscal_calendar__date_day: '2023-12-25T00:00:00.000Z', sales__trailing_amount: '30', sales__mtd_amount: '90' },
  ]));

  it('resolves the series range at query time when none is given', async () => runQueryTest({
    measures: ['sales.wtd_amount'],
    timeDimensions: [{
      dimension: 'fiscal_calendar.date',
      granularity: 'week',
    }],
    order: [{ id: 'fiscal_calendar.date' }],
  }, Array.from({ length: 13 }, (_, i) => ({
    fiscal_calendar__date_week: new Date(Date.UTC(2023, 11, 17 + i * 7)).toISOString().replace('Z', 'Z'),
    sales__wtd_amount: '70',
  }))));
});
