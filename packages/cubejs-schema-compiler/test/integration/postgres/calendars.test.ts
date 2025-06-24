import { getEnv } from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Calendar cubes', () => {
  jest.setTimeout(200000);

  // language=YAML
  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: calendar_orders
    sql: >
      SELECT
        gs.id,
        100 + gs.id AS user_id,
        (ARRAY['new', 'processed', 'shipped'])[(gs.id % 3) + 1] AS status,
        make_timestamp(
          2025,
          (gs.id % 12) + 1,
          1 + (gs.id * 7 % 25),
          0,
          0,
          0
        ) AS created_at
      FROM generate_series(1, 40) AS gs(id)

    joins:
      - name: custom_calendar
        sql: "{CUBE}.created_at = {custom_calendar.date_val}"
        relationship: many_to_one

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
        public: true

      - name: user_id
        sql: user_id
        type: number

      - name: status
        sql: status
        type: string
        meta:
          addDesc: The status of order
          moreNum: 42

      - name: created_at
        sql: created_at
        type: time

    measures:
      - name: count
        type: count

      - name: count_shifted
        type: count
        multi_stage: true
        sql: "{count}"
        time_shift:
          - time_dimension: created_at
            interval: 1 year
            type: prior

      - name: completed_count
        type: count
        filters:
          - sql: "{CUBE}.status = 'completed'"

      - name: completed_percentage
        sql: "({completed_count} / NULLIF({count}, 0)) * 100.0"
        type: number
        format: percent

      - name: total
        type: count
        rolling_window:
          trailing: unbounded

  - name: custom_calendar
    sql: >
      WITH base AS (
        SELECT
          gs.n - 1 AS day_offset,
          DATE '2025-02-02' + (gs.n - 1) AS date_val
        FROM generate_series(1, 364) AS gs(n)
      ),
      retail_calc AS (
        SELECT
          date_val,
          date_val AS retail_date,
          '2025' AS retail_year_name,
          (day_offset / 7) + 1 AS retail_week,
          -- Group of months 4-5-4 (13 weeks = 3 months)
          ((day_offset / 7) / 13) + 1 AS retail_quarter,
          (day_offset / 7) % 13 AS week_in_quarter,
          DATE '2025-02-02' AS retail_year_begin_date
        FROM base
      ),
      final AS (
        SELECT
          date_val,
          retail_date,
          retail_year_name,
          ('Retail Month ' || ((retail_quarter - 1) * 3 +
            CASE
              WHEN week_in_quarter < 4 THEN 1
              WHEN week_in_quarter < 9 THEN 2
              ELSE 3
            END)) AS retail_month_long_name,
          ('WK' || LPAD(retail_week::text, 2, '0')) AS retail_week_name,
          retail_year_begin_date,
          ('Q' || retail_quarter || ' 2025') AS retail_quarter_year,
          (SELECT MIN(date_val) FROM retail_calc r2
           WHERE r2.retail_quarter = r.retail_quarter
             AND CASE
                   WHEN week_in_quarter < 4 THEN 1
                   WHEN week_in_quarter < 9 THEN 2
                   ELSE 3
                 END =
                 CASE
                   WHEN r.week_in_quarter < 4 THEN 1
                   WHEN r.week_in_quarter < 9 THEN 2
                   ELSE 3
                 END
          ) AS retail_month_begin_date,
          date_val - (extract(dow from date_val)::int) AS retail_week_begin_date,
          ('2025-WK' || LPAD(retail_week::text, 2, '0')) AS retail_year_week
        FROM retail_calc r
      )
      SELECT *
      FROM final
      ORDER BY date_val

    calendar: true

    dimensions:
      # Plain date value
      - name: date_val
        sql: "{CUBE}.date_val"
        type: time
        primary_key: true

      ##### Retail Dates ####
      - name: retail_date
        sql: retail_date
        type: time

        granularities:
          - name: year
            sql: "{CUBE.retail_year_begin_date}"

          - name: quarter
            sql: "{CUBE.retail_quarter_year}"

          - name: month
            sql: "{CUBE.retail_month_begin_date}"

          - name: week
            sql: "{CUBE.retail_week_begin_date}"

            # Casually defining custom granularities should also work.
            # While maybe not very sound from a business standpoint,
            # such definition should be allowed in this data model
          - name: fortnight
            interval: 2 week
            origin: "2025-01-01"

      - name: retail_year
        sql: "{CUBE}.retail_year_name"
        type: string

      - name: retail_month_long_name
        sql: "{CUBE}.retail_month_long_name"
        type: string

      - name: retail_week_name
        sql: "{CUBE}.retail_week_name"
        type: string

      - name: retail_year_begin_date
        sql: "{CUBE}.retail_year_begin_date"
        type: time

      - name: retail_quarter_year
        sql: "{CUBE}.retail_quarter_year"
        type: string

      - name: retail_month_begin_date
        sql: "{CUBE}.retail_month_begin_date"
        type: string

      - name: retail_week_begin_date
        sql: "{CUBE}.retail_week_begin_date"
        type: string

      - name: retail_year_week
        sql: "{CUBE}.retail_year_week"
        type: string
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

    const qp = query.buildSqlAndParams();
    console.log(qp);

    const res = await dbRunner.testQuery(qp);
    console.log(JSON.stringify(res));

    expect(res).toEqual(
      expectedResult
    );
  }

  it('Count by retail year', async () => runQueryTest({
    measures: ['calendar_orders.count'],
    timeDimensions: [{
      dimension: 'custom_calendar.retail_date',
      granularity: 'year',
      dateRange: ['2025-02-01', '2026-03-01']
    }],
    order: [{ id: 'custom_calendar.retail_date' }]
  }, [
    {
      calendar_orders__count: '36',
      custom_calendar__retail_date_year: '2025-02-02T00:00:00.000Z',
    }
  ]));

  it('Count by retail month', async () => runQueryTest({
    measures: ['calendar_orders.count'],
    timeDimensions: [{
      dimension: 'custom_calendar.retail_date',
      granularity: 'month',
      dateRange: ['2025-02-01', '2026-03-01']
    }],
    order: [{ id: 'custom_calendar.retail_date' }]
  }, [
    {
      calendar_orders__count: '3',
      custom_calendar__retail_date_month: '2025-02-02T00:00:00.000Z',
    },
    {
      calendar_orders__count: '4',
      custom_calendar__retail_date_month: '2025-03-02T00:00:00.000Z',
    },
    {
      calendar_orders__count: '4',
      custom_calendar__retail_date_month: '2025-04-06T00:00:00.000Z',
    },
    {
      calendar_orders__count: '4',
      custom_calendar__retail_date_month: '2025-05-04T00:00:00.000Z',
    },
    {
      calendar_orders__count: '4',
      custom_calendar__retail_date_month: '2025-06-01T00:00:00.000Z',
    },
    {
      calendar_orders__count: '2',
      custom_calendar__retail_date_month: '2025-07-06T00:00:00.000Z',
    },
    {
      calendar_orders__count: '3',
      custom_calendar__retail_date_month: '2025-08-03T00:00:00.000Z',
    },
    {
      calendar_orders__count: '3',
      custom_calendar__retail_date_month: '2025-08-31T00:00:00.000Z',
    },
    {
      calendar_orders__count: '3',
      custom_calendar__retail_date_month: '2025-10-05T00:00:00.000Z',
    },
    {
      calendar_orders__count: '3',
      custom_calendar__retail_date_month: '2025-11-02T00:00:00.000Z',
    },
    {
      calendar_orders__count: '3',
      custom_calendar__retail_date_month: '2025-11-30T00:00:00.000Z',
    }
  ]));

  it('Count by retail week', async () => runQueryTest({
    measures: ['calendar_orders.count'],
    timeDimensions: [{
      dimension: 'custom_calendar.retail_date',
      granularity: 'week',
      dateRange: ['2025-02-01', '2025-04-01']
    }],
    order: [{ id: 'custom_calendar.retail_date' }]
  }, [
    {
      calendar_orders__count: '1',
      custom_calendar__retail_date_week: '2025-02-02T00:00:00.000Z',
    },
    {
      calendar_orders__count: '1',
      custom_calendar__retail_date_week: '2025-02-09T00:00:00.000Z',
    },
    {
      calendar_orders__count: '1',
      custom_calendar__retail_date_week: '2025-02-16T00:00:00.000Z',
    },
    {
      calendar_orders__count: '1',
      custom_calendar__retail_date_week: '2025-03-02T00:00:00.000Z',
    },
    {
      calendar_orders__count: '1',
      custom_calendar__retail_date_week: '2025-03-09T00:00:00.000Z',
    },
    {
      calendar_orders__count: '1',
      custom_calendar__retail_date_week: '2025-03-16T00:00:00.000Z',
    },
    {
      calendar_orders__count: '1',
      custom_calendar__retail_date_week: '2025-03-23T00:00:00.000Z',
    }
  ]));

  it('Count by fortnight custom granularity', async () => runQueryTest({
    measures: ['calendar_orders.count'],
    timeDimensions: [{
      dimension: 'custom_calendar.retail_date',
      granularity: 'fortnight',
      dateRange: ['2025-02-01', '2025-04-01']
    }],
    order: [{ id: 'custom_calendar.retail_date' }]
  }, [
    {
      calendar_orders__count: '2',
      custom_calendar__retail_date_fortnight: '2025-01-29T00:00:00.000Z', // Notice it starts on 2025-01-29, not 2025-02-01
    },
    {
      calendar_orders__count: '1',
      custom_calendar__retail_date_fortnight: '2025-02-12T00:00:00.000Z',
    },
    {
      calendar_orders__count: '1',
      custom_calendar__retail_date_fortnight: '2025-02-26T00:00:00.000Z',
    },
    {
      calendar_orders__count: '3',
      custom_calendar__retail_date_fortnight: '2025-03-12T00:00:00.000Z',
    }
  ]));
});
