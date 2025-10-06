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
          2024 + (case when gs.id < 41 then 0 else 1 end),
          (gs.id % 12) + 1,
          1 + (gs.id * 7 % 25),
          0,
          0,
          0
        ) AS created_at
      FROM generate_series(1, 80) AS gs(id)

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
        type: number
        multi_stage: true
        sql: "{count}"
        time_shift:
          - time_dimension: created_at
            interval: 1 year
            type: prior

      - name: count_shifted_calendar_y
        type: number
        multi_stage: true
        sql: "{count}"
        time_shift:
          - interval: 1 year
            type: prior

      - name: count_shifted_y_named
        type: number
        multi_stage: true
        sql: "{count}"
        time_shift:
          - name: one_year

      - name: count_shifted_y_named_common_interval
        type: number
        multi_stage: true
        sql: "{count}"
        time_shift:
          - name: one_year_common_interval

      - name: count_shifted_y1d_named
        type: number
        multi_stage: true
        sql: "{count}"
        time_shift:
          - name: one_year_and_one_day

      - name: count_shifted_calendar_m
        type: number
        multi_stage: true
        sql: "{count}"
        time_shift:
          - interval: 1 month
            type: prior

      - name: count_shifted_calendar_w
        type: number
        multi_stage: true
        sql: "{count}"
        time_shift:
          - interval: 1 week
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
    # language=SQL
    sql: >
      WITH base AS (SELECT gs.n - 1                       AS day_offset,
                           DATE '2024-02-04' + (gs.n - 1) AS date_val
                    FROM generate_series(1, 728) AS gs(n)
      ),
           retail_calc AS (SELECT date_val,
                                  date_val                      AS retail_date,

                                  CASE
                                      WHEN day_offset < 364 THEN '2024'
                                      ELSE '2025'
                                      END                       AS retail_year_name,

                                  (day_offset % 364)            AS day_of_retail_year,
                                  ((day_offset % 364) / 7) + 1  AS retail_week,
                                  ((day_offset % 364) / 91) + 1 AS retail_quarter,
                                  ((day_offset % 364) / 7) % 13 AS week_in_quarter,

                                  DATE '2024-02-04' + CASE
                                                          WHEN day_offset < 364 THEN 0
                                                          ELSE 364
                                      END                       AS retail_year_begin_date,

                                  ((day_offset / 7) / 13) * 3 +
                                  CASE
                                      WHEN ((day_offset / 7) % 13) < 4 THEN 1
                                      WHEN ((day_offset / 7) % 13) < 9 THEN 2
                                      ELSE 3
                                      END                       AS global_month,

                                  ((day_offset / 7) / 13) + 1   AS global_quarter,
                                  day_offset + 1                AS global_day_number,

                                  ((day_offset / 7) / 13) * 3 +
                                  CASE
                                      WHEN ((day_offset / 7) % 13) < 4 THEN 1
                                      WHEN ((day_offset / 7) % 13) < 9 THEN 2
                                      ELSE 3
                                      END - CASE
                                                WHEN day_offset < 364 THEN 0
                                                ELSE 12
                                      END                       AS retail_month_in_year,

                                  row_number() OVER (
                                      PARTITION BY
                                          ((day_offset / 7) / 13) * 3 +
                                          CASE
                                              WHEN ((day_offset / 7) % 13) < 4 THEN 1
                                              WHEN ((day_offset / 7) % 13) < 9 THEN 2
                                              ELSE 3
                                              END
                                      ORDER BY date_val
                                      )                         AS day_in_retail_month,

                                  row_number() OVER (
                                      PARTITION BY ((day_offset / 7) / 13) + 1
                                      ORDER BY date_val
                                      )                         AS day_in_retail_quarter,

                                  row_number() OVER (
                                      ORDER BY date_val
                                      )                         AS day_in_retail_year
                           FROM base),
           final AS (SELECT r.date_val::timestamp,
                            r.retail_date::timestamp,
                            r.retail_year_name,
                            ('Retail Month ' || r.retail_month_in_year)                        AS retail_month_long_name,
                            ('WK' || LPAD(r.retail_week::text, 2, '0'))                        AS retail_week_name,
                            r.retail_year_begin_date,
                            ('Q' || r.retail_quarter || ' ' || r.retail_year_name)             AS retail_quarter_year,

                            (SELECT MIN(date_val)
                             FROM retail_calc r2
                             WHERE r2.global_month = r.global_month
                               AND r2.day_in_retail_month = 1)                                 AS retail_month_begin_date,

                            r.date_val - (extract(dow from r.date_val)::int)                   AS retail_week_begin_date,
                            (r.retail_year_name || '-WK' || LPAD(r.retail_week::text, 2, '0')) AS retail_year_week,

                            r_prev_month.date_val::timestamp                                   AS retail_date_prev_month,
                            r_prev_quarter.date_val::timestamp                                 AS retail_date_prev_quarter,
                            r_prev_year.date_val::timestamp                                    AS retail_date_prev_year

                     FROM retail_calc r
                              LEFT JOIN retail_calc r_prev_month
                                        ON r_prev_month.global_month = r.global_month - 1
                                            AND r_prev_month.day_in_retail_month = r.day_in_retail_month
                              LEFT JOIN retail_calc r_prev_quarter
                                        ON r_prev_quarter.global_quarter = r.global_quarter - 1
                                            AND r_prev_quarter.day_in_retail_quarter = r.day_in_retail_quarter
                              LEFT JOIN retail_calc r_prev_year
                                        ON r_prev_year.global_day_number = r.global_day_number - 364)
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

        granularities:
          - name: year
            sql: "{CUBE}.retail_year_begin_date"

          - name: quarter
            sql: "{CUBE}.retail_quarter_year"

#          - name: month
#            sql: "{CUBE}.retail_month_begin_date"

          - name: week
            sql: "{CUBE}.retail_week_begin_date"

            # Casually defining custom granularities should also work.
            # While maybe not very sound from a business standpoint,
            # such definition should be allowed in this data model
          - name: fortnight
            interval: 2 week
            origin: "2025-01-01"

        time_shift:
          - interval: 1 month
            type: prior
            sql: "{CUBE}.retail_date_prev_month"

          - interval: 1 quarter
            type: prior
            sql: "{CUBE}.retail_date_prev_quarter"

          - interval: 1 year
            type: prior
            sql: "{CUBE}.retail_date_prev_year"

          - name: one_year
            sql: "{CUBE}.retail_date_prev_year"

          - name: one_year_common_interval
            interval: 1 year
            type: prior

          - name: one_year_and_one_day
            sql: "({CUBE}.retail_date_prev_year + interval '1 day')"

      ##### Retail Dates ####
      - name: retail_date
        sql: retail_date
        type: time

        granularities:
          - name: year
            sql: "{CUBE}.retail_year_begin_date"

          - name: quarter
            sql: "{CUBE}.retail_quarter_year"

#          - name: month
#            sql: "{CUBE}.retail_month_begin_date"

          - name: week
            sql: "{CUBE}.retail_week_begin_date"

            # Casually defining custom granularities should also work.
            # While maybe not very sound from a business standpoint,
            # such definition should be allowed in this data model
          - name: fortnight
            interval: 2 week
            origin: "2025-01-01"

        time_shift:
          - interval: 1 month
            type: prior
            sql: "{CUBE}.retail_date_prev_month"

          - interval: 1 quarter
            type: prior
            sql: "{CUBE}.retail_date_prev_quarter"

          - interval: 1 year
            type: prior
            sql: "{CUBE}.retail_date_prev_year"

          - name: one_year
            sql: "{CUBE}.retail_date_prev_year"

          - name: one_year_common_interval
            interval: 1 year
            type: prior

          - name: one_year_and_one_day
            sql: "({CUBE}.retail_date_prev_year + interval '1 day')"

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

  describe('Common queries to calendar cube', () => {
    it('Value of time-shift custom granularity non-pk time dimension', async () => runQueryTest({
      dimensions: ['custom_calendar.retail_date'],
      timeDimensions: [{
        dimension: 'custom_calendar.retail_date',
        dateRange: ['2025-02-02', '2025-02-06']
      }],
      order: [{ id: 'custom_calendar.retail_date' }]
    }, [
      {
        custom_calendar__retail_date: '2025-02-02T00:00:00.000Z',
      },
      {
        custom_calendar__retail_date: '2025-02-03T00:00:00.000Z',
      },
      {
        custom_calendar__retail_date: '2025-02-04T00:00:00.000Z',
      },
      {
        custom_calendar__retail_date: '2025-02-05T00:00:00.000Z',
      },
      {
        custom_calendar__retail_date: '2025-02-06T00:00:00.000Z',
      },
    ]));

    it('Year granularity of time-shift custom granularity non-pk time dimension', async () => runQueryTest({
      timeDimensions: [{
        dimension: 'custom_calendar.retail_date',
        granularity: 'year',
        dateRange: ['2025-02-02', '2025-02-06']
      }],
      order: [{ id: 'custom_calendar.retail_date' }]
    }, [
      {
        custom_calendar__retail_date_year: '2025-02-02T00:00:00.000Z',
      },
    ]));

    it('Value of time-shift custom granularity pk time dimension', async () => runQueryTest({
      dimensions: ['custom_calendar.date_val'],
      timeDimensions: [{
        dimension: 'custom_calendar.date_val',
        dateRange: ['2025-02-02', '2025-02-06']
      }],
      order: [{ id: 'custom_calendar.date_val' }]
    }, [
      {
        custom_calendar__date_val: '2025-02-02T00:00:00.000Z',
      },
      {
        custom_calendar__date_val: '2025-02-03T00:00:00.000Z',
      },
      {
        custom_calendar__date_val: '2025-02-04T00:00:00.000Z',
      },
      {
        custom_calendar__date_val: '2025-02-05T00:00:00.000Z',
      },
      {
        custom_calendar__date_val: '2025-02-06T00:00:00.000Z',
      },
    ]));

    it('Year granularity of time-shift custom granularity pk time dimension', async () => runQueryTest({
      timeDimensions: [{
        dimension: 'custom_calendar.date_val',
        granularity: 'year',
        dateRange: ['2025-02-02', '2025-02-06']
      }],
      order: [{ id: 'custom_calendar.date_val' }]
    }, [
      {
        custom_calendar__date_val_year: '2025-02-02T00:00:00.000Z',
      },
    ]));
  });

  describe('Custom granularities', () => {
    it('Count by retail year', async () => runQueryTest({
      measures: ['calendar_orders.count'],
      timeDimensions: [{
        dimension: 'custom_calendar.retail_date',
        granularity: 'year',
        dateRange: ['2025-02-02', '2026-02-01']
      }],
      order: [{ id: 'custom_calendar.retail_date' }]
    }, [
      {
        calendar_orders__count: '37',
        custom_calendar__retail_date_year: '2025-02-02T00:00:00.000Z',
      }
    ]));

    it('Count by retail month', async () => runQueryTest({
      measures: ['calendar_orders.count'],
      timeDimensions: [{
        dimension: 'custom_calendar.retail_date',
        granularity: 'month',
        dateRange: ['2025-02-02', '2026-02-01']
      }],
      order: [{ id: 'custom_calendar.retail_date' }]
    }, [
      {
        calendar_orders__count: '3',
        custom_calendar__retail_date_month: '2025-02-01T00:00:00.000Z',
      },
      {
        calendar_orders__count: '3',
        custom_calendar__retail_date_month: '2025-03-01T00:00:00.000Z',
      },
      {
        calendar_orders__count: '3',
        custom_calendar__retail_date_month: '2025-04-01T00:00:00.000Z',
      },
      {
        calendar_orders__count: '3',
        custom_calendar__retail_date_month: '2025-05-01T00:00:00.000Z',
      },
      {
        calendar_orders__count: '4',
        custom_calendar__retail_date_month: '2025-06-01T00:00:00.000Z',
      },
      {
        calendar_orders__count: '4',
        custom_calendar__retail_date_month: '2025-07-01T00:00:00.000Z',
      },
      {
        calendar_orders__count: '4',
        custom_calendar__retail_date_month: '2025-08-01T00:00:00.000Z',
      },
      {
        calendar_orders__count: '4',
        custom_calendar__retail_date_month: '2025-09-01T00:00:00.000Z',
      },
      {
        calendar_orders__count: '3',
        custom_calendar__retail_date_month: '2025-10-01T00:00:00.000Z',
      },
      {
        calendar_orders__count: '3',
        custom_calendar__retail_date_month: '2025-11-01T00:00:00.000Z',
      },
      {
        calendar_orders__count: '3',
        custom_calendar__retail_date_month: '2025-12-01T00:00:00.000Z',
      },
    ]));

    it('Count by retail week', async () => runQueryTest({
      measures: ['calendar_orders.count'],
      timeDimensions: [{
        dimension: 'custom_calendar.retail_date',
        granularity: 'week',
        dateRange: ['2025-02-02', '2025-04-01']
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
        custom_calendar__retail_date_week: '2025-02-23T00:00:00.000Z',
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
        custom_calendar__retail_date_week: '2025-03-30T00:00:00.000Z',
      },
    ]));

    it('Count by fortnight custom granularity', async () => runQueryTest({
      measures: ['calendar_orders.count'],
      timeDimensions: [{
        dimension: 'custom_calendar.retail_date',
        granularity: 'fortnight',
        dateRange: ['2025-02-02', '2025-04-01']
      }],
      order: [{ id: 'custom_calendar.retail_date' }]
    }, [
      {
        calendar_orders__count: '1',
        custom_calendar__retail_date_fortnight: '2025-01-29T00:00:00.000Z', // Notice it starts on 2025-01-29, not 2025-02-01
      },
      {
        calendar_orders__count: '2',
        custom_calendar__retail_date_fortnight: '2025-02-12T00:00:00.000Z',
      },
      {
        calendar_orders__count: '2',
        custom_calendar__retail_date_fortnight: '2025-02-26T00:00:00.000Z',
      },
      {
        calendar_orders__count: '1',
        custom_calendar__retail_date_fortnight: '2025-03-12T00:00:00.000Z',
      },
      {
        calendar_orders__count: '1',
        custom_calendar__retail_date_fortnight: '2025-03-26T00:00:00.000Z',
      },
    ]));
  });

  describe('Time-shifts', () => {
    describe('Non-PK dimension time-shifts', () => {
      it('Count shifted by retail year (custom shift + custom granularity)', async () => runQueryTest({
        measures: ['calendar_orders.count', 'calendar_orders.count_shifted_calendar_y'],
        timeDimensions: [{
          dimension: 'custom_calendar.retail_date',
          granularity: 'year',
          dateRange: ['2025-02-02', '2026-02-01']
        }],
        order: [{ id: 'custom_calendar.retail_date' }]
      }, [
        {
          calendar_orders__count: '37',
          calendar_orders__count_shifted_calendar_y: '39',
          custom_calendar__retail_date_year: '2025-02-02T00:00:00.000Z',
        },
      ]));

      it('Count shifted by retail year (custom named shift + custom granularity)', async () => runQueryTest({
        measures: ['calendar_orders.count', 'calendar_orders.count_shifted_y_named'],
        timeDimensions: [{
          dimension: 'custom_calendar.retail_date',
          granularity: 'year',
          dateRange: ['2025-02-02', '2026-02-01']
        }],
        order: [{ id: 'custom_calendar.retail_date' }]
      }, [
        {
          calendar_orders__count: '37',
          calendar_orders__count_shifted_y_named: '39',
          custom_calendar__retail_date_year: '2025-02-02T00:00:00.000Z',
        },
      ]));

      it('Count shifted by retail month (custom shift + common granularity)', async () => runQueryTest({
        measures: ['calendar_orders.count', 'calendar_orders.count_shifted_calendar_m'],
        timeDimensions: [{
          dimension: 'custom_calendar.retail_date',
          granularity: 'month',
          dateRange: ['2025-02-02', '2026-02-01']
        }],
        order: [{ id: 'custom_calendar.retail_date' }]
      }, [
        {
          calendar_orders__count: '3',
          calendar_orders__count_shifted_calendar_m: '3',
          custom_calendar__retail_date_month: '2025-02-01T00:00:00.000Z',
        },
        {
          calendar_orders__count: '3',
          calendar_orders__count_shifted_calendar_m: '4',
          custom_calendar__retail_date_month: '2025-03-01T00:00:00.000Z',
        },
        {
          calendar_orders__count: '3',
          calendar_orders__count_shifted_calendar_m: '2',
          custom_calendar__retail_date_month: '2025-04-01T00:00:00.000Z',
        },
        {
          calendar_orders__count: '3',
          calendar_orders__count_shifted_calendar_m: '2',
          custom_calendar__retail_date_month: '2025-05-01T00:00:00.000Z',
        },
        {
          calendar_orders__count: '4',
          calendar_orders__count_shifted_calendar_m: '3',
          custom_calendar__retail_date_month: '2025-06-01T00:00:00.000Z',
        },
        {
          calendar_orders__count: '4',
          calendar_orders__count_shifted_calendar_m: '4',
          custom_calendar__retail_date_month: '2025-07-01T00:00:00.000Z',
        },
        {
          calendar_orders__count: '4',
          calendar_orders__count_shifted_calendar_m: '4',
          custom_calendar__retail_date_month: '2025-08-01T00:00:00.000Z',
        },
        {
          calendar_orders__count: '4',
          calendar_orders__count_shifted_calendar_m: '3',
          custom_calendar__retail_date_month: '2025-09-01T00:00:00.000Z',
        },
        {
          calendar_orders__count: '3',
          calendar_orders__count_shifted_calendar_m: '4',
          custom_calendar__retail_date_month: '2025-10-01T00:00:00.000Z',
        },
        {
          calendar_orders__count: '3',
          calendar_orders__count_shifted_calendar_m: '3',
          custom_calendar__retail_date_month: '2025-11-01T00:00:00.000Z',
        },
        {
          calendar_orders__count: '3',
          calendar_orders__count_shifted_calendar_m: '3',
          custom_calendar__retail_date_month: '2025-12-01T00:00:00.000Z',
        },
        {
          calendar_orders__count: null,
          calendar_orders__count_shifted_calendar_m: '3',
          custom_calendar__retail_date_month: '2026-01-01T00:00:00.000Z',
        },
      ]));

      it('Count shifted by retail week (common shift + custom granularity)', async () => runQueryTest({
        measures: ['calendar_orders.count', 'calendar_orders.count_shifted_calendar_w'],
        timeDimensions: [{
          dimension: 'custom_calendar.retail_date',
          granularity: 'week',
          dateRange: ['2025-02-02', '2025-04-12']
        }],
        order: [{ id: 'custom_calendar.retail_date' }]
      }, [
        {
          calendar_orders__count: '1',
          calendar_orders__count_shifted_calendar_w: null,
          custom_calendar__retail_date_week: '2025-02-02T00:00:00.000Z',
        },
        {
          calendar_orders__count: '1',
          calendar_orders__count_shifted_calendar_w: '1',
          custom_calendar__retail_date_week: '2025-02-09T00:00:00.000Z',
        },
        {
          calendar_orders__count: '1',
          calendar_orders__count_shifted_calendar_w: '1',
          custom_calendar__retail_date_week: '2025-02-16T00:00:00.000Z',
        },
        {
          calendar_orders__count: '1',
          calendar_orders__count_shifted_calendar_w: '1',
          custom_calendar__retail_date_week: '2025-02-23T00:00:00.000Z',
        },
        {
          calendar_orders__count: null,
          calendar_orders__count_shifted_calendar_w: '1',
          custom_calendar__retail_date_week: '2025-03-02T00:00:00.000Z',
        },
        {
          calendar_orders__count: '1',
          calendar_orders__count_shifted_calendar_w: null,
          custom_calendar__retail_date_week: '2025-03-09T00:00:00.000Z',
        },
        {
          calendar_orders__count: '1',
          calendar_orders__count_shifted_calendar_w: '1',
          custom_calendar__retail_date_week: '2025-03-16T00:00:00.000Z',
        },
        {
          calendar_orders__count: null,
          calendar_orders__count_shifted_calendar_w: '1',
          custom_calendar__retail_date_week: '2025-03-23T00:00:00.000Z',
        },
        {
          calendar_orders__count: '1',
          calendar_orders__count_shifted_calendar_w: null,
          custom_calendar__retail_date_week: '2025-03-30T00:00:00.000Z',
        },
        {
          calendar_orders__count: '1',
          calendar_orders__count_shifted_calendar_w: '1',
          custom_calendar__retail_date_week: '2025-04-06T00:00:00.000Z',
        },
      ]));

      it('Count shifted by retail year and another custom calendar year (2 custom named shifts + custom granularity)', async () => runQueryTest({
        measures: ['calendar_orders.count', 'calendar_orders.count_shifted_y_named', 'calendar_orders.count_shifted_y1d_named'],
        timeDimensions: [{
          dimension: 'custom_calendar.retail_date',
          granularity: 'year',
          dateRange: ['2025-02-02', '2026-02-01']
        }],
        order: [{ id: 'custom_calendar.retail_date' }]
      }, [
        {
          calendar_orders__count: '37',
          calendar_orders__count_shifted_y_named: '39',
          calendar_orders__count_shifted_y1d_named: '39',
          custom_calendar__retail_date_year: '2025-02-02T00:00:00.000Z',
        },
      ]));

      it('Count shifted by year (custom named shift with common interval + custom granularity)', async () => runQueryTest({
        measures: ['calendar_orders.count', 'calendar_orders.count_shifted_y_named_common_interval'],
        timeDimensions: [{
          dimension: 'custom_calendar.retail_date',
          granularity: 'year',
          dateRange: ['2025-02-02', '2026-02-01']
        }],
        order: [{ id: 'custom_calendar.retail_date' }]
      }, [
        {
          calendar_orders__count: '37',
          calendar_orders__count_shifted_y_named_common_interval: '39',
          custom_calendar__retail_date_year: '2025-02-02T00:00:00.000Z',
        },
      ]));
    });

    describe('PK dimension time-shifts', () => {
      it.skip('Count shifted by retail year (custom shift + custom granularity)1', async () => runQueryTest({
        measures: ['calendar_orders.count', 'calendar_orders.count_shifted_calendar_y'],
        timeDimensions: [{
          dimension: 'custom_calendar.date_val',
          granularity: 'year',
          dateRange: ['2025-02-02', '2026-02-01']
        }],
        order: [{ id: 'custom_calendar.date_val' }]
      }, [
        {
          calendar_orders__count: '37',
          calendar_orders__count_shifted_calendar_y: '39',
          custom_calendar__date_val_year: '2025-02-02T00:00:00.000Z',
        },
      ]));

      it.skip('Count shifted by retail year (custom named shift + custom granularity)1', async () => runQueryTest({
        measures: ['calendar_orders.count', 'calendar_orders.count_shifted_y_named'],
        timeDimensions: [{
          dimension: 'custom_calendar.date_val',
          granularity: 'year',
          dateRange: ['2025-02-02', '2026-02-01']
        }],
        order: [{ id: 'custom_calendar.date_val' }]
      }, [
        {
          calendar_orders__count: '37',
          calendar_orders__count_shifted_y_named: '39',
          custom_calendar__date_val_year: '2025-02-02T00:00:00.000Z',
        },
      ]));

      it.skip('Count shifted by retail month (custom shift + common granularity)', async () => runQueryTest({
        measures: ['calendar_orders.count', 'calendar_orders.count_shifted_calendar_m'],
        timeDimensions: [{
          dimension: 'custom_calendar.date_val',
          granularity: 'month',
          dateRange: ['2025-02-02', '2026-02-01']
        }],
        order: [{ id: 'custom_calendar.date_val' }]
      }, [
        {
          calendar_orders__count: '3',
          calendar_orders__count_shifted_calendar_m: '3',
          custom_calendar__date_val_month: '2025-02-01T00:00:00.000Z',
        },
        {
          calendar_orders__count: '3',
          calendar_orders__count_shifted_calendar_m: '4',
          custom_calendar__date_val_month: '2025-03-01T00:00:00.000Z',
        },
        {
          calendar_orders__count: '3',
          calendar_orders__count_shifted_calendar_m: '2',
          custom_calendar__date_val_month: '2025-04-01T00:00:00.000Z',
        },
        {
          calendar_orders__count: '3',
          calendar_orders__count_shifted_calendar_m: '2',
          custom_calendar__date_val_month: '2025-05-01T00:00:00.000Z',
        },
        {
          calendar_orders__count: '4',
          calendar_orders__count_shifted_calendar_m: '3',
          custom_calendar__date_val_month: '2025-06-01T00:00:00.000Z',
        },
        {
          calendar_orders__count: '4',
          calendar_orders__count_shifted_calendar_m: '4',
          custom_calendar__date_val_month: '2025-07-01T00:00:00.000Z',
        },
        {
          calendar_orders__count: '4',
          calendar_orders__count_shifted_calendar_m: '4',
          custom_calendar__date_val_month: '2025-08-01T00:00:00.000Z',
        },
        {
          calendar_orders__count: '4',
          calendar_orders__count_shifted_calendar_m: '3',
          custom_calendar__date_val_month: '2025-09-01T00:00:00.000Z',
        },
        {
          calendar_orders__count: '3',
          calendar_orders__count_shifted_calendar_m: '4',
          custom_calendar__date_val_month: '2025-10-01T00:00:00.000Z',
        },
        {
          calendar_orders__count: '3',
          calendar_orders__count_shifted_calendar_m: '3',
          custom_calendar__date_val_month: '2025-11-01T00:00:00.000Z',
        },
        {
          calendar_orders__count: '3',
          calendar_orders__count_shifted_calendar_m: '3',
          custom_calendar__date_val_month: '2025-12-01T00:00:00.000Z',
        },
      ]));

      it.skip('Count shifted by retail week (common shift + custom granularity)', async () => runQueryTest({
        measures: ['calendar_orders.count', 'calendar_orders.count_shifted_calendar_w'],
        timeDimensions: [{
          dimension: 'custom_calendar.date_val',
          granularity: 'week',
          dateRange: ['2025-02-02', '2026-02-01']
        }],
        order: [{ id: 'custom_calendar.date_val' }]
      }, [
        {
          calendar_orders__count: '1',
          calendar_orders__count_shifted_calendar_w: '1',
          custom_calendar__date_val_week: '2025-02-09T00:00:00.000Z',
        },
        {
          calendar_orders__count: '1',
          calendar_orders__count_shifted_calendar_w: '1',
          custom_calendar__date_val_week: '2025-02-16T00:00:00.000Z',
        },
        {
          calendar_orders__count: '1',
          calendar_orders__count_shifted_calendar_w: '1',
          custom_calendar__date_val_week: '2025-02-23T00:00:00.000Z',
        },
        {
          calendar_orders__count: '1',
          calendar_orders__count_shifted_calendar_w: '1',
          custom_calendar__date_val_week: '2025-03-16T00:00:00.000Z',
        },
        {
          calendar_orders__count: '1',
          calendar_orders__count_shifted_calendar_w: '1',
          custom_calendar__date_val_week: '2025-04-06T00:00:00.000Z',
        },
      ]));

      it.skip('Count shifted by retail year and another custom calendar year (2 custom named shifts + custom granularity)', async () => runQueryTest({
        measures: ['calendar_orders.count', 'calendar_orders.count_shifted_y_named', 'calendar_orders.count_shifted_y1d_named'],
        timeDimensions: [{
          dimension: 'custom_calendar.date_val',
          granularity: 'year',
          dateRange: ['2025-02-02', '2026-02-01']
        }],
        order: [{ id: 'custom_calendar.date_val' }]
      }, [
        {
          calendar_orders__count: '37',
          calendar_orders__count_shifted_y_named: '39',
          calendar_orders__count_shifted_y1d_named: '39',
          custom_calendar__date_val_year: '2025-02-02T00:00:00.000Z',
        },
      ]));

      it.skip('Count shifted by year (custom named shift with common interval + custom granularity)', async () => runQueryTest({
        measures: ['calendar_orders.count', 'calendar_orders.count_shifted_y_named_common_interval'],
        timeDimensions: [{
          dimension: 'custom_calendar.date_val',
          granularity: 'year',
          dateRange: ['2025-02-02', '2026-02-01']
        }],
        order: [{ id: 'custom_calendar.date_val' }]
      }, [
        {
          calendar_orders__count: '37',
          calendar_orders__count_shifted_y_named_common_interval: '39',
          custom_calendar__retail_date_year: '2025-02-02T00:00:00.000Z',
        },
      ]));
    });
  });
});
