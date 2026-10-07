import { getEnv } from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// A multi-stage `filter.include` date range on a time dimension anchors the
// rolling windows it wraps, as a query `dateRange` would, so the measure works
// in a query without time dimensions.
describe('Multi-stage filter include with a date range', () => {
  jest.setTimeout(200000);

  // Daily amounts in `visitors`: Jan 3 = 100, Jan 5 = 200, Jan 6 = 300,
  // Jan 7 = 900. `recent` has rows relative to today.
  const model = (range: string, rollup: boolean) => `
cubes:
  - name: visitors_fi
    sql: "select * from visitors"
    sql_alias: vfi
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: created_at
        sql: created_at
        type: time
        granularities:
          - name: half_year
            interval: 6 months
            origin: "2016-07-01"
    measures:
      - name: amount
        sql: amount
        type: sum
      - name: amount_htd
        multi_stage: true
        type: sum
        sql: "{amount}"
        rolling_window:
          type: to_date
          granularity: half_year
      - name: amount_r3
        multi_stage: true
        type: sum
        sql: "{amount}"
        rolling_window:
          trailing: 3 day
      - name: amount_mtd
        multi_stage: true
        type: sum
        sql: "{amount}"
        rolling_window:
          type: to_date
          granularity: month
      - name: amount_r3_on
        multi_stage: true
        type: number
        sql: "{amount_r3}"
        filter:
          include:
            - member: visitors_fi.created_at
              operator: inDateRange
              values: ${range}
      - name: amount_mtd_on
        multi_stage: true
        type: number
        sql: "{amount_mtd}"
        filter:
          include:
            - member: visitors_fi.created_at
              operator: inDateRange
              values: ${range}
      - name: amount_r3_on_positive
        multi_stage: true
        type: number
        sql: "{amount_r3}"
        filter:
          include:
            - and:
                - member: visitors_fi.created_at
                  operator: inDateRange
                  values: ${range}
                - member: visitors_fi.id
                  operator: gt
                  values: ["0"]
${rollup ? `    pre_aggregations:
      - name: daily
        measures:
          - amount
        time_dimension: created_at
        granularity: day
        partition_granularity: day
` : ''}
  - name: recent
    # Rows a few days inside the ranges below, so a day changing between
    # planning and execution moves no row across a bound.
    sql: >
      select 1 as id, 100 as amount, now() - interval '3 day' as created_at union all
      select 2, 200, now() - interval '4 day' union all
      select 3, 1000, now() - interval '30 day'
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: created_at
        sql: created_at
        type: time
    measures:
      - name: amount
        sql: amount
        type: sum
      - name: amount_r10
        multi_stage: true
        type: sum
        sql: "{amount}"
        rolling_window:
          trailing: 10 day
      - name: amount_r10_yesterday
        multi_stage: true
        type: number
        sql: "{amount_r10}"
        filter:
          include:
            - member: recent.created_at
              operator: inDateRange
              values: [yesterday]
      - name: amount_last_7_days
        multi_stage: true
        type: number
        sql: "{amount}"
        filter:
          include:
            - member: recent.created_at
              operator: inDateRange
              values: [last 7 days]
`;

  const evaluate = async (
    measures: string[],
    { range = '["2017-01-07", "2017-01-07"]', withPreAggregations = false, timeDimensions = [] as any[] } = {},
  ) => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(model(range, withPreAggregations));
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures,
      timeDimensions,
      timezone: 'UTC',
      preAggregationsSchema: '',
    });

    if (withPreAggregations) {
      expect(query.buildSqlAndParams()[0]).toContain('vfi_daily');
      return dbRunner.evaluateQueryWithPreAggregations(query);
    }
    return dbRunner.testQuery(query.buildSqlAndParams());
  };

  if (getEnv('nativeSqlPlanner')) {
    it('a trailing window reads the days before the included range', async () => {
      expect(await evaluate(['visitors_fi.amount_r3_on'])).toEqual([{ vfi__amount_r3_on: '1400' }]);
    });

    it('a to_date window reads from the start of the period', async () => {
      expect(await evaluate(['visitors_fi.amount_mtd_on'])).toEqual([{ vfi__amount_mtd_on: '1500' }]);
    });

    it('a rollup serves the windows with the partitions they read', async () => {
      expect(await evaluate(['visitors_fi.amount_r3_on', 'visitors_fi.amount_mtd_on'], { withPreAggregations: true })).toEqual([{
        vfi__amount_r3_on: '1400',
        vfi__amount_mtd_on: '1500',
      }]);
    });

    it('a relative date range resolves at query time', async () => {
      expect(await evaluate(['recent.amount_r10_yesterday', 'recent.amount_last_7_days'])).toEqual([{
        recent__amount_r10_yesterday: '300',
        recent__amount_last_7_days: '300',
      }]);
    });

    it('a date range nested in a top-level and anchors the window too', async () => {
      expect(await evaluate(['visitors_fi.amount_r3_on_positive'])).toEqual([{ vfi__amount_r3_on_positive: '1400' }]);
    });

    it('a relative value it cannot resolve is an error', async () => {
      await expect(evaluate(['visitors_fi.amount_r3_on'], { range: '[2 weeks ago]' }))
        .rejects.toThrow("Can't parse date '2 weeks ago'");
    });

    it('a relative value next to an absolute one is an error', async () => {
      await expect(evaluate(['visitors_fi.amount_r3_on'], { range: '[last month, "2017-01-07"]' }))
        .rejects.toThrow("Can't parse date 'last month'");
    });

    it('a query date range on the same dimension still bounds the result', async () => {
      // Jan 1 - 5 of the month to Jan 7.
      expect(await evaluate(['visitors_fi.amount_mtd_on'], {
        timeDimensions: [{ dimension: 'visitors_fi.created_at', dateRange: ['2017-01-01', '2017-01-05'] }],
      })).toEqual([{ vfi__amount_mtd_on: '300' }]);
    });

    it('a to_date window over a query date range without granularity reads from the start of the period', async () => {
      expect(await evaluate(['visitors_fi.amount_mtd'], {
        timeDimensions: [{ dimension: 'visitors_fi.created_at', dateRange: ['2017-01-06', '2017-01-07'] }],
      })).toEqual([{ vfi__amount_mtd: '1500' }]);
    });

    it('a to_date window over a custom period reads from the start of that period', async () => {
      // Half years start on Jan 1 and Jul 1, so Jan 6 - 7 reads Jan 1 - 7.
      expect(await evaluate(['visitors_fi.amount_htd'], {
        timeDimensions: [{ dimension: 'visitors_fi.created_at', dateRange: ['2017-01-06', '2017-01-07'] }],
      })).toEqual([{ vfi__amount_htd: '1500' }]);
    });

    it('with a granularity every row keeps its own window', async () => {
      expect(await evaluate(['visitors_fi.amount_r3_on', 'visitors_fi.amount_mtd_on'], {
        timeDimensions: [{ dimension: 'visitors_fi.created_at', granularity: 'day', dateRange: ['2017-01-05', '2017-01-07'] }],
      })).toEqual([
        { vfi__created_at_day: '2017-01-05T00:00:00.000Z', vfi__amount_r3_on: '300', vfi__amount_mtd_on: '300' },
        { vfi__created_at_day: '2017-01-06T00:00:00.000Z', vfi__amount_r3_on: '500', vfi__amount_mtd_on: '600' },
        { vfi__created_at_day: '2017-01-07T00:00:00.000Z', vfi__amount_r3_on: '1400', vfi__amount_mtd_on: '1500' },
      ]);
    });
  } else {
    it.skip('multi-stage measures need Tesseract', () => {
      // Multi-stage measures are planned by Tesseract only.
    });
  }
});
