import { getEnv } from '@cubejs-backend/shared';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// A multi-stage measure that declares both `grain` and `rolling_window`.
// The grain says at what grain the value inside a bucket is computed; the
// window says which rows land in the bucket. Both have to hold at once.
//
// Inline data: two equally weighted rows per day, so the weighted daily
// factor equals the row factor — 1.10, 1.20, 0.50. Linked over the three
// days that is exactly -0.34, and every window below is wide enough to
// cover all three, so at any grain coarser than a day the windowed measure
// has to agree with the plain one.
//
// The measures round to six digits so the assertions compare exact strings
// rather than IEEE noise from EXP/LN.
describe('Multi-Stage grain with rolling window', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: returns
    sql: >
      SELECT '2024-01-01'::date as DAY, 'A' as SECURITY, 10.0 as IRR, 100.0 as WEIGHT
      union all
      SELECT '2024-01-01'::date, 'B', 10.0, 100.0
      union all
      SELECT '2024-01-02'::date, 'A', 20.0, 100.0
      union all
      SELECT '2024-01-02'::date, 'B', 20.0, 100.0
      union all
      SELECT '2024-01-03'::date, 'A', -50.0, 100.0
      union all
      SELECT '2024-01-03'::date, 'B', -50.0, 100.0

    dimensions:
      - name: day
        sql: DAY
        type: time

      - name: security
        sql: SECURITY
        type: string

      - name: irr
        sql: IRR
        type: number

      - name: weight
        sql: WEIGHT
        type: number

    measures:
      - name: weight_sum
        sql: "{CUBE.weight}"
        type: sum

      - name: irr_weight_sum
        sql: "{CUBE.irr} * {CUBE.weight}"
        type: sum

      - name: daily_irr_weight
        multi_stage: true
        sql: "{CUBE.irr_weight_sum}"
        type: number

      - name: daily_weight
        multi_stage: true
        sql: "{CUBE.weight_sum}"
        type: number

      - name: log_return_sum
        multi_stage: true
        sql: "LN(1.0 + ({CUBE.daily_irr_weight} / NULLIF({CUBE.daily_weight}, 0)) / 100.0)"
        type: sum
        grain:
          include:
            - returns.day

      - name: twr
        multi_stage: true
        sql: "ROUND((EXP({CUBE.log_return_sum}) - 1)::numeric, 6)"
        type: number

      - name: log_return_sum_ytd
        multi_stage: true
        sql: "LN(1.0 + ({CUBE.daily_irr_weight} / NULLIF({CUBE.daily_weight}, 0)) / 100.0)"
        type: sum
        grain:
          include:
            - returns.day
        rolling_window:
          type: to_date
          granularity: year

      - name: twr_ytd
        multi_stage: true
        sql: "ROUND((EXP({CUBE.log_return_sum_ytd}) - 1)::numeric, 6)"
        type: number

      - name: log_return_sum_1y
        multi_stage: true
        sql: "LN(1.0 + ({CUBE.daily_irr_weight} / NULLIF({CUBE.daily_weight}, 0)) / 100.0)"
        type: sum
        grain:
          include:
            - returns.day
        rolling_window:
          trailing: 1 year
          offset: end

      - name: twr_1y
        multi_stage: true
        sql: "ROUND((EXP({CUBE.log_return_sum_1y}) - 1)::numeric, 6)"
        type: number
    `);

  if (getEnv('nativeSqlPlanner')) {
    it('day granularity: the window accumulates over the declared grain', async () => dbRunner.runQueryTest({
      measures: ['returns.twr', 'returns.twr_ytd', 'returns.twr_1y'],
      timeDimensions: [{
        dimension: 'returns.day',
        granularity: 'day',
        dateRange: ['2024-01-01', '2024-01-03'],
      }],
      order: [{ id: 'returns.day' }],
      timezone: 'UTC',
    }, [
      // Per-day factors 1.10 / 1.20 / 0.50; the windows link them up.
      { returns__day_day: '2024-01-01T00:00:00.000Z', returns__twr: '0.100000', returns__twr_ytd: '0.100000', returns__twr_1y: '0.100000' },
      { returns__day_day: '2024-01-02T00:00:00.000Z', returns__twr: '0.200000', returns__twr_ytd: '0.320000', returns__twr_1y: '0.320000' },
      { returns__day_day: '2024-01-03T00:00:00.000Z', returns__twr: '-0.500000', returns__twr_ytd: '-0.340000', returns__twr_1y: '-0.340000' },
    ], { joinGraph, cubeEvaluator, compiler }));

    it('month granularity: query grain does not replace the declared grain', async () => dbRunner.runQueryTest({
      measures: ['returns.twr', 'returns.twr_ytd', 'returns.twr_1y'],
      timeDimensions: [{
        dimension: 'returns.day',
        granularity: 'month',
        dateRange: ['2024-01-01', '2024-01-31'],
      }],
      order: [{ id: 'returns.day' }],
      timezone: 'UTC',
    }, [
      { returns__day_month: '2024-01-01T00:00:00.000Z', returns__twr: '-0.340000', returns__twr_ytd: '-0.340000', returns__twr_1y: '-0.340000' },
    ], { joinGraph, cubeEvaluator, compiler }));

    it('no time dimension: the window is one bucket over the whole range', async () => dbRunner.runQueryTest({
      measures: ['returns.twr', 'returns.twr_ytd', 'returns.twr_1y'],
      timezone: 'UTC',
    }, [
      { returns__twr: '-0.340000', returns__twr_ytd: '-0.340000', returns__twr_1y: '-0.340000' },
    ], { joinGraph, cubeEvaluator, compiler }));

    it('non-time dimension: the grain survives a plain group by', async () => dbRunner.runQueryTest({
      measures: ['returns.twr', 'returns.twr_ytd', 'returns.twr_1y'],
      dimensions: ['returns.security'],
      order: [{ id: 'returns.security' }],
      timezone: 'UTC',
    }, [
      { returns__security: 'A', returns__twr: '-0.340000', returns__twr_ytd: '-0.340000', returns__twr_1y: '-0.340000' },
      { returns__security: 'B', returns__twr: '-0.340000', returns__twr_ytd: '-0.340000', returns__twr_1y: '-0.340000' },
    ], { joinGraph, cubeEvaluator, compiler }));
  } else {
    test.skip('day granularity: the window accumulates over the declared grain', () => { expect(1).toBe(1); });
    test.skip('month granularity: query grain does not replace the declared grain', () => { expect(1).toBe(1); });
    test.skip('no time dimension: the window is one bucket over the whole range', () => { expect(1).toBe(1); });
    test.skip('non-time dimension: the grain survives a plain group by', () => { expect(1).toBe(1); });
  }
});
