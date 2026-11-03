import { getEnv } from '@cubejs-backend/shared';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// https://github.com/cube-js/cube/issues/11240
// A time_shift measure queried with a dimension of a joined date cube. The shifted
// CTE takes `day_name` from the unshifted fact date, and the final query aligns the
// current and shifted rows on `day_name` too. So each day splits into two rows: one
// with `total` and the current day name, one with `total_prior_day` and the previous
// day name. Expected: one row per day with the current day name and both measures.
describe('Issue 11240: time_shift measure with a dimension of a joined date cube', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: dim_date
    sql: >
      SELECT '2026-07-09'::date AS date, 'Thursday' AS day_name UNION ALL
      SELECT '2026-07-10'::date, 'Friday' UNION ALL
      SELECT '2026-07-11'::date, 'Saturday'
    public: false
    dimensions:
      - name: date
        sql: date
        type: time
        primary_key: true
      - name: day_name
        sql: day_name
        type: string

  - name: daily_facts
    sql: >
      SELECT '2026-07-09'::date AS date, 9 AS count UNION ALL
      SELECT '2026-07-10'::date, 10 UNION ALL
      SELECT '2026-07-11'::date, 11
    joins:
      - name: dim_date
        relationship: many_to_one
        sql: "{CUBE}.date = {dim_date}.date"
    dimensions:
      - name: date
        sql: date
        type: time
        primary_key: true
    measures:
      - name: total
        sql: count
        type: sum
      - name: total_prior_day
        multi_stage: true
        sql: "{total}"
        type: number
        time_shift:
          - interval: 1 day
            type: prior
`);

  if (getEnv('nativeSqlPlanner')) {
    it('one row per day with the current day name', async () => dbRunner.runQueryTest({
      measures: ['daily_facts.total', 'daily_facts.total_prior_day'],
      dimensions: ['dim_date.day_name'],
      timeDimensions: [{
        dimension: 'daily_facts.date',
        granularity: 'day',
        dateRange: ['2026-07-10', '2026-07-11'],
      }],
      order: [{ id: 'daily_facts.date' }],
      timezone: 'UTC',
    }, [
      {
        dim_date__day_name: 'Friday',
        daily_facts__date_day: '2026-07-10T00:00:00.000Z',
        daily_facts__total: '10',
        daily_facts__total_prior_day: '9',
      },
      {
        dim_date__day_name: 'Saturday',
        daily_facts__date_day: '2026-07-11T00:00:00.000Z',
        daily_facts__total: '11',
        daily_facts__total_prior_day: '10',
      },
    ], { joinGraph, cubeEvaluator, compiler }));
  } else {
    it.skip('Tesseract only', () => {
      // Tesseract only
    });
  }
});
