// Reproduces https://github.com/cube-js/cube/issues/11240: a `time_shift` measure
// queried with a dimension of a joined cube. The shifted CTE joins that cube on the
// unshifted fact date, so its `day_name` belongs to the previous day, and the final
// join uses `day_name` to align the shifted and current rows. Each day comes back as
// two rows, one with `total` and one with `total_prior_day`.
import { getEnv } from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Multi-stage time_shift with a dimension of a joined cube', () => {
  jest.setTimeout(200000);

  const compilers = prepareYamlCompiler(`
cubes:
  - name: dim_date
    sql: >
      SELECT d::date AS date, trim(to_char(d, 'Day')) AS day_name
      FROM generate_series('2025-07-08'::date, '2025-07-12'::date, '1 day') d
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
      SELECT d::date AS date, extract(day from d)::int AS count
      FROM generate_series('2025-07-08'::date, '2025-07-12'::date, '1 day') d
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

  // Multi-stage measures are only supported by Tesseract.
  const testIfNative = getEnv('nativeSqlPlanner') ? it : it.skip;

  testIfNative('aligns shifted rows by the time dimension only', async () => {
    await compilers.compiler.compile();
    const query = new PostgresQuery(compilers, {
      measures: ['daily_facts.total', 'daily_facts.total_prior_day'],
      dimensions: ['dim_date.day_name'],
      timeDimensions: [{
        dimension: 'daily_facts.date',
        granularity: 'day',
        dateRange: ['2025-07-10', '2025-07-11'],
      }],
      order: [{ id: 'daily_facts.date' }],
      timezone: 'UTC',
    });

    const res = await dbRunner.testQuery(query.buildSqlAndParams());
    expect(res).toEqual([
      {
        dim_date__day_name: 'Thursday',
        daily_facts__date_day: '2025-07-10T00:00:00.000Z',
        daily_facts__total: '10',
        daily_facts__total_prior_day: '9',
      },
      {
        dim_date__day_name: 'Friday',
        daily_facts__date_day: '2025-07-11T00:00:00.000Z',
        daily_facts__total: '11',
        daily_facts__total_prior_day: '10',
      },
    ]);
  });
});
