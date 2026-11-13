import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// Repro for https://github.com/cube-js/cube/issues/9549
// A multi_stage `time_shift` of `1 year` queried with `week` granularity renders
// `date_trunc('week', date + interval '1 year')`, i.e. it shifts every raw row
// and then re-buckets. Because week grids of consecutive years do not line up,
// each "prior year" value is the sum of an arbitrary 7-day window (e.g. Sat..Fri)
// that is not any week of the prior year. The prior-year value of a week must be
// the revenue of an actual (full) week bucket of the prior year.
describe('Issue 9549: weekly granularity with 1 year time_shift', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: >
      SELECT d::timestamp AS created_at,
             (d::date - '2023-01-01'::date + 1) AS amount
      FROM generate_series('2023-01-01'::date, '2025-12-31'::date, interval '1 day') d

    dimensions:
      - name: date
        sql: created_at
        type: time

    measures:
      - name: revenue
        sql: amount
        type: sum

      - name: revenue_prior_year
        multi_stage: true
        sql: "{revenue}"
        type: number
        time_shift:
          - time_dimension: date
            interval: 1 year
            type: prior
  `);

  async function run(q: any): Promise<any[]> {
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, q);
    return dbRunner.testQuery(query.buildSqlAndParams());
  }

  it('prior-year value of a week equals the revenue of a real prior-year week', async () => {
    const priorYearWeeks = await run({
      measures: ['orders.revenue'],
      timeDimensions: [{
        dimension: 'orders.date',
        granularity: 'week',
        dateRange: ['2023-11-01', '2024-02-28'],
      }],
      timezone: 'UTC',
    });
    const weeklyRevenues = new Set(priorYearWeeks.map((r) => String(r.orders__revenue)));

    const res = await run({
      measures: ['orders.revenue', 'orders.revenue_prior_year'],
      timeDimensions: [{
        dimension: 'orders.date',
        granularity: 'week',
        dateRange: ['2024-12-23', '2025-01-12'],
      }],
      order: [['orders.date', 'asc']],
      timezone: 'UTC',
    });
    console.log(JSON.stringify(res));

    expect(res.length).toBe(3);

    for (const row of res) {
      // Currently fails: e.g. week 2024-12-23 gets 2023-12-23..2023-12-29 (Sat..Fri) = 2520,
      // which is not the revenue of any prior-year week bucket.
      expect(weeklyRevenues).toContain(String(row.orders__revenue_prior_year));
    }
  });
});
