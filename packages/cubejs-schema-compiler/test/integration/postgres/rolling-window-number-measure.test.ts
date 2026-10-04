// Regression test for https://github.com/cube-js/cube/issues/7914
// A `type: number` measure whose SQL is itself an aggregate (median via
// PERCENTILE_CONT / APPROX_QUANTILES) combined with `rolling_window` and a
// time dimension granularity generates invalid SQL: the base CTE selects the
// aggregate next to the truncated time column without GROUP BY, and the
// rolling CTE groups by the series without aggregating the measure.
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Rolling window over a number-type measure (issue #7914)', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube(\`visitors\`, {
      sql: \`select * from visitors\`,

      measures: {
        medianAmount: {
          type: 'number',
          sql: \`PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY \${CUBE}.amount)\`,
          rollingWindow: { trailing: '3 day', offset: 'end' },
        },
      },

      dimensions: {
        id: {
          type: 'number',
          sql: 'id',
          primaryKey: true
        },
        createdAt: {
          type: 'time',
          sql: 'created_at'
        },
      },
    })
  `);

  it('rolling median by day', () => compiler.compile().then(async () => {
    const query = new PostgresQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['visitors.medianAmount'],
        timeDimensions: [
          {
            dimension: 'visitors.createdAt',
            granularity: 'day',
            dateRange: ['2017-01-05', '2017-01-07'],
          },
        ],
        order: [{ id: 'visitors.createdAt' }],
        timezone: 'UTC',
      }
    );

    // Seed visitors: 01-03:100, 01-05:200, 01-06:300, 01-07:400, 01-07:500
    // Trailing 3 days ending at each day:
    //   01-05 -> {100, 200}           -> 150
    //   01-06 -> {200, 300}           -> 250
    //   01-07 -> {200, 300, 400, 500} -> 350
    // Today this fails with: column "visitors.created_at" must appear in the
    // GROUP BY clause or be used in an aggregate function
    const res = await dbRunner.testQuery(query.buildSqlAndParams());
    expect(res).toEqual([
      { visitors__created_at_day: '2017-01-05T00:00:00.000Z', visitors__median_amount: 150 },
      { visitors__created_at_day: '2017-01-06T00:00:00.000Z', visitors__median_amount: 250 },
      { visitors__created_at_day: '2017-01-07T00:00:00.000Z', visitors__median_amount: 350 },
    ]);
  }));
});
