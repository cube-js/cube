import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';
import { testWithPreAggregation } from './pre-aggregation-utils';

// Repro for https://github.com/cube-js/cube/issues/7730
// A rolling-window `count_distinct` measure is non-additive: the distinct count over a
// 1-week window cannot be derived from per-day distinct counts stored in a rollup.
// Such a query must not be served from the `day` rollup (or must at least return the
// same values as the query against the source data). Tesseract matches the rollup and
// computes COUNT(DISTINCT <per-day distinct count>) over it, returning 1 for every day.
describe('Issue 7730: rolling count_distinct with a rollup pre-aggregation', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: session
    sql: >
      SELECT 1 AS id, '2024-01-01T00:00:00.000Z'::timestamptz AS created_at UNION ALL
      SELECT 2 AS id, '2024-01-02T00:00:00.000Z'::timestamptz AS created_at UNION ALL
      SELECT 3 AS id, '2024-01-03T00:00:00.000Z'::timestamptz AS created_at UNION ALL
      SELECT 1 AS id, '2024-01-09T00:00:00.000Z'::timestamptz AS created_at UNION ALL
      SELECT 4 AS id, '2024-01-10T00:00:00.000Z'::timestamptz AS created_at
    dimensions:
      - name: created_at
        sql: created_at
        type: time
    measures:
      - name: weekly_distinct
        sql: id
        type: count_distinct
        rolling_window:
          leading: 1 week
          offset: start
    pre_aggregations:
      - name: main
        measures:
          - weekly_distinct
        time_dimension: created_at
        granularity: day
  `);

  // Distinct ids in [day, day + 1 week)
  const expected = [
    ['2024-01-01', '3'],
    ['2024-01-02', '2'],
    ['2024-01-03', '2'],
    ['2024-01-04', '2'],
    ['2024-01-05', '2'],
    ['2024-01-06', '2'],
    ['2024-01-07', '2'],
    ['2024-01-08', '2'],
    ['2024-01-09', '2'],
    ['2024-01-10', '1'],
  ].map(([day, value]) => ({
    session__created_at_day: `${day}T00:00:00.000Z`,
    session__weekly_distinct: value,
  }));

  it('returns correct rolling distinct counts', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['session.weekly_distinct'],
      timeDimensions: [{
        dimension: 'session.created_at',
        granularity: 'day',
        dateRange: ['2024-01-01', '2024-01-10'],
      }],
      timezone: 'UTC',
      preAggregationsSchema: '',
    });

    const descriptions: any[] = query.preAggregations?.preAggregationsDescription() || [];
    const desc = descriptions.find((d) => d.preAggregationId === 'session.main');

    const res = desc
      ? await testWithPreAggregation(desc, query)
      : await dbRunner.testQuery(query.buildSqlAndParams());

    expect(res).toEqual(expected);
  });
});
