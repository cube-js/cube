import { prepareYamlCompiler } from './PrepareCompiler';
import { PostgresQuery } from '../../src/adapter/PostgresQuery';

// https://github.com/cube-js/cube/issues/7730
// A rolling-window `count_distinct` measure with a day-granularity rollup.
// The rollup stores COUNT(DISTINCT id) per day, which can't be combined into a
// distinct count over a 1-week window. Tesseract nevertheless matches the rollup and
// computes COUNT(DISTINCT <daily distinct count>) over it, returning wrong results.
// The legacy planner correctly falls back to the source table.
describe('Issue #7730: rolling window count_distinct pre-aggregation', () => {
  const schema = `
cubes:
  - name: session
    sql: SELECT 1 AS id, '2024-01-01T00:00:00.000Z'::timestamp AS created_at
    dimensions:
      - name: created_at
        sql: "{CUBE}.created_at"
        type: time
    measures:
      - name: weekly_count
        sql: id
        type: count_distinct
        rolling_window:
          leading: 1 week
          offset: start
    pre_aggregations:
      - name: main
        measures:
          - session.weekly_count
        time_dimension: session.created_at
        granularity: day
`;

  it('does not compute a rolling distinct count from per-day distinct counts', async () => {
    const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(schema);
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['session.weekly_count'],
      timeDimensions: [{
        dimension: 'session.created_at',
        granularity: 'day',
        dateRange: ['2024-01-01', '2024-01-31'],
      }],
      timezone: 'UTC',
    });

    const [sql] = query.buildSqlAndParams();
    // The day rollup holds COUNT(DISTINCT id) per day; a distinct count over a week
    // can't be derived from it, so the query must not read from this rollup.
    expect(sql).not.toContain('stb_pre_aggregations.session_main');
  });
});
