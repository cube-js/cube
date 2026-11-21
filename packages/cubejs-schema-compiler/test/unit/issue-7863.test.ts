// @link https://github.com/cube-js/cube/issues/7863
//
// A rollup with `count` measures that have a `rolling_window: { trailing: '3 week' }`
// and `granularity: week` should serve a query for those measures (or for calculated
// `number` measures over them) at `week` granularity. The original error from the issue
// ("Invalid identifier '#cube_repro__gte_count'") no longer happens, and the legacy
// planner does match the rollup. Tesseract, the default planner, doesn't: it maps a
// `week` rolling-window interval to `day` granularity
// (GranularityHelper::granularity_from_interval), so the rolling base query is grouped by
// day and the week rollup is skipped. The query falls back to the source database.
// The results are still correct; only the acceleration is lost.
// Reproduced end to end on Cube v1.7.49 (Postgres + Cube Store): `usedPreAggregations`
// is empty for `mov_avg_count` / `gte_mov_avg_pct` with Tesseract and contains
// `cube_repro_main` with CUBEJS_TESSERACT_SQL_PLANNER=false.
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from './PrepareCompiler';

describe('issue #7863: rolling window with week trailing interval and week rollup', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube('cube_repro', {
      sql: 'select * from cube_repro',

      dimensions: {
        id: { sql: 'id', type: 'number', primaryKey: true },
        value: { sql: 'value', type: 'number' },
        date: { sql: 'date', type: 'time' },
      },

      measures: {
        count: { type: 'count' },
        gte_count: {
          type: 'count',
          filters: [{ sql: \`\${CUBE}.value >= 175\` }],
        },
        cumulative_count: {
          type: 'count',
          rollingWindow: { trailing: 'unbounded' },
        },
        mov_avg_count: {
          type: 'count',
          rollingWindow: { trailing: '3 week' },
        },
        gte_mov_avg_count: {
          type: 'count',
          rollingWindow: { trailing: '3 week' },
          filters: [{ sql: \`\${CUBE}.value >= 175\` }],
        },
        gte_pct: {
          type: 'number',
          sql: \`100.0 * \${CUBE.gte_count} / \${CUBE.count}\`,
        },
        gte_mov_avg_pct: {
          type: 'number',
          sql: \`100.0 * \${CUBE.gte_mov_avg_count} / \${CUBE.mov_avg_count}\`,
        },
      },

      preAggregations: {
        main: {
          measures: [
            CUBE.count,
            CUBE.gte_count,
            CUBE.cumulative_count,
            CUBE.mov_avg_count,
            CUBE.gte_mov_avg_count,
          ],
          timeDimension: CUBE.date,
          granularity: 'week',
        },
      },
    });
  `);

  beforeAll(async () => {
    await compiler.compile();
  });

  const usedPreAggregations = (measures: string[], useNativeSqlPlanner: boolean) => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures,
      timeDimensions: [{
        dimension: 'cube_repro.date',
        granularity: 'week',
        dateRange: ['2023-06-05', '2023-07-30'],
      }],
      timezone: 'UTC',
      preAggregationsSchema: '',
      useNativeSqlPlanner,
    });
    query.buildSqlAndParams();
    return (query.preAggregations?.preAggregationsDescription() || [])
      .map((d: any) => d.preAggregationId);
  };

  // Control cases: these already match with Tesseract.
  it('matches the week rollup for a calculated measure over plain counts', () => {
    expect(usedPreAggregations(['cube_repro.gte_pct'], true)).toContain('cube_repro.main');
  });

  it('matches the week rollup for an unbounded rolling window', () => {
    expect(usedPreAggregations(['cube_repro.cumulative_count'], true)).toContain('cube_repro.main');
  });

  it('matches the week rollup for a 3 week trailing window with the legacy planner', () => {
    expect(usedPreAggregations(['cube_repro.mov_avg_count'], false)).toContain('cube_repro.main');
  });

  // Failing cases: Tesseract skips the week rollup for a `3 week` trailing window.
  it('matches the week rollup for a 3 week trailing window', () => {
    expect(usedPreAggregations(['cube_repro.mov_avg_count'], true)).toContain('cube_repro.main');
  });

  it('matches the week rollup for a calculated measure over 3 week trailing windows', () => {
    expect(usedPreAggregations([
      'cube_repro.gte_pct',
      'cube_repro.gte_mov_avg_pct',
    ], true)).toContain('cube_repro.main');
  });
});
