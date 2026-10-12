import { prepareJsCompiler } from './PrepareCompiler';
import { PostgresQuery } from '../../src/adapter/PostgresQuery';

// https://github.com/cube-js/cube/issues/7863
// Calculated (`type: number`) measures built on top of `rolling_window` count
// measures, with a week rollup holding every component measure. Querying the
// calculated measures should be served from the rollup.
//
// The original CubeStore "Invalid identifier" error no longer happens on the default
// (Tesseract) planner, but Tesseract does not match the rollup at all as soon as a
// `trailing: 3 week` measure is involved: a 3-week trailing window over week buckets
// is week-aligned, yet the window is treated as needing day granularity. The legacy
// planner matches the rollup.
describe('Issue #7863: calculated measures over rolling windows and a week rollup', () => {
  const { compiler, cubeEvaluator, joinGraph } = prepareJsCompiler(`
cube('cube_repro', {
  sql_table: 'cube_repro',
  dimensions: {
    id: { sql: 'id', type: 'number', primary_key: true },
    value: { sql: 'value', type: 'number' },
    date: { sql: 'date', type: 'time' },
    value_category: {
      type: 'string',
      sql: \`CASE WHEN \${CUBE.value} >= 175 THEN 'GteThreshold' ELSE 'LtThreshold' END\`,
    },
  },
  measures: {
    count: { type: 'count' },
    cumulative_count: { type: 'count', rolling_window: { trailing: 'unbounded' } },
    mov_avg_count: { type: 'count', rolling_window: { trailing: '3 week' } },
    gte_count: { type: 'count', filters: [{ sql: \`\${CUBE.value_category} = 'GteThreshold'\` }] },
    gte_cumulative_count: { type: 'count', rolling_window: { trailing: 'unbounded' }, filters: [{ sql: \`\${CUBE.value_category} = 'GteThreshold'\` }] },
    gte_mov_avg_count: { type: 'count', rolling_window: { trailing: '3 week' }, filters: [{ sql: \`\${CUBE.value_category} = 'GteThreshold'\` }] },
    gte_pct: { type: 'number', sql: \`100.0 * \${CUBE.gte_count} / \${CUBE.count}\` },
    gte_cumulative_pct: { type: 'number', sql: \`100.0 * \${CUBE.gte_cumulative_count} / \${CUBE.cumulative_count}\` },
    gte_mov_avg_pct: { type: 'number', sql: \`100.0 * \${CUBE.gte_mov_avg_count} / \${CUBE.mov_avg_count}\` },
  },
  pre_aggregations: {
    main: {
      measures: [
        cube_repro.gte_count, cube_repro.count,
        cube_repro.gte_cumulative_count, cube_repro.cumulative_count,
        cube_repro.gte_mov_avg_count, cube_repro.mov_avg_count,
      ],
      timeDimension: cube_repro.date,
      granularity: 'week',
    },
  },
});
`);

  it('serves the issue query (all three calculated measures) from the week rollup', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['cube_repro.gte_pct', 'cube_repro.gte_cumulative_pct', 'cube_repro.gte_mov_avg_pct'],
      timeDimensions: [{
        dimension: 'cube_repro.date',
        granularity: 'week',
        dateRange: ['2023-06-05', '2023-07-30T23:59:59.999'],
      }],
      order: [{ id: 'cube_repro.date' }],
      timezone: 'UTC',
    });

    const [sql] = query.buildSqlAndParams();
    expect(sql).toContain('stb_pre_aggregations.cube_repro_main');
  });
});
