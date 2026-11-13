// https://github.com/cube-js/cube/issues/9495
//
// The reporter's rollup lists `cohort_date` at day, week, month, quarter and
// year granularities. A day query against it reads a coarser column.
import { BigqueryQuery } from '../../src/adapter/BigqueryQuery';
import { prepareJsCompiler } from './PrepareCompiler';

const model = (granularities: string[]) => `
cube('daily_analysis', {
  sql: \`SELECT * FROM ds.daily_analysis\`,
  measures: { count: { type: 'count' } },
  dimensions: {
    app_name: { sql: 'app_name', type: 'string' },
    cohort_date: { sql: 'cohort_date', type: 'time' },
  },
  preAggregations: {
    rollup_by_activity: {
      external: false,
      dimensions: [CUBE.app_name, CUBE.cohort_date],
      measures: [CUBE.count],
      timeDimensions: [
        ${granularities.map(g => `{ dimension: CUBE.cohort_date, granularity: '${g}' }`).join(',\n        ')}
      ],
    },
  },
});
`;

async function buildSql(granularities: string[], query: any, useNativeSqlPlanner: boolean) {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(model(granularities));
  await compiler.compile();
  const q = new BigqueryQuery({ joinGraph, cubeEvaluator, compiler }, {
    ...query,
    timezone: 'UTC',
    useNativeSqlPlanner,
  });
  const [sql] = q.buildSqlAndParams();
  // Make sure the query is actually served from the rollup.
  expect(sql).toContain('daily_analysis_rollup_by_activity');
  return sql;
}

describe('issue 9495: BigQuery rollup listing several granularities of one time dimension', () => {
  // The reporter's rollup lists the same time dimension with several
  // granularities. A day query must be served from the `_day` column, not
  // from a coarser one (which silently returns wrong data).
  it('uses the matching granularity column (Tesseract)', async () => {
    const sql = await buildSql(['day', 'week', 'month', 'quarter', 'year'], {
      measures: ['daily_analysis.count'],
      timeDimensions: [{ dimension: 'daily_analysis.cohort_date', granularity: 'day', dateRange: ['2025-04-05', '2025-04-05'] }],
    }, true);

    expect(sql).toContain('`daily_analysis__cohort_date_day`');
    expect(sql).not.toMatch(/daily_analysis__cohort_date_(week|month|quarter|year)/);
  });
});
