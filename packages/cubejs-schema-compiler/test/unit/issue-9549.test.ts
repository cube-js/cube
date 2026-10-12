import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// #9549: a multi-stage `time_shift` (prior, 1 year) queried at `week`
// granularity shifts the raw timestamp and then truncates
// (`date_trunc('week', date + interval '1 year')`), so each prior-year value is
// the sum over an arbitrary 7-day window straddling two prior-year weeks rather
// than the value of a prior-year week bucket. Exact for month/quarter/year, wrong
// for week. Executing the generated SQL in DuckDB on daily rows
// (amount = day index from 2023-12-01) gave prior-year values 182/231/280 for
// weeks 2024-12-23/2024-12-30/2025-01-06, while the actual prior-year weekly
// revenues are 147 (2023-12-18), 196 (2023-12-25), 245 (2024-01-01),
// 294 (2024-01-08): none match. The shift must be applied to the week bucket,
// not to the raw timestamp before truncation.
describe('Issue #9549 - weekly granularity with a 1 year time_shift', () => {
  const model = `
cubes:
  - name: sales
    sql_table: sales
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: date
        sql: date
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
`;

  it('does not truncate a year-shifted raw timestamp to a week', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(model);
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['sales.revenue', 'sales.revenue_prior_year'],
      timeDimensions: [{ dimension: 'sales.date', granularity: 'week', dateRange: ['2024-12-23', '2025-01-12'] }],
      timezone: 'UTC',
    });
    const [sql] = query.buildSqlAndParams();

    expect(sql).toContain('interval \'1 year\'');
    // Shifting by a year and then truncating to the week mixes two prior-year
    // weeks into each bucket.
    expect(sql).not.toMatch(/date_trunc\('week',\s*\(\("sales"\.date \+ interval '1 year'\)/);
  });
});
