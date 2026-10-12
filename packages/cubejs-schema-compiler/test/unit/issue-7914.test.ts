import { prepareYamlCompiler } from './PrepareCompiler';
import { BigqueryQuery } from '../../src/adapter/BigqueryQuery';

// https://github.com/cube-js/cube/issues/7914
// A `type: number` measure whose SQL is itself an aggregate (BigQuery
// `APPROX_QUANTILES(...)[OFFSET(50)]`) with a `rolling_window` and a granularity produces
// invalid SQL: the subquery that computes the aggregate has no GROUP BY even though it also
// selects the (non-aggregated) time dimension, and the outer GROUP BY query selects the
// measure without aggregating it. BigQuery rejects it with "SELECT list expression
// references ... which is neither grouped nor aggregated". Same with both planners.
describe('Issue #7914: aggregate number measure with rolling window and granularity', () => {
  const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(`
cubes:
  - name: snaps
    sql_table: ds.snaps
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: salary
        sql: salary
        type: number
      - name: snapshot_date
        sql: snapshot_date
        type: time
    measures:
      - name: median_salary
        sql: "APPROX_QUANTILES({snaps.salary}, 100)[OFFSET(50)]"
        type: number
        rolling_window:
          trailing: 1 day
          offset: end
`);

  beforeAll(async () => {
    await compiler.compile();
  });

  it('groups the subquery that computes the aggregate by the time dimension', () => {
    const query = new BigqueryQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['snaps.median_salary'],
      timeDimensions: [{
        dimension: 'snaps.snapshot_date',
        granularity: 'week',
        dateRange: ['2024-01-01', '2024-01-21'],
      }],
      timezone: 'UTC',
    });

    const [sql] = query.buildSqlAndParams();

    // From the aggregate expression up to the point where its subquery/CTE is joined in,
    // there must be a GROUP BY: otherwise the time dimension selected next to it is
    // neither grouped nor aggregated.
    const aggStart = sql.indexOf('APPROX_QUANTILES');
    expect(aggStart).toBeGreaterThan(-1);
    const rest = sql.slice(aggStart);
    const joinedAt = rest.search(/AS `(rolling_source|snaps_median_salary_cumulative__base)`/);
    expect(joinedAt).toBeGreaterThan(-1);
    expect(rest.slice(0, joinedAt)).toMatch(/GROUP BY/);
  });
});
