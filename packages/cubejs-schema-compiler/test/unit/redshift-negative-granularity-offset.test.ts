import { RedshiftQuery } from '../../src/adapter/RedshiftQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// https://github.com/cube-js/cube/issues/11474
// A custom granularity with a negative offset (the documented `sunday_week` recipe) made
// RedshiftQuery.subtractInterval render `DATEADD(day, --1, ...)`. `--` starts a line comment,
// so the rest of the select line is swallowed and the query fails with a syntax error.
describe('RedshiftQuery: negative offset on a custom granularity', () => {
  // language=YAML
  const model = `
cubes:
  - name: orders
    sql: >
      SELECT 1 AS id, DATE '2026-01-07' AS event_ts, 100 AS amount
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: event_ts
        type: time
        sql: "{CUBE}.event_ts"
        granularities:
          - name: sunday_week
            interval: 1 week
            offset: -1 day
          - name: fiscal_quarter
            interval: 1 quarter
            offset: -3 months
    measures:
      - name: total
        type: sum
        sql: "{CUBE}.amount"
`;

  it('subtractInterval never emits a double minus', () => {
    const q: any = Object.create(RedshiftQuery.prototype);

    expect(q.subtractInterval('x', '-1 day')).toBe('DATEADD(day, 1, x)');
    expect(q.subtractInterval('x', '1 day')).toBe('DATEADD(day, -1, x)');
  });

  it.each(['sunday_week', 'fiscal_quarter'])('renders valid SQL for %s', async (granularity) => {
    const compilers = prepareYamlCompiler(model);
    await compilers.compiler.compile();

    const query = new RedshiftQuery(compilers, {
      measures: ['orders.total'],
      timeDimensions: [{
        dimension: 'orders.event_ts',
        granularity,
        dateRange: ['2026-01-01', '2026-02-01'],
      }],
      timezone: 'UTC',
    });

    const [sql] = query.buildSqlAndParams();

    expect(sql).not.toMatch(/--/);
  });
});
