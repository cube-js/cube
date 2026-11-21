import { prepareYamlCompiler } from './PrepareCompiler';
import { RedshiftQuery } from '../../src/adapter/RedshiftQuery';

// https://github.com/cube-js/cube/issues/11474
describe('RedshiftQuery', () => {
  it('subtractInterval with a negative interval does not emit a SQL line comment', () => {
    const query = new RedshiftQuery(prepareYamlCompiler('cubes: []'), { measures: [] });

    const sql = query.subtractInterval('t.ts', '-1 day');

    expect(sql).not.toContain('--');
    expect(sql).toEqual('DATEADD(day, 1, t.ts)');
  });

  describe('custom granularity with a negative offset', () => {
    const compilers = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: SELECT 1 AS id, DATE '2026-01-07' AS event_ts, 100 AS amount
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
    measures:
      - name: total
        type: sum
        sql: "{CUBE}.amount"
`);

    beforeAll(async () => {
      await compilers.compiler.compile();
    });

    it('renders valid DATEADD for the inverted offset', () => {
      const query = new RedshiftQuery(compilers, {
        measures: ['orders.total'],
        timeDimensions: [{
          dimension: 'orders.event_ts',
          granularity: 'sunday_week',
          dateRange: ['2026-01-01', '2026-02-01'],
        }],
        timezone: 'UTC',
      });

      const [sql] = query.buildSqlAndParams();

      expect(sql).not.toContain('--');
      expect(sql).toContain('DATEADD(day, 1, ');
    });
  });
});
