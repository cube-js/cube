import { RedshiftQuery } from '../../src/adapter/RedshiftQuery';
import { prepareJsCompiler } from './PrepareCompiler';

// https://github.com/cube-js/cube/issues/11474
// A custom granularity with a negative `offset` makes RedshiftQuery.subtractInterval emit
// `DATEADD(day, --1, ...)`. Redshift reads `--` as a line comment, so the query fails with
// `syntax error at or near "FROM"`. Reproduced end to end on cubejs/cube v1.8.22.
describe('issue #11474: Redshift custom granularity with a negative offset', () => {
  const compilers = prepareJsCompiler(`
cube('orders', {
  sql: \`SELECT 1 AS id, DATE '2026-01-07' AS event_ts, 100 AS amount\`,
  dimensions: {
    id: { sql: 'id', type: 'number', primaryKey: true },
    event_ts: {
      sql: \`\${CUBE}.event_ts\`,
      type: 'time',
      granularities: {
        sunday_week: { interval: '1 week', offset: '-1 day' },
      },
    },
  },
  measures: {
    total: { sql: \`\${CUBE}.amount\`, type: 'sum' },
  },
});
`);

  it('does not emit a double minus (SQL line comment) for the negated offset', async () => {
    await compilers.compiler.compile();

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

    expect(sql).toContain('date_trunc(\'week\'');
    expect(sql).not.toMatch(/--\s*\d/);
  });

  it('negates a negative interval without producing `--`', () => {
    const query = new RedshiftQuery(compilers, { measures: [] });

    expect(query.subtractInterval('ts', '-1 day')).not.toContain('--');
  });
});
