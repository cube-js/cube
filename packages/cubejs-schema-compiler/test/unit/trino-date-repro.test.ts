import { TrinoQuery } from '../../src/adapter/TrinoQuery';
import { PrestodbQuery } from '../../src/adapter/PrestodbQuery';
import { prepareJsCompiler } from './PrepareCompiler';

const stubNativeInstance = {
  newJinjaEngine: () => ({
    loadTemplate: () => {},
    renderTemplate: () => Promise.resolve(''),
  }),
  loadPythonContext: () => Promise.resolve(null),
};

describe('Generated time dimension over a DATE column (Trino/Presto)', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube('Events', {
      sql: \`select 1 as id, DATE '2024-01-15' as d UNION ALL select 2 as id, DATE '2024-02-20' as d\`,
      measures: { count: { type: 'count' } },
      dimensions: {
        id: { sql: 'id', type: 'number', primaryKey: true },
        // exactly what ScaffoldingSchema.columnType() emits for a DATE column:
        // type: 'time' with no CAST in sql:
        d: { sql: 'd', type: 'time' },
      },
    });
  `, { nativeInstance: stubNativeInstance as any });

  it('Trino: does not apply AT TIME ZONE to a bare DATE-typed field without casting first', async () => {
    await compiler.compile();

    const query = new TrinoQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['Events.count'],
      timeDimensions: [{ dimension: 'Events.d', granularity: 'month' }],
      timezone: 'UTC',
    });

    const sql = query.buildSqlAndParams()[0];
    console.log(sql);

    // BUG: convertTz() applies `AT TIME ZONE` directly to the field before
    // any CAST to TIMESTAMP happens (`("events".d AT TIME ZONE 'UTC')`).
    // Trino rejects `AT TIME ZONE` on a DATE value with TYPE_MISMATCH, so
    // this generated SQL fails at query time whenever the underlying column
    // is DATE-typed (exactly what generated data models produce, since
    // ScaffoldingSchema maps any DATE column to `type: time`). A correct
    // fix casts to TIMESTAMP first, then applies AT TIME ZONE.
    expect(sql).not.toMatch(/\.d\s+AT TIME ZONE/);
  });

  it('Presto: does not cast to TIMESTAMP before applying AT TIME ZONE', async () => {
    await compiler.compile();

    const query = new PrestodbQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['Events.count'],
      timeDimensions: [{ dimension: 'Events.d', granularity: 'month' }],
      timezone: 'UTC',
    });

    const sql = query.buildSqlAndParams()[0];
    console.log(sql);

    // Same bug for Presto's convertTz(): `timezone_minute`/`timezone_hour`
    // are applied to `("events".d AT TIME ZONE 'UTC')` with no prior CAST.
    expect(sql).not.toMatch(/\.d\s+AT TIME ZONE/);
  });
});
