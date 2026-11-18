import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// https://github.com/cube-js/cube/issues/12850
//
// On 2026-10-25 Europe/Berlin falls back from CEST (+02:00) to CET (+01:00), so
// local 02:00-02:59 happens twice and the local day is 25 hours long. The
// fixture has one row per UTC hour across that day. Each repeated local hour
// must stay a separate bucket (grouped) or a distinguishable value (ungrouped).
describe('Repeated DST hour (fall back)', () => {
  jest.setTimeout(200000);

  const compilers = prepareJsCompiler(`
    cube(\`dst_facts\`, {
      sql: \`
        select at, 1 as value
        from generate_series(timestamp '2026-10-24 22:00', timestamp '2026-10-25 22:00', interval '1 hour') as at
      \`,

      measures: {
        value: {
          type: 'sum',
          sql: 'value',
        },
      },

      dimensions: {
        at: {
          type: 'time',
          sql: 'at',
          primaryKey: true,
          shown: true,
        },
      },
    })
  `);

  const run = async (q: any) => {
    await compilers.compiler.compile();
    const query = compilers.createQuery(PostgresQuery, {
      timezone: 'Europe/Berlin',
      // Cube server default (CUBESQL_SQL_PUSH_DOWN=true): raw time dimensions
      // are converted to the query timezone too.
      convertTzForRawTimeDimension: true,
      ...q,
    });
    return dbRunner.testQuery(query.buildSqlAndParams());
  };

  it('hour granularity returns 25 buckets for the 25-hour day', async () => {
    const res = await run({
      measures: ['dst_facts.value'],
      timeDimensions: [{
        dimension: 'dst_facts.at',
        granularity: 'hour',
        dateRange: ['2026-10-25', '2026-10-25'],
      }],
      order: [{ id: 'dst_facts.at' }],
    });

    // Today: 24 rows, and the 02:00 row has value 2 (CEST and CET hours merged).
    expect(res.length).toBe(25);
    expect(res.every((r: any) => r.dst_facts__value === '1')).toBe(true);
  });

  it('time dimension without granularity keeps the repeated hour separate', async () => {
    const res = await run({
      measures: ['dst_facts.value'],
      dimensions: ['dst_facts.at'],
      timeDimensions: [{
        dimension: 'dst_facts.at',
        dateRange: ['2026-10-25T00:00:00.000', '2026-10-25T03:59:59.999'],
      }],
      order: [{ id: 'dst_facts.at' }],
    });

    // UTC 22:00Z, 23:00Z, 00:00Z, 01:00Z, 02:00Z -> five separate rows.
    expect(res.length).toBe(5);
    expect(res.every((r: any) => r.dst_facts__value === '1')).toBe(true);
  });

  it('ungrouped values for the repeated hour are distinguishable', async () => {
    const res = await run({
      dimensions: ['dst_facts.at'],
      ungrouped: true,
      timeDimensions: [{
        dimension: 'dst_facts.at',
        dateRange: ['2026-10-25T00:00:00.000', '2026-10-25T03:59:59.999'],
      }],
      order: [{ id: 'dst_facts.at' }],
    });

    expect(res.length).toBe(5);
    // Today: local 02:00 appears twice with no offset, so only 4 distinct values.
    expect(new Set(res.map((r: any) => r.dst_facts__at)).size).toBe(5);
  });
});
