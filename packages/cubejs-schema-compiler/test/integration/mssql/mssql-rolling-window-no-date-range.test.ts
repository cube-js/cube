import { MssqlQuery } from '../../../src/adapter/MssqlQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './MSSqlDbRunner';

// https://github.com/cube-js/cube/issues/12023
// A rolling window measure queried with a granularity but WITHOUT a dateRange
// must return the same values as with an explicit dateRange covering the data.
describe('MSSql rolling window with granularity and no dateRange', () => {
  jest.setTimeout(200000);

  // Production default: offset-based conversion, which accepts DATE columns.
  const prevNamedTz = process.env.CUBEJS_DB_MSSQL_USE_NAMED_TIMEZONES;
  beforeAll(() => {
    process.env.CUBEJS_DB_MSSQL_USE_NAMED_TIMEZONES = 'false';
  });
  afterAll(() => {
    process.env.CUBEJS_DB_MSSQL_USE_NAMED_TIMEZONES = prevNamedTz;
  });

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: Repro
    sql: >
      SELECT DATEADD(day, a.d + 10 * b.d, CAST('2026-01-01' AS date)) AS day,
             s.store AS store,
             CAST(100.25 * (a.d + 10 * b.d + 1) + s.k AS DECIMAL(18, 5)) AS amount
      FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS a(d)
      CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5)) AS b(d)
      CROSS JOIN (VALUES ('A', 1.5), ('B', 2.5), ('C', 3.5)) AS s(store, k)
    dimensions:
      - name: day
        sql: day
        type: time
    measures:
      - name: amount_7d
        sql: amount
        type: sum
        rolling_window:
          trailing: 7 day
          offset: end
`);

  // Day n (0-based from 2026-01-01) has 3 rows summing to 300.75 * (n + 1) + 7.5.
  const expectedRows = Array.from({ length: 60 }, (_, n) => {
    let sum = 0;
    for (let i = Math.max(0, n - 6); i <= n; i++) {
      sum += 300.75 * (i + 1) + 7.5;
    }
    return [new Date(Date.UTC(2026, 0, 1 + n)).toISOString().slice(0, 10), sum];
  });

  const runQuery = async (dateRange?: [string, string]) => {
    await compiler.compile();
    const query = new MssqlQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['Repro.amount_7d'],
        timeDimensions: [
          {
            dimension: 'Repro.day',
            granularity: 'day',
            ...(dateRange ? { dateRange } : {}),
          },
        ],
        order: [{ id: 'Repro.day', desc: false }],
        timezone: 'UTC',
      }
    );
    const res: any[] = await dbRunner.testQuery(query.buildSqlAndParams());
    return res.map(r => [
      new Date(r.repro__day_day).toISOString().slice(0, 10),
      Number(r.repro__amount_7d),
    ]);
  };

  it('with dateRange', async () => {
    expect(await runQuery(['2026-01-01', '2026-03-01'])).toEqual(expectedRows);
  });

  it('without dateRange', async () => {
    expect(await runQuery()).toEqual(expectedRows);
  });
});
