import { getEnv } from '@cubejs-backend/shared';
import { MssqlQuery } from '../../../src/adapter/MssqlQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './MSSqlDbRunner';

// MSSQL generates a rolling window's time series with a recursive CTE, which
// refers to itself by name, and needs a series with no points to stay valid.
describe('MSSQL multi-stage filter include with a date range', () => {
  jest.setTimeout(200000);

  // Daily amounts: Jan 3 = 100, Jan 5 = 200, Jan 6 = 300, Jan 7 = 900.
  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: visitors
    sql: "select * from ##visitors"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: created_at
        sql: created_at
        type: time
    measures:
      - name: amount
        sql: amount
        type: sum
      - name: amount_r3
        multi_stage: true
        type: sum
        sql: "{amount}"
        rolling_window:
          trailing: 3 day
      - name: amount_r3_on
        multi_stage: true
        type: number
        sql: "{amount_r3}"
        filter:
          include:
            - member: visitors.created_at
              operator: inDateRange
              values: ["2017-01-07", "2017-01-07"]
`);

  const evaluate = async (measures: string[], dateRange: [string, string]) => {
    await compiler.compile();
    const query = new MssqlQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures,
      timeDimensions: [{ dimension: 'visitors.created_at', granularity: 'day', dateRange }],
      timezone: 'UTC',
      order: [{ id: 'visitors.created_at' }],
    });
    const rows = await dbRunner.testQuery(query.buildSqlAndParams());
    return rows.map((row: Record<string, unknown>) => Object.fromEntries(
      Object.entries(row).map(([key, value]) => [key, value === null ? null : String(value)])
    ));
  };

  if (getEnv('nativeSqlPlanner')) {
    it('a window narrowed by the include reads its own series', async () => {
      expect(await evaluate(['visitors.amount_r3_on', 'visitors.amount_r3'], ['2017-01-05', '2017-01-07'])).toEqual([
        { visitors__created_at_day: '2017-01-05T00:00:00.000Z', visitors__amount_r3: '300', visitors__amount_r3_on: null },
        { visitors__created_at_day: '2017-01-06T00:00:00.000Z', visitors__amount_r3: '500', visitors__amount_r3_on: null },
        { visitors__created_at_day: '2017-01-07T00:00:00.000Z', visitors__amount_r3: '1400', visitors__amount_r3_on: '1400' },
      ]);
    });

    it('an include outside the query range has no rows', async () => {
      expect(await evaluate(['visitors.amount_r3_on'], ['2017-01-01', '2017-01-05'])).toEqual([]);
    });
  } else {
    it.skip('multi-stage measures need Tesseract', () => {
      // Multi-stage measures are planned by Tesseract only.
    });
  }
});
