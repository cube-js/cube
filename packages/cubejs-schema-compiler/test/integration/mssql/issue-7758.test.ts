/**
 * https://github.com/cube-js/cube/issues/7758
 *
 * Week granularity is wrong on MS SQL. Cube weeks start on Monday, but
 * MssqlQuery truncates with `dateadd(week, DATEDIFF(week, 0, date), 0)`.
 * `DATEDIFF(week, ...)` counts Sunday boundaries (it ignores DATEFIRST), so
 * every Sunday is put into the week of the *following* Monday. The labels
 * start on Monday, but each bucket really holds Sunday..Saturday.
 *
 * With one row per day from 2023-12-30 (Sat) to 2024-01-10 (Wed), Cube
 * returns 1 / 7 / 4 instead of 2 / 7 / 3 for the weeks of 2023-12-25,
 * 2024-01-01 and 2024-01-08.
 *
 * Reproduced end to end on Cube v1.7.49 with SQL Server 2022.
 */
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './MSSqlDbRunner';

describe('Issue #7758: MSSQL week granularity', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
  cubes:
    - name: days
      sql: >
        SELECT num AS id, DATEADD(day, num, CAST('2023-12-30' AS DATETIME2)) AS date
        FROM ##numbers
        WHERE num < 12

      measures:
        - name: count
          type: count

      dimensions:
        - name: id
          sql: id
          type: number
          primary_key: true

        - name: date
          sql: date
          type: time
  `);

  const normalize = (rows: any[], dateKey: string) => rows.map(row => ({
    week: new Date(row[dateKey]).toISOString().slice(0, 10),
    count: Number(row.days__count),
  }));

  const expected = [
    // Sat 2023-12-30, Sun 2023-12-31
    { week: '2023-12-25', count: 2 },
    // Mon 2024-01-01 .. Sun 2024-01-07
    { week: '2024-01-01', count: 7 },
    // Mon 2024-01-08 .. Wed 2024-01-10
    { week: '2024-01-08', count: 3 },
  ];

  const run = async (timeDimension: Record<string, any>) => {
    await compiler.compile();
    const query = dbRunner.newTestQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['days.count'],
      timeDimensions: [{ dimension: 'days.date', granularity: 'week', ...timeDimension }],
      order: [{ id: 'days.date' }],
      timezone: 'UTC',
    });
    return dbRunner.testQuery(query.buildSqlAndParams());
  };

  it('buckets Sundays into the week starting on the preceding Monday', async () => {
    const res = await run({});
    expect(normalize(res, 'days__date_week')).toEqual(expected);
  });

  it('buckets Sundays into the preceding Monday week with a date range', async () => {
    const res = await run({ dateRange: ['2023-12-30', '2024-01-10'] });
    expect(normalize(res, 'days__date_week')).toEqual(expected);
  });
});
