import { prepareYamlCompiler } from './PrepareCompiler';
import { MssqlQuery } from '../../src/adapter/MssqlQuery';

// https://github.com/cube-js/cube/issues/7758
// MSSQL truncates `week` with `dateadd(week, DATEDIFF(week, 0, x), 0)`. Day 0 (1900-01-01)
// is a Monday, but `DATEDIFF(week, ...)` counts *Sunday* boundaries (independent of
// DATEFIRST), so every Sunday is placed into the week starting on the *following* Monday:
// 2023-12-31 (Sun) lands in the 2024-01-01 bucket instead of 2023-12-25. Cube weeks start
// on Monday, so Sunday must stay in the week that started six days earlier.
// A correct form is e.g. `dateadd(day, DATEDIFF(day, 0, x) / 7 * 7, 0)`.
describe('Issue #7758: MSSQL week granularity shifts Sundays to the next week', () => {
  const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(`
cubes:
  - name: events
    sql_table: events
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: date
        sql: date
        type: time
    measures:
      - name: count
        type: count
`);

  beforeAll(async () => {
    await compiler.compile();
  });

  it('does not truncate weeks with DATEDIFF(week, ...), which counts Sunday boundaries', () => {
    const query = new MssqlQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['events.count'],
      timeDimensions: [{
        dimension: 'events.date',
        granularity: 'week',
        dateRange: ['2023-12-30', '2024-01-10'],
      }],
      timezone: 'UTC',
    });

    const [sql] = query.buildSqlAndParams();

    expect(sql).toMatch(/events__date_week/);
    expect(sql).not.toMatch(/DATEDIFF\(\s*week\s*,/i);
  });
});
