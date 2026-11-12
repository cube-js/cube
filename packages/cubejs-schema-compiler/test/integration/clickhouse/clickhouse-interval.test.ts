import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { ClickHouseQuery } from '../../../src/adapter/ClickHouseQuery';
import { ClickHouseDbRunner } from './ClickHouseDbRunner';

// Only the SQL API reads this template, for `col + INTERVAL '1 day'`. The SQL API passes the
// parts of an interval as MILLISECOND, DAY and MONTH, see DecomposedDayTime and
// DecomposedMonthDayNano in cubesql.
describe('ClickHouse interval expression', () => {
  jest.setTimeout(200000);

  const dbRunner = new ClickHouseDbRunner();

  const noDataSet = async () => Promise.resolve();

  afterAll(async () => {
    await dbRunner.tearDown();
  });

  const compilers = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: "SELECT 1 AS id"
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
`);

  beforeAll(async () => {
    await compilers.compile();
  });

  it.each([
    ['DateTime plus days', 'toDateTime(\'2025-01-31 10:00:00\', \'UTC\')', 1, 'DAY', '2025-02-01T10:00:00.000'],
    ['DateTime plus a month at the end of a month', 'toDateTime(\'2025-01-31 10:00:00\', \'UTC\')', 1, 'MONTH', '2025-02-28T10:00:00.000'],
    ['DateTime plus milliseconds', 'toDateTime(\'2025-01-31 10:00:00\', \'UTC\')', 1, 'MILLISECOND', '2025-01-31T10:00:00.001'],
    ['DateTime64(3) plus days', 'toDateTime64(\'2025-01-31 10:00:00.007\', 3, \'UTC\')', 1, 'DAY', '2025-02-01T10:00:00.007'],
    ['Date plus days', 'toDate(\'2025-02-28\')', 1, 'DAY', '2025-03-01T00:00:00.000'],
    ['a negative number of days', 'toDateTime(\'2025-03-01 00:00:00\', \'UTC\')', -1, 'DAY', '2025-02-28T00:00:00.000'],
    ['a number of zero', 'toDateTime(\'2025-03-01 00:00:00\', \'UTC\')', 0, 'DAY', '2025-03-01T00:00:00.000'],
    ['DateTime plus days over a leap day', 'toDateTime(\'2024-02-29 00:00:00\', \'UTC\')', 1, 'DAY', '2024-03-01T00:00:00.000'],
    ['DateTime minus days back to a leap day', 'toDateTime(\'2024-03-01 00:00:00\', \'UTC\')', -1, 'DAY', '2024-02-29T00:00:00.000'],
    ['DateTime plus a month to a leap day', 'toDateTime(\'2024-01-31 10:00:00\', \'UTC\')', 1, 'MONTH', '2024-02-29T10:00:00.000'],
    ['DateTime plus days at the end of a year', 'toDateTime(\'2025-12-31 23:59:59\', \'UTC\')', 1, 'DAY', '2026-01-01T23:59:59.000'],
    ['a NULL time', 'CAST(NULL AS Nullable(DateTime))', 1, 'DAY', null],
  ])('%s', async (_name, value, num, datePart, expected) => {
    const template = compilers.createQuery(ClickHouseQuery, { timezone: 'UTC' })
      .sqlTemplates().expressions.interval_single_date_part;
    const interval = template
      .replace('{{ num }}', String(num))
      .replace('{{ date_part }}', datePart);

    const [row] = await dbRunner.testQuery([`SELECT ${value} + ${interval} AS v`, []], noDataSet);
    expect(row.v).toEqual(expected);
  });
});
