import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { ClickHouseQuery } from '../../../src/adapter/ClickHouseQuery';
import { ClickHouseDbRunner } from './ClickHouseDbRunner';

// https://github.com/cube-js/cube/issues/9051
// A `type: time` dimension over a DateTime64(3) / DateTime64(6) column loses its
// sub-second part: `convertTz` wraps the column in `toDateTime64(..., 0)`, and the
// filter parameters are parsed with `parseDateTimeBestEffort`, which yields a
// second-precision DateTime. So the values come back truncated to seconds, rows
// cannot be ordered by their milliseconds, and sub-second date ranges match the
// wrong rows.
describe('Issue 9051: ClickHouse time dimensions keep sub-second precision', () => {
  jest.setTimeout(200000);

  const dbRunner = new ClickHouseDbRunner();

  // The cube reads an inline SELECT, so no table has to be seeded
  const noDataSet = async () => Promise.resolve();

  afterAll(async () => {
    await dbRunner.tearDown();
  });

  const row = (id: number, ms: string, us: string) => `SELECT ${id} AS id, toDateTime64('2024-12-16 ${ms}', 3, 'UTC') AS created, toDateTime64('2024-12-16 ${us}', 6, 'UTC') AS created_us`;

  const compilers = prepareYamlCompiler(`
cubes:
  - name: logs
    sql: >
      ${row(1, '10:00:00.123', '10:00:00.123456')}
      UNION ALL ${row(2, '10:00:00.456', '10:00:00.456789')}
      UNION ALL ${row(3, '10:00:00.789', '10:00:00.789012')}
      UNION ALL ${row(4, '10:00:01.001', '10:00:01.001001')}
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: created
        sql: "{CUBE}.created"
        type: time
      - name: created_us
        sql: "{CUBE}.created_us"
        type: time
    measures:
      - name: count
        type: count
`);

  // `convertTzForRawTimeDimension` is what the API gateway passes for every REST query
  const query = (options: any) => new ClickHouseQuery(compilers, {
    timezone: 'UTC',
    convertTzForRawTimeDimension: true,
    ...options,
  });

  beforeAll(async () => {
    await compilers.compiler.compile();
  });

  it('returns the milliseconds of a time dimension without granularity', async () => {
    const [sql, params] = query({
      dimensions: ['logs.id', 'logs.created', 'logs.created_us'],
      order: [{ id: 'logs.created', desc: false }],
      ungrouped: true,
    }).buildSqlAndParams();

    const rows = await dbRunner.testQuery([sql, params], noDataSet);

    expect(rows.map(r => r.logs__created)).toEqual([
      '2024-12-16T10:00:00.123',
      '2024-12-16T10:00:00.456',
      '2024-12-16T10:00:00.789',
      '2024-12-16T10:00:01.001',
    ]);
    expect(rows.map(r => r.logs__created_us)).toEqual([
      '2024-12-16T10:00:00.123',
      '2024-12-16T10:00:00.456',
      '2024-12-16T10:00:00.789',
      '2024-12-16T10:00:01.001',
    ]);
  });

  it('groups by second granularity', async () => {
    const [sql, params] = query({
      measures: ['logs.count'],
      timeDimensions: [{ dimension: 'logs.created', granularity: 'second' }],
      order: [{ id: 'logs.created', desc: false }],
    }).buildSqlAndParams();

    const rows = await dbRunner.testQuery([sql, params], noDataSet);

    expect(rows.map(r => [r.logs__created_second, r.logs__count])).toEqual([
      ['2024-12-16T10:00:00.000', '3'],
      ['2024-12-16T10:00:01.000', '1'],
    ]);
  });

  it('matches only the rows inside a sub-second date range', async () => {
    const [sql, params] = query({
      dimensions: ['logs.id'],
      timeDimensions: [{
        dimension: 'logs.created',
        dateRange: ['2024-12-16T10:00:00.400', '2024-12-16T10:00:00.800'],
      }],
      order: [{ id: 'logs.id', desc: false }],
      ungrouped: true,
    }).buildSqlAndParams();

    const rows = await dbRunner.testQuery([sql, params], noDataSet);

    expect(rows.map(r => r.logs__id)).toEqual(['2', '3']);
  });

  it('compares an afterDate filter with milliseconds', async () => {
    const [sql, params] = query({
      dimensions: ['logs.id'],
      filters: [{ member: 'logs.created_us', operator: 'afterDate', values: ['2024-12-16T10:00:00.500'] }],
      order: [{ id: 'logs.id', desc: false }],
      ungrouped: true,
    }).buildSqlAndParams();

    const rows = await dbRunner.testQuery([sql, params], noDataSet);

    expect(rows.map(r => r.logs__id)).toEqual(['3', '4']);
  });
});
