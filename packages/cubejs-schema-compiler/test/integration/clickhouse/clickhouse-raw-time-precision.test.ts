import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { ClickHouseQuery } from '../../../src/adapter/ClickHouseQuery';
import { ClickHouseDbRunner } from './ClickHouseDbRunner';

// A raw time dimension is converted to the query timezone when convertTzForRawTimeDimension is set,
// as the SQL API sets it, and the conversion must keep the source column's sub-second part.
describe('ClickHouse raw time dimension precision', () => {
  jest.setTimeout(200000);

  const dbRunner = new ClickHouseDbRunner();

  // The data lives in the cube's own SQL
  const noDataSet = async () => Promise.resolve();

  afterAll(async () => {
    await dbRunner.tearDown();
  });

  const compilers = prepareYamlCompiler(`
cubes:
  - name: logs
    sql: >
      SELECT 1 AS id,
        toDateTime64('2025-06-30 10:00:00.123456', 6, 'UTC') AS at_micros,
        toDateTime64('2025-06-30 10:00:00.123', 3, 'UTC') AS at_millis,
        toDateTime('2025-06-30 10:00:01', 'UTC') AS at_seconds,
        toDate('2025-06-30') AS day
      UNION ALL
      SELECT 2 AS id,
        toDateTime64('2025-06-30 10:00:00.123455', 6, 'UTC') AS at_micros,
        toDateTime64('2025-06-30 10:00:00.987', 3, 'UTC') AS at_millis,
        toDateTime('2025-06-30 10:00:02', 'UTC') AS at_seconds,
        toDate('2025-07-01') AS day
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
        public: true
      - name: at_micros
        sql: "{CUBE}.at_micros"
        type: time
      - name: at_millis
        sql: "{CUBE}.at_millis"
        type: time
      - name: at_seconds
        sql: "{CUBE}.at_seconds"
        type: time
      - name: day
        sql: "{CUBE}.day"
        type: time
    measures:
      - name: count
        type: count
`);

  beforeAll(async () => {
    await compilers.compiler.compile();
  });

  const rawTimeQuery = (timezone: string, order: { id: string, desc?: boolean }[]) => new ClickHouseQuery(compilers, {
    dimensions: ['logs.id', 'logs.at_millis', 'logs.at_seconds', 'logs.day'],
    order,
    ungrouped: true,
    timezone,
    convertTzForRawTimeDimension: true,
  });

  it('keeps milliseconds and leaves DateTime and Date values intact (UTC)', async () => {
    const rows = await dbRunner.testQuery(rawTimeQuery('UTC', [{ id: 'logs.id' }]).buildSqlAndParams(), noDataSet);
    expect(rows).toEqual([
      {
        logs__id: '1',
        logs__at_millis: '2025-06-30T10:00:00.123',
        logs__at_seconds: '2025-06-30T10:00:01.000',
        logs__day: '2025-06-30T00:00:00.000',
      },
      {
        logs__id: '2',
        logs__at_millis: '2025-06-30T10:00:00.987',
        logs__at_seconds: '2025-06-30T10:00:02.000',
        logs__day: '2025-07-01T00:00:00.000',
      },
    ]);
  });

  it('keeps milliseconds when converting to another timezone', async () => {
    const rows = await dbRunner.testQuery(rawTimeQuery('America/New_York', [{ id: 'logs.id' }]).buildSqlAndParams(), noDataSet);
    expect(rows.map(r => [r.logs__at_millis, r.logs__at_seconds])).toEqual([
      ['2025-06-30T06:00:00.123', '2025-06-30T06:00:01.000'],
      ['2025-06-30T06:00:00.987', '2025-06-30T06:00:02.000'],
    ]);
  });

  it('orders by microseconds', async () => {
    const query = new ClickHouseQuery(compilers, {
      dimensions: ['logs.id', 'logs.at_micros'],
      order: [{ id: 'logs.at_micros' }, { id: 'logs.id' }],
      ungrouped: true,
      timezone: 'UTC',
      convertTzForRawTimeDimension: true,
    });
    const rows = await dbRunner.testQuery(query.buildSqlAndParams(), noDataSet);
    expect(rows.map(r => r.logs__id)).toEqual(['2', '1']);
  });
});
