import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { ClickHouseQuery } from '../../../src/adapter/ClickHouseQuery';
import { ClickHouseDbRunner } from './ClickHouseDbRunner';

describe('ClickHouse time values with milliseconds', () => {
  jest.setTimeout(200000);

  const dbRunner = new ClickHouseDbRunner();

  const noDataSet = async () => Promise.resolve();

  afterAll(async () => {
    await dbRunner.tearDown();
  });

  const compilers = prepareYamlCompiler(`
cubes:
  - name: events
    sql: >
      SELECT 1 AS id, toDateTime64('2025-01-01 10:00:00.007', 3, 'UTC') AS at,
        toDateTime64('2025-01-01 10:00:00.007891', 6, 'UTC') AS at_micros,
        toDateTime('2025-01-01 10:00:00', 'UTC') AS at_seconds,
        toDate('2025-01-01') AS day,
        toNullable(toDateTime64('2025-01-01 10:00:00.007', 3, 'UTC')) AS at_or_null,
        '2025-01-01 10:00:00' AS at_text,
        1735725600 AS at_epoch
      UNION ALL
      SELECT 2 AS id, toDateTime64('2025-01-01 10:00:00.500', 3, 'UTC') AS at,
        toDateTime64('2025-01-01 10:00:00.500123', 6, 'UTC') AS at_micros,
        toDateTime('2025-01-01 10:00:01', 'UTC') AS at_seconds,
        toDate('2025-01-02') AS day,
        NULL AS at_or_null,
        '2025-01-01 10:00:01' AS at_text,
        1735725601 AS at_epoch
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
        public: true
      - name: at
        sql: "{CUBE}.at"
        type: time
      - name: at_micros
        sql: "{CUBE}.at_micros"
        type: time
      - name: at_seconds
        sql: "{CUBE}.at_seconds"
        type: time
      - name: day
        sql: "{CUBE}.day"
        type: time
      - name: at_or_null
        sql: "{CUBE}.at_or_null"
        type: time
      - name: at_text
        sql: "{CUBE}.at_text"
        type: time
      - name: at_epoch
        sql: "{CUBE}.at_epoch"
        type: time
    measures:
      - name: count
        type: count
`);

  beforeAll(async () => {
    await compilers.compiler.compile();
  });

  const query = (options: any) => new ClickHouseQuery(compilers, {
    timezone: 'UTC',
    // The API gateway passes this for each REST query
    convertTzForRawTimeDimension: true,
    ...options,
  });

  it.each([
    ['DateTime64(3)', 'at', ['2025-01-01T10:00:00.007', '2025-01-01T10:00:00.500']],
    ['DateTime64(6), cut to milliseconds', 'at_micros', ['2025-01-01T10:00:00.007', '2025-01-01T10:00:00.500']],
    ['DateTime', 'at_seconds', ['2025-01-01T10:00:00.000', '2025-01-01T10:00:01.000']],
    ['Date', 'day', ['2025-01-01T00:00:00.000', '2025-01-02T00:00:00.000']],
    ['Nullable(DateTime64(3))', 'at_or_null', ['2025-01-01T10:00:00.007', null]],
    ['String', 'at_text', ['2025-01-01T10:00:00.000', '2025-01-01T10:00:01.000']],
    ['UInt32, seconds of the epoch', 'at_epoch', ['2025-01-01T10:00:00.000', '2025-01-01T10:00:01.000']],
  ])('selects a time member of type %s', async (_name, member, expected) => {
    const rows = await dbRunner.testQuery(query({
      dimensions: ['events.id', `events.${member}`],
      order: [{ id: 'events.id' }],
      ungrouped: true,
    }).buildSqlAndParams(), noDataSet);

    expect(rows.map((r: any) => r[`events__${member}`])).toEqual(expected);
  });

  it('keeps a granularity of a second in whole seconds', async () => {
    const rows = await dbRunner.testQuery(query({
      measures: ['events.count'],
      timeDimensions: [{ dimension: 'events.at', granularity: 'second' }],
      order: [{ id: 'events.at' }],
    }).buildSqlAndParams(), noDataSet);

    expect(rows.map((r: any) => [r.events__at_second, Number(r.events__count)])).toEqual([
      ['2025-01-01T10:00:00.000', 2],
    ]);
  });

  it('reads the milliseconds in the query time zone', async () => {
    const rows = await dbRunner.testQuery(query({
      dimensions: ['events.id', 'events.at'],
      order: [{ id: 'events.id' }],
      ungrouped: true,
      timezone: 'America/New_York',
    }).buildSqlAndParams(), noDataSet);

    expect(rows.map((r: any) => r.events__at)).toEqual(['2025-01-01T05:00:00.007', '2025-01-01T05:00:00.500']);
  });

  it('orders rows inside one second by their milliseconds', async () => {
    const rows = await dbRunner.testQuery(query({
      dimensions: ['events.id', 'events.at'],
      order: [{ id: 'events.at', desc: true }],
      ungrouped: true,
    }).buildSqlAndParams(), noDataSet);

    expect(rows.map((r: any) => Number(r.events__id))).toEqual([2, 1]);
  });

  it('groups a raw time member by millisecond', async () => {
    const rows = await dbRunner.testQuery(query({
      dimensions: ['events.at'],
      measures: ['events.count'],
      order: [{ id: 'events.at' }],
    }).buildSqlAndParams(), noDataSet);

    expect(rows.map((r: any) => [r.events__at, Number(r.events__count)])).toEqual([
      ['2025-01-01T10:00:00.007', 1],
      ['2025-01-01T10:00:00.500', 1],
    ]);
  });
});
