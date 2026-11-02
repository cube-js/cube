import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { ClickHouseQuery } from '../../../src/adapter/ClickHouseQuery';
import { ClickHouseDbRunner } from './ClickHouseDbRunner';

// Time filters arrive with ISO-8601 values ('2025-06-30T00:00:00.000Z'), as the SQL API sends for
// a Power BI date selection, and ClickHouse rejects that form where it converts the value itself.
describe('ClickHouse time dimension value filters', () => {
  jest.setTimeout(200000);

  const dbRunner = new ClickHouseDbRunner();

  // The data lives in the cube's own SQL
  const noDataSet = async () => Promise.resolve();

  afterAll(async () => {
    await dbRunner.tearDown();
  });

  const compilers = prepareYamlCompiler(`
cubes:
  - name: days
    sql: >
      SELECT 1 AS id, toDate('2025-06-30') AS day, toDateTime64('2025-06-30 00:00:00.123', 3, 'UTC') AS at
      UNION ALL
      SELECT 2 AS id, toDate('2025-09-30') AS day, toDateTime64('2025-09-30 00:00:00.000', 3, 'UTC') AS at
      UNION ALL
      SELECT 3 AS id, toDate('2025-12-31') AS day, toDateTime64('2025-12-31 00:00:00.000', 3, 'UTC') AS at
      UNION ALL
      SELECT 4 AS id, toDate('2026-03-31') AS day, toDateTime64('2026-03-31 00:00:00.000', 3, 'UTC') AS at
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
        public: true
      - name: day
        sql: "{CUBE}.day"
        type: time
      - name: day_time
        sql: "toDateTime({CUBE}.day)"
        type: time
      - name: at
        sql: "{CUBE}.at"
        type: time
      - name: day_or_null
        sql: "if({CUBE}.id = 4, NULL, {CUBE}.day)"
        type: time
      - name: at_micros
        sql: "toDateTime64({CUBE}.at, 6, 'UTC') + toIntervalMicrosecond(456)"
        type: time
      - name: at_new_york
        sql: "toTimeZone({CUBE}.at, 'America/New_York')"
        type: time
      - name: day_text
        sql: "toString({CUBE}.day)"
        type: time
        field_type: string
    measures:
      - name: count
        type: count
`);

  beforeAll(async () => {
    await compilers.compiler.compile();
  });

  type Filter = { member: string, operator: string, values: (string | null)[] } | { or: Filter[] };

  const idsFor = async (filter: Filter) => {
    const query = new ClickHouseQuery(compilers, {
      dimensions: ['days.id'],
      filters: [filter],
      order: [{ id: 'days.id' }],
      timezone: 'UTC',
    });
    const rows = await dbRunner.testQuery(query.buildSqlAndParams(), noDataSet);
    return rows.map((r: any) => Number(r.days__id));
  };

  it.each(['days.day', 'days.day_time'])('keeps only the selected days of %s, not the range between them', async (member) => {
    expect(await idsFor({
      member,
      operator: 'equals',
      // Three values, as ClickHouse rewrites an OR chain of three or more equalities into IN
      values: ['2025-06-30T00:00:00.000Z', '2025-12-31T00:00:00.000Z', '2026-03-31T00:00:00.000Z'],
    })).toEqual([1, 3, 4]);
  });

  it.each(['days.day', 'days.day_time'])('excludes the selected days of %s', async (member) => {
    expect(await idsFor({
      member,
      operator: 'notEquals',
      values: ['2025-06-30T00:00:00.000Z', '2025-12-31T00:00:00.000Z', '2026-03-31T00:00:00.000Z'],
    })).toEqual([2]);
  });

  it.each(['days.day', 'days.day_time', 'days.at'])('selects and excludes a single day of %s', async (member) => {
    const values = member === 'days.at' ? ['2025-06-30T00:00:00.123Z'] : ['2025-06-30T00:00:00.000Z'];

    expect(await idsFor({ member, operator: 'equals', values })).toEqual([1]);
    expect(await idsFor({ member, operator: 'notEquals', values })).toEqual([2, 3, 4]);
  });

  it.each(['days.day', 'days.day_time'])('keeps the days of an OR group of single equalities on %s', async (member) => {
    // Three or more, as ClickHouse rewrites an OR chain of equalities into IN
    expect(await idsFor({
      or: ['2025-06-30T00:00:00.000Z', '2025-12-31T00:00:00.000Z', '2026-03-31T00:00:00.000Z']
        .map(value => ({ member, operator: 'equals', values: [value] })),
    })).toEqual([1, 3, 4]);
  });

  it('compares a time member with a value', async () => {
    const values = ['2025-09-30T00:00:00.000Z'];

    expect(await idsFor({ member: 'days.day', operator: 'gt', values })).toEqual([3, 4]);
    expect(await idsFor({ member: 'days.day', operator: 'lte', values })).toEqual([1, 2]);
  });

  it('selects and excludes the NULL among the values of a nullable member', async () => {
    const values = ['2025-06-30T00:00:00.000Z', '2025-12-31T00:00:00.000Z', null];

    expect(await idsFor({ member: 'days.day_or_null', operator: 'equals', values })).toEqual([1, 3, 4]);
    expect(await idsFor({ member: 'days.day_or_null', operator: 'notEquals', values })).toEqual([2]);
  });

  it('compares a DateTime64 member with the milliseconds of each value', async () => {
    expect(await idsFor({
      member: 'days.at',
      operator: 'equals',
      values: ['2025-06-30T00:00:00.123Z', '2025-12-31T00:00:00.000Z'],
    })).toEqual([1, 3]);

    // The same day without its milliseconds is a different instant
    expect(await idsFor({
      member: 'days.at',
      operator: 'equals',
      values: ['2025-06-30T00:00:00.000Z', '2025-09-30T00:00:00.000Z'],
    })).toEqual([2]);
  });

  it('compares a DateTime64(6) member with the microseconds of each value', async () => {
    expect(await idsFor({
      member: 'days.at_micros',
      operator: 'equals',
      values: ['2025-06-30T00:00:00.123456Z', '2025-09-30T00:00:00.000456Z'],
    })).toEqual([1, 2]);

    // The same instant cut to milliseconds is a different value
    expect(await idsFor({
      member: 'days.at_micros',
      operator: 'equals',
      values: ['2025-06-30T00:00:00.123Z'],
    })).toEqual([]);
  });

  it('reads a value without an offset as UTC, whatever the zone of the member', async () => {
    expect(await idsFor({
      member: 'days.at_new_york',
      operator: 'equals',
      values: ['2025-09-30 00:00:00'],
    })).toEqual([2]);
  });

  it('honours the offset of a value', async () => {
    expect(await idsFor({
      member: 'days.day_time',
      operator: 'equals',
      values: ['2025-06-30T02:00:00.000+02:00', '2025-09-30T00:00:00.000Z'],
    })).toEqual([1, 2]);
  });

  it('keeps the values of a field_type string member as text', async () => {
    const values = ['2025-06-30', '2025-12-31'];

    expect(await idsFor({ member: 'days.day_text', operator: 'equals', values: [values[0]] })).toEqual([1]);
    expect(await idsFor({ member: 'days.day_text', operator: 'equals', values })).toEqual([1, 3]);
    expect(await idsFor({ member: 'days.day_text', operator: 'notEquals', values })).toEqual([2, 4]);
  });
});

// The SQL API turns `col > X` into a date range that starts one millisecond after X
describe('ClickHouse date range bounds', () => {
  jest.setTimeout(200000);

  const dbRunner = new ClickHouseDbRunner();

  const noDataSet = async () => Promise.resolve();

  afterAll(async () => {
    await dbRunner.tearDown();
  });

  const row = (id: number, at: string, nullable: string) => `SELECT ${id} AS id, toDateTime('${at}', 'UTC') AS at,
        toDateTime64('${at}', 3, 'UTC') AS at_millis,
        toDateTime64('${at}', 3, 'UTC') + toIntervalMillisecond(999) AS at_millis_last, toDate('${at.substring(0, 10)}') AS day,
        ${nullable} AS at_or_null`;

  const compilers = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: >
      ${row(1, '2025-03-15 05:00:00', "toNullable(toDateTime('2025-03-15 05:00:00', 'UTC'))")}
      UNION ALL
      ${row(2, '2025-03-15 05:00:01', "toNullable(toDateTime('2025-03-15 05:00:01', 'UTC'))")}
      UNION ALL
      ${row(3, '2025-03-16 05:00:00', "toNullable(toDateTime('2025-03-16 05:00:00', 'UTC'))")}
      UNION ALL
      ${row(4, '2025-03-16 05:00:01', 'NULL')}
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
        public: true
      - name: at
        sql: "{CUBE}.at"
        type: time
      - name: at_millis
        sql: "{CUBE}.at_millis"
        type: time
      - name: at_millis_last
        sql: "{CUBE}.at_millis_last"
        type: time
      - name: day
        sql: "{CUBE}.day"
        type: time
      - name: at_or_null
        sql: "{CUBE}.at_or_null"
        type: time
    measures:
      - name: count
        type: count
`);

  beforeAll(async () => {
    await compilers.compiler.compile();
  });

  it.each([
    ['DateTime, a lower bound one millisecond after a row', 'at', '2025-03-15T05:00:00.001', '2025-03-16T05:00:00.000', 'UTC', [2, 3]],
    ['DateTime, bounds on rows', 'at', '2025-03-15T05:00:00.000', '2025-03-16T05:00:00.000', 'UTC', [1, 2, 3]],
    ['DateTime, an upper bound one millisecond before a row', 'at', '2025-03-15T05:00:00.000', '2025-03-16T04:59:59.999', 'UTC', [1, 2]],
    ['DateTime, an upper bound one millisecond after a row', 'at', '2025-03-15T05:00:00.000', '2025-03-16T05:00:00.001', 'UTC', [1, 2, 3]],
    ['DateTime, a lower bound one millisecond after a row in the query time zone', 'at', '2025-03-15T01:00:00.001', '2025-03-16T01:00:00.000', 'America/New_York', [2, 3]],
    ['DateTime64(3), a lower bound one millisecond after a row', 'at_millis', '2025-03-15T05:00:00.001', '2025-03-16T05:00:00.000', 'UTC', [2, 3]],
    ['Date, whole days', 'day', '2025-03-15T00:00:00.000', '2025-03-15T23:59:59.999', 'UTC', [1, 2]],
    ['Nullable(DateTime), a NULL is outside of each range', 'at_or_null', '2025-03-15T05:00:00.001', '2025-03-17T00:00:00.000', 'UTC', [2, 3]],
    ['DateTime, a fraction inside the second of a row', 'at', '2025-03-15T05:00:00.501', '2025-03-16T05:00:00.000', 'UTC', [2, 3]],
    ['DateTime, a lower bound one millisecond after the upper bound', 'at', '2025-03-15T05:00:00.001', '2025-03-15T05:00:00.000', 'UTC', []],
    ['Date, a lower bound one millisecond after a day', 'day', '2025-03-15T00:00:00.001', '2025-03-16T00:00:00.000', 'UTC', [3, 4]],
    ['DateTime64(3), an upper bound on the last millisecond of a second', 'at_millis_last', '2025-03-15T05:00:00.000', '2025-03-15T05:00:00.999', 'UTC', [1]],
    ['DateTime64(3), an upper bound one millisecond before a row', 'at_millis_last', '2025-03-15T05:00:00.000', '2025-03-15T05:00:00.998', 'UTC', []],
  ])('%s', async (_name, member, from, to, timezone, expected) => {
    const query = new ClickHouseQuery(compilers, {
      dimensions: ['orders.id'],
      timeDimensions: [{ dimension: `orders.${member}`, dateRange: [from, to] }],
      order: [{ id: 'orders.id' }],
      timezone,
    });
    const rows = await dbRunner.testQuery(query.buildSqlAndParams(), noDataSet);
    expect(rows.map((r: any) => Number(r.orders__id))).toEqual(expected);
  });
});
