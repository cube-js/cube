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
