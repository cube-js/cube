import { streamToArray } from '@cubejs-backend/shared';
import { DuckDBConnection, DuckDBInstance } from '@duckdb/node-api';
import { once } from 'events';
import { mkdtemp, rm } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';
import { Readable } from 'stream';
import { DuckDBDriver } from '../../src';

// Every type the legacy `duckdb` package rendered differently from @duckdb/node-api's defaults.
const TYPES_SQL = `SELECT
  ?::VARCHAR AS text, ?::BOOLEAN AS flag, ?::INTEGER AS missing, ?::BIGINT AS large_integer,
  ?::DOUBLE AS floating, ?::TIMESTAMP AS parameter_date, ?::BLOB AS binary, ?::BLOB AS bytes,
  12345678901234567890.123456789::DECIMAL(38,9) AS precise_decimal,
  -0.0012300::DECIMAL(18,7) AS fraction, 100::DECIMAL(18,3) AS whole, 0::DECIMAL(18,3) AS zero,
  100::DECIMAL(18,0) AS scale_zero,
  170141183460469231731687303715884105727::HUGEINT AS huge,
  DATE '2020-01-02' AS date,
  TIMESTAMP '2020-01-02 03:04:05.123456' AS timestamp,
  TIMESTAMP_S '2020-01-02 03:04:05' AS seconds,
  TIMESTAMP_MS '2020-01-02 03:04:05.123' AS millis,
  TIMESTAMP_NS '2020-01-02 03:04:05.123456789' AS nanos,
  TIMESTAMPTZ '2020-01-02 03:04:05.123456+02' AS zoned,
  TIMESTAMP '1960-01-02 03:04:05.123' AS before_epoch,
  TIME '03:04:05.123456' AS time,
  TIMETZ '03:04:05+02' AS time_zoned,
  INTERVAL '1 month 2 days 3 hours' AS interval,
  [1::BIGINT, 2::BIGINT] AS list,
  {a: 5::BIGINT, b: [TIME '01:02:03'], c: TIMESTAMP '2020-01-02 03:04:05.123',
   d: DATE '2020-01-02', e: 'ab'::BLOB} AS struct,
  MAP {'k': 1::BIGINT} AS map,
  UUID '550e8400-e29b-41d4-a716-446655440000' AS uuid`;

const TYPES_PARAMS = [
  "a'quoted string", false, null, 9007199254740993n, 1.25,
  new Date('2020-01-02T03:04:05.123Z'), Buffer.from([0, 255]), new Uint8Array([1, 128]),
];

const TYPES_EXPECTED = [{
  text: "a'quoted string",
  flag: false,
  missing: null,
  large_integer: '9007199254740993',
  floating: '1.25',
  parameter_date: '2020-01-02T03:04:05.123Z',
  binary: Buffer.from([0, 255]),
  bytes: Buffer.from([1, 128]),
  precise_decimal: '12345678901234567890.123456789',
  fraction: '-0.00123',
  whole: '100',
  zero: '0',
  scale_zero: '100',
  huge: '170141183460469231731687303715884105727',
  date: '2020-01-02T00:00:00.000Z',
  timestamp: '2020-01-02T03:04:05.123Z',
  seconds: '2020-01-02T03:04:05.000Z',
  millis: '2020-01-02T03:04:05.123Z',
  nanos: '2020-01-02T03:04:05.123Z',
  zoned: '2020-01-02T01:04:05.123Z',
  before_epoch: '1960-01-02T03:04:05.123Z',
  time: '03:04:05.123456',
  time_zoned: '03:04:05+02',
  interval: { months: 1, days: 2, micros: 10800000000 },
  list: ['1', '2'],
  struct: {
    a: '5',
    b: ['01:02:03'],
    c: '2020-01-02T03:04:05.123Z',
    d: '2020-01-02T00:00:00.000Z',
    e: Buffer.from('ab'),
  },
  map: [{ key: 'k', value: '1' }],
  uuid: '550e8400-e29b-41d4-a716-446655440000',
}];

describe('DuckDBDriver', () => {
  let driver: DuckDBDriver;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    driver = new DuckDBDriver({});
    await driver.query('CREATE SCHEMA IF NOT EXISTS test;', []);
    await driver.uploadTable(
      'test.select_test',
      [
        { name: 'id', type: 'bigint' },
        { name: 'created', type: 'timestamp' },
        { name: 'created_date', type: 'date' },
        { name: 'price', type: 'decimal' },
      ],
      {
        rows: [
          { id: 1, created: '2020-01-01 01:01:01.11111', created_date: '2020-01-01', price: '100' },
          { id: 2, created: '2020-02-02 02:02:02.22222', created_date: '2020-02-02', price: '200' },
          { id: 3, created: '2020-03-03 03:03:03.33333', created_date: '2020-03-03', price: '300' }
        ]
      }
    );
  });

  afterAll(async () => {
    await driver.release();
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  test('query', async () => {
    const result = await driver.query('select * from test.select_test ORDER BY id ASC', []);
    expect(result).toEqual([
      { id: '1', created: '2020-01-01T01:01:01.111Z', created_date: '2020-01-01T00:00:00.000Z', price: '100' },
      { id: '2', created: '2020-02-02T02:02:02.222Z', created_date: '2020-02-02T00:00:00.000Z', price: '200' },
      { id: '3', created: '2020-03-03T03:03:03.333Z', created_date: '2020-03-03T00:00:00.000Z', price: '300' }
    ]);
  });

  test('query with Date parameter', async () => {
    const result = await driver.query('SELECT ?::TIMESTAMP AS created', [new Date('2020-04-04T04:04:04.444Z')]);

    expect(result).toEqual([
      { created: '2020-04-04T04:04:04.444Z' }
    ]);
  });

  test('column types', async () => {
    expect(await driver.tableColumnTypes('test.select_test')).toEqual([
      {
        name: 'id',
        type: 'bigint',
      },
      {
        name: 'created',
        type: 'timestamp',
      },
      {
        name: 'created_date',
        type: 'timestamp',
      },
      {
        name: 'price',
        type: 'decimal(18,3)',
      }
    ]);
  });

  test('stream', async () => {
    const tableData = await driver.stream('select * from test.select_test ORDER BY id ASC', [], {
      highWaterMark: 1000,
    });

    expect(await tableData.types).toEqual(undefined);
    expect(await streamToArray(tableData.rowStream as any)).toEqual([
      { id: '1', created: '2020-01-01T01:01:01.111Z', created_date: '2020-01-01T00:00:00.000Z', price: '100' },
      { id: '2', created: '2020-02-02T02:02:02.222Z', created_date: '2020-02-02T00:00:00.000Z', price: '200' },
      { id: '3', created: '2020-03-03T03:03:03.333Z', created_date: '2020-03-03T00:00:00.000Z', price: '300' }
    ]);
    await tableData.release?.();
  });

  test('stream with Date parameter', async () => {
    const tableData = await driver.stream('SELECT ?::TIMESTAMP AS created', [new Date('2020-04-04T04:04:04.444Z')], {
      highWaterMark: 1000,
    });

    expect(await streamToArray(tableData.rowStream as any)).toEqual([
      { created: '2020-04-04T04:04:04.444Z' }
    ]);
    await tableData.release?.();
  });

  test('query keeps legacy value shapes and stays JSON-serializable', async () => {
    const result = await driver.query(TYPES_SQL, TYPES_PARAMS);

    expect(result).toEqual(TYPES_EXPECTED);
    expect(() => JSON.stringify(result)).not.toThrow();
  });

  test('stream keeps legacy value shapes', async () => {
    const tableData = await driver.stream(TYPES_SQL, TYPES_PARAMS, { highWaterMark: 1 });

    try {
      expect(await streamToArray(tableData.rowStream as Readable)).toEqual(TYPES_EXPECTED);
    } finally {
      await tableData.release?.();
    }
  });

  test('stream tolerates null values', async () => {
    const tableData = await driver.stream('SELECT 1 AS one', null as unknown as unknown[], { highWaterMark: 1 });

    expect(await streamToArray(tableData.rowStream as Readable)).toEqual([{ one: '1' }]);
    await tableData.release?.();
  });

  test('empty results', async () => {
    expect(await driver.query('SELECT 1 WHERE false')).toEqual([]);

    const tableData = await driver.stream('SELECT 1 WHERE false', [], { highWaterMark: 1 });
    expect(await streamToArray(tableData.rowStream as Readable)).toEqual([]);
    await tableData.release?.();
  });

  test('concurrent streams and queries', async () => {
    const sql = 'SELECT range AS id FROM range(?)';
    const first = await driver.stream(sql, [10000], { highWaterMark: 1 });
    const second = await driver.stream(sql, [5000], { highWaterMark: 2 });

    try {
      const [firstRows, secondRows, queryRows] = await Promise.all([
        streamToArray(first.rowStream as Readable),
        streamToArray(second.rowStream as Readable),
        driver.query(sql, [10000]),
      ]);
      expect(firstRows).toEqual(queryRows);
      expect(firstRows).toHaveLength(10000);
      expect(secondRows).toEqual(queryRows.slice(0, 5000));
    } finally {
      await first.release?.();
      await second.release?.();
    }
  });

  test.each(['complete', 'release unread', 'destroy unread', 'break early'] as const)(
    'closes the stream connection exactly once: %s',
    async (mode) => {
      const close = jest.spyOn(DuckDBConnection.prototype, 'closeSync');
      const tableData = await driver.stream('SELECT * FROM range(10000)', [], { highWaterMark: 1 });
      const rowStream = tableData.rowStream as Readable;
      const closed = once(rowStream, 'close');

      if (mode === 'complete') {
        await streamToArray(rowStream);
      } else if (mode === 'release unread') {
        await tableData.release?.();
      } else if (mode === 'destroy unread') {
        rowStream.destroy();
      } else {
        // breaking out of async iteration destroys the Readable with an AbortError
        closed.catch(() => undefined);

        for await (const row of rowStream) {
          expect(row).toEqual({ range: '0' });
          break;
        }
      }

      await closed.catch((e) => {
        if (mode !== 'break early') {
          throw e;
        }
      });
      await tableData.release?.();
      await tableData.release?.();

      expect(close).toHaveBeenCalledTimes(1);
      await expect(driver.testConnection()).resolves.toBeUndefined();
    }
  );

  test('conversion error mid-stream destroys the stream and closes its connection', async () => {
    const close = jest.spyOn(DuckDBConnection.prototype, 'closeSync');
    const tableData = await driver.stream("SELECT DATE 'infinity' AS date", [], { highWaterMark: 1 });

    await expect(streamToArray(tableData.rowStream as Readable)).rejects.toThrow(RangeError);
    await tableData.release?.();

    expect(close).toHaveBeenCalledTimes(1);
    await expect(driver.testConnection()).resolves.toBeUndefined();
  });

  test.each([
    ['a scalar', '1::BIGINT', '1'],
    ['an object', '[1::BIGINT, 2::BIGINT]', ['1', '2']],
  ])('keeps a __proto__ column of %s in a result too wide for an object shape', async (_name, expr, expected) => {
    const columns = [
      `${expr} AS "__proto__"`,
      ...Array.from({ length: 127 }, (_, i) => `${i}::INTEGER AS c${i}`),
    ];

    const [row] = await driver.query<Record<string, unknown>>(`SELECT ${columns.join(', ')}`);

    const protoCell = (o: unknown) => Object.getOwnPropertyDescriptor(o, '__proto__')?.value;

    expect(Object.keys(row)).toHaveLength(128);
    expect(protoCell(row)).toEqual(expected);
    // JSON.parse also defines __proto__ as an own property, so the cell survives a round trip
    expect(protoCell(JSON.parse(JSON.stringify(row)))).toEqual(expected);
  });

  test('a throwing close surfaces as a stream error', async () => {
    const close = jest.spyOn(DuckDBConnection.prototype, 'closeSync').mockImplementation(() => {
      throw new Error('closeSync failed');
    });
    const tableData = await driver.stream('SELECT * FROM range(10000)', [], { highWaterMark: 1 });
    const rowStream = tableData.rowStream as Readable;
    const error = once(rowStream, 'error');

    rowStream.destroy();

    // without this the stream would never emit 'close', and release() below would hang
    await expect(error).resolves.toEqual([new Error('closeSync failed')]);
    await expect(tableData.release?.()).resolves.toBeUndefined();

    expect(close).toHaveBeenCalledTimes(1);
  });

  test('failed query and stream do not break the driver', async () => {
    const close = jest.spyOn(DuckDBConnection.prototype, 'closeSync');

    await expect(driver.query('SELECT * FROM nonexistent_table')).rejects.toThrow();
    await expect(driver.stream('SELECT * FROM nonexistent_table', [], { highWaterMark: 1 })).rejects.toThrow();

    expect(close).toHaveBeenCalledTimes(1);
    await expect(driver.testConnection()).resolves.toBeUndefined();
  });
});

describe('DuckDBDriver lifecycle', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  test('initializes lazily once and can be reused after release', async () => {
    const create = jest.spyOn(DuckDBInstance, 'create');
    const driver = new DuckDBDriver({ initSql: 'CREATE TABLE initial AS SELECT 42 AS value; INSERT INTO initial VALUES (43);' });
    const expected = [{ value: '42' }, { value: '43' }];

    try {
      expect(create).not.toHaveBeenCalled();
      const [first, second] = await Promise.all([
        driver.query('SELECT * FROM initial ORDER BY value'),
        driver.query('SELECT * FROM initial ORDER BY value'),
      ]);
      expect(first).toEqual(expected);
      expect(second).toEqual(expected);
      expect(create).toHaveBeenCalledTimes(1);

      await driver.release();
      await driver.release();
      expect(await driver.query('SELECT * FROM initial ORDER BY value')).toEqual(expected);
      expect(create).toHaveBeenCalledTimes(2);
    } finally {
      await driver.release();
    }
  });

  test('reopens a file-backed database after release', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'cube-duckdb-'));
    const driver = new DuckDBDriver({ databasePath: join(directory, 'test.duckdb') });

    try {
      await driver.query('CREATE TABLE persisted AS SELECT 42 AS value');
      await driver.release();
      expect(await driver.query('SELECT * FROM persisted')).toEqual([{ value: '42' }]);
    } finally {
      await driver.release();
      await rm(directory, { recursive: true, force: true });
    }
  });
});
