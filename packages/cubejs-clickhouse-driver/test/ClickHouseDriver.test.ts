import { ClickhouseDBRunner } from '@cubejs-backend/testing-shared';
import { streamToArray } from '@cubejs-backend/shared';

import { ClickHouseDriver } from '../src';
import type { ClickHouseDriverOptions } from '../src';

describe('ClickHouseDriver', () => {
  jest.setTimeout(20 * 1000);

  let container: any;
  let config: ClickHouseDriverOptions;

  const doWithDriver = async (cb: (driver: ClickHouseDriver) => Promise<any>) => {
    const driver = new ClickHouseDriver(config);

    try {
      await cb(driver);
    } catch (e) {
      const newError = new Error('doWithDriver failed', { cause: e });
      console.log(newError);
      throw newError;
    } finally {
      await driver.release();
    }
  };

  // eslint-disable-next-line func-names
  beforeAll(async () => {
    container = await ClickhouseDBRunner.startContainer({});

    config = {
      host: 'localhost',
      port: container.getMappedPort(8123),
    };

    await doWithDriver(async (driver) => {
      await driver.createSchemaIfNotExists('test');
      await driver.command(
        `
            CREATE TABLE test.types_test (
              date Date,
              datetime DateTime,
              datetime64_millis DateTime64(3, 'UTC'),
              datetime64_micros DateTime64(6, 'UTC'),
              datetime64_nanos DateTime64(9, 'UTC'),
              int8 Int8,
              int16 Int16,
              int32 Int32,
              int64 Int64,
              uint8 UInt8,
              uint16 UInt16,
              uint32 UInt32,
              uint64 UInt64,
              float32 Float32,
              float64 Nullable(Float64),
              decimal32 Decimal32(2),
              decimal64 Decimal64(2),
              decimal128 Decimal128(2),
              enum8 Enum('hello' = 1, 'world' = 2),
              enum16 Enum('hello' = 1, 'world' = 1000)
            ) ENGINE Log
        `
      );

      await driver.insert('test.types_test', [
        ['2020-01-01', '2020-01-01 00:00:00', '2020-01-01 00:00:00.000', '2020-01-01 00:00:00.000000', '2020-01-01 00:00:00.000000000', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1.01, 1.01, 1.01, 'hello', 'world']
      ]);
      await driver.insert('test.types_test', [
        ['2020-01-02', '2020-01-02 00:00:00', '2020-01-02 00:00:00.123', '2020-01-02 00:00:00.123456', '2020-01-02 00:00:00.123456789', 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2.02, 2.02, 2.02, 'hello', 'world']
      ]);
      await driver.insert('test.types_test', [
        ['2020-01-03', '2020-01-03 00:00:00', '2020-01-03 00:00:00.234', '2020-01-03 00:00:00.234567', '2020-01-03 00:00:00.234567890', 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3.03, 3.03, 3.03, 'hello', 'world']
      ]);
    });
  }, 30 * 1000);

  // eslint-disable-next-line func-names
  afterAll(async () => {
    jest.setTimeout(10 * 1000);

    await doWithDriver(async (driver) => {
      await driver.command('DROP DATABASE test');
    });

    if (container) {
      await container.stop();
    }
  }, 30 * 1000);

  it('should construct', async () => {
    await doWithDriver(async () => {
      //
    });
  });

  it('should test connection', async () => {
    await doWithDriver(async (driver) => {
      await driver.testConnection();
    });
  });

  it('should select raw sql', async () => {
    await doWithDriver(async (driver) => {
      const numbers = await driver.query('SELECT number FROM system.numbers LIMIT 10', []);
      expect(numbers).toEqual([
        { number: '0' },
        { number: '1' },
        { number: '2' },
        { number: '3' },
        { number: '4' },
        { number: '5' },
        { number: '6' },
        { number: '7' },
        { number: '8' },
        { number: '9' },
      ]);
    });
  });

  it('should select raw sql multiple times', async () => {
    await doWithDriver(async (driver) => {
      let numbers = await driver.query('SELECT number FROM system.numbers LIMIT 5', []);
      expect(numbers).toEqual([
        { number: '0' },
        { number: '1' },
        { number: '2' },
        { number: '3' },
        { number: '4' },
      ]);
      numbers = await driver.query('SELECT number FROM system.numbers LIMIT 5', []);
      expect(numbers).toEqual([
        { number: '0' },
        { number: '1' },
        { number: '2' },
        { number: '3' },
        { number: '4' },
      ]);
    });
  });

  it('should get tables', async () => {
    await doWithDriver(async (driver) => {
      const tables = await driver.getTablesQuery('system');
      expect(tables).toContainEqual({ table_name: 'numbers' });
    });
  });

  it('should create schema if not exists', async () => {
    await doWithDriver(async (driver) => {
      const name = `temp_${Date.now()}`;
      try {
        await driver.createSchemaIfNotExists(name);
      } finally {
        await driver.command(`DROP DATABASE ${name}`);
      }
    });
  });

  it('should normalise all numbers as strings', async () => {
    await doWithDriver(async (driver) => {
      const values = await driver.query('SELECT * FROM test.types_test LIMIT 1', []);
      expect(values).toEqual([{
        date: '2020-01-01T00:00:00.000',
        datetime: '2020-01-01T00:00:00.000',
        datetime64_millis: '2020-01-01T00:00:00.000',
        datetime64_micros: '2020-01-01T00:00:00.000',
        datetime64_nanos: '2020-01-01T00:00:00.000',
        int8: '1',
        int16: '1',
        int32: '1',
        int64: '1',
        uint8: '1',
        uint16: '1',
        uint32: '1',
        uint64: '1',
        float32: '1',
        float64: '1',
        decimal32: '1.01',
        decimal64: '1.01',
        decimal128: '1.01',
        enum8: 'hello',
        enum16: 'world',
      }]);
    });
  });

  it('should normalise all dates as ISO8601', async () => {
    await doWithDriver(async (driver) => {
      const name = `temp_${Date.now()}`;
      try {
        await driver.createSchemaIfNotExists(name);
        await driver.command(`CREATE TABLE ${name}.a (dateTime DateTime, date Date) ENGINE Log`);
        await driver.insert(`${name}.a`, [['2019-04-30 11:55:00', '2019-04-30']]);

        const values = await driver.query(`SELECT * FROM ${name}.a`, []);
        expect(values).toEqual([{
          dateTime: '2019-04-30T11:55:00.000',
          date: '2019-04-30T00:00:00.000',
        }]);
      } finally {
        await driver.command(`DROP DATABASE ${name}`);
      }
    });
  });

  it('should substitute parameters', async () => {
    await doWithDriver(async (driver) => {
      const name = `temp_${Date.now()}`;
      try {
        await driver.createSchemaIfNotExists(name);
        await driver.command(`CREATE TABLE ${name}.test (x Int32, s String) ENGINE Log`);
        await driver.insert(`${name}.test`, [[1, 'str1'], [2, 'str2'], [3, 'str3']]);
        const values = await driver.query(`SELECT * FROM ${name}.test WHERE x = ?`, [2]);
        expect(values).toEqual([{ x: '2', s: 'str2' }]);
      } finally {
        await driver.command(`DROP DATABASE ${name}`);
      }
    });
  });

  it('should return null for missing values on left outer join', async () => {
    await doWithDriver(async (driver) => {
      const name = `temp_${Date.now()}`;
      try {
        await driver.createSchemaIfNotExists(name);
        await driver.command(`CREATE TABLE ${name}.a (x Int32, s String) ENGINE Log`);
        await driver.insert(`${name}.a`, [[1, 'str1'], [2, 'str2'], [3, 'str3']]);

        await driver.command(`CREATE TABLE ${name}.b (x Int32, s String) ENGINE Log`);
        await driver.insert(`${name}.b`, [[2, 'str2'], [3, 'str3'], [4, 'str4']]);

        const values = await driver.query(`SELECT * FROM ${name}.a LEFT OUTER JOIN ${name}.b ON a.x = b.x`, []);
        expect(values).toEqual([
          {
            x: '1', s: 'str1', 'b.x': null, 'b.s': null
          },
          {
            x: '2', s: 'str2', 'b.x': '2', 'b.s': 'str2'
          },
          {
            x: '3', s: 'str3', 'b.x': '3', 'b.s': 'str3'
          }
        ]);
      } finally {
        await driver.command(`DROP DATABASE ${name}`);
      }
    });
  });

  it('datetime with specific timezone', async () => {
    await doWithDriver(async (driver) => {
      const rows = await driver.query('SELECT toDateTime(?, \'Asia/Istanbul\') as dt', [
        '2020-01-01 00:00:00'
      ]);
      expect(rows).toEqual([{
        dt: '2020-01-01T00:00:00.000'
      }]);
    });
  });

  it('query types_test', async () => {
    await doWithDriver(async (driver) => {
      const tableData = await driver.query('SELECT date, datetime, datetime64_micros FROM test.types_test ORDER BY int8', []);
      expect(tableData).toEqual([
        {
          date: '2020-01-01T00:00:00.000',
          datetime: '2020-01-01T00:00:00.000',
          datetime64_micros: '2020-01-01T00:00:00.000',
        },
        {
          date: '2020-01-02T00:00:00.000',
          datetime: '2020-01-02T00:00:00.000',
          datetime64_micros: '2020-01-02T00:00:00.123',
        },
        {
          date: '2020-01-03T00:00:00.000',
          datetime: '2020-01-03T00:00:00.000',
          datetime64_micros: '2020-01-03T00:00:00.234',
        }
      ]);
    });
  });

  it('stream', async () => {
    await doWithDriver(async (driver) => {
      const tableData = await driver.stream('SELECT * FROM test.types_test ORDER BY int8', [], {
        highWaterMark: 100,
      });

      try {
        expect(tableData.types).toEqual([
          { name: 'date', type: 'date' },
          { name: 'datetime', type: 'timestamp' },
          { name: 'datetime64_millis', type: 'timestamp' },
          { name: 'datetime64_micros', type: 'timestamp' },
          { name: 'datetime64_nanos', type: 'timestamp' },
          { name: 'int8', type: 'int' },
          { name: 'int16', type: 'int' },
          { name: 'int32', type: 'int' },
          { name: 'int64', type: 'bigint' },
          { name: 'uint8', type: 'int' },
          { name: 'uint16', type: 'int' },
          { name: 'uint32', type: 'int' },
          { name: 'uint64', type: 'bigint' },
          { name: 'float32', type: 'float' },
          { name: 'float64', type: 'double' },
          { name: 'decimal32', type: 'decimal' },
          { name: 'decimal64', type: 'decimal' },
          { name: 'decimal128', type: 'decimal' },
          { name: 'enum8', type: 'text' },
          { name: 'enum16', type: 'text' },
        ]);
        expect(await streamToArray(tableData.rowStream as any)).toEqual([
          { date: '2020-01-01T00:00:00.000', datetime: '2020-01-01T00:00:00.000', datetime64_millis: '2020-01-01T00:00:00.000', datetime64_micros: '2020-01-01T00:00:00.000', datetime64_nanos: '2020-01-01T00:00:00.000', int8: '1', int16: '1', int32: '1', int64: '1', uint8: '1', uint16: '1', uint32: '1', uint64: '1', float32: '1', float64: '1', decimal32: '1.01', decimal64: '1.01', decimal128: '1.01', enum8: 'hello', enum16: 'world' },
          { date: '2020-01-02T00:00:00.000', datetime: '2020-01-02T00:00:00.000', datetime64_millis: '2020-01-02T00:00:00.123', datetime64_micros: '2020-01-02T00:00:00.123', datetime64_nanos: '2020-01-02T00:00:00.123', int8: '2', int16: '2', int32: '2', int64: '2', uint8: '2', uint16: '2', uint32: '2', uint64: '2', float32: '2', float64: '2', decimal32: '2.02', decimal64: '2.02', decimal128: '2.02', enum8: 'hello', enum16: 'world' },
          { date: '2020-01-03T00:00:00.000', datetime: '2020-01-03T00:00:00.000', datetime64_millis: '2020-01-03T00:00:00.234', datetime64_micros: '2020-01-03T00:00:00.234', datetime64_nanos: '2020-01-03T00:00:00.234', int8: '3', int16: '3', int32: '3', int64: '3', uint8: '3', uint16: '3', uint32: '3', uint64: '3', float32: '3', float64: '3', decimal32: '3.03', decimal64: '3.03', decimal128: '3.03', enum8: 'hello', enum16: 'world' },
        ]);
      } finally {
        // @ts-ignore
        await tableData.release();
      }
    });
  });
});
