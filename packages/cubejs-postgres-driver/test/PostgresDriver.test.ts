import { PostgresDBRunner } from '@cubejs-backend/testing-shared';
import { StartedTestContainer } from 'testcontainers';
import { PostgresDriver } from '../src';

const streamToArray = require('stream-to-array');

function largeParams(): Array<string> {
  return new Array(65536).fill('foo');
}

describe('PostgresDriver', () => {
  let container: StartedTestContainer;
  let driver: PostgresDriver;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    container = await PostgresDBRunner.startContainer({ volumes: [] });
    driver = new PostgresDriver({
      host: container.getHost(),
      port: container.getMappedPort(5432),
      user: 'test',
      password: 'test',
      database: 'test',
    });
    await driver.query('CREATE SCHEMA IF NOT EXISTS test;', []);
  });

  afterAll(async () => {
    await container.stop();
  });

  test('type coercion', async () => {
    await driver.query('CREATE TYPE CUBEJS_TEST_ENUM AS ENUM (\'FOO\');', []);

    const data = await driver.query(
      `
        SELECT
          CAST('2020-01-01' as DATE) as date,
          CAST('2020-01-01 00:00:00' as TIMESTAMP) as timestamp,
          CAST('2020-01-01 00:00:00+02' as TIMESTAMPTZ) as timestamptz,
          CAST('1.0' as DECIMAL(10,2)) as decimal,
          CAST('FOO' as CUBEJS_TEST_ENUM) as enum
      `,
      []
    );

    expect(data).toEqual([
      {
        // Date in UTC
        date: '2020-01-01T00:00:00.000',
        timestamp: '2020-01-01T00:00:00.000',
        // converted to utc
        timestamptz: '2019-12-31T22:00:00.000',
        // Numerics as string
        decimal: '1.00',
        // Enum datatypes as string
        enum: 'FOO',
      }
    ]);
  });

  test('too many params', async () => {
    await expect(
      driver.query(`SELECT 'foo'::TEXT;`, largeParams())
    )
      .rejects
      .toThrow('PostgreSQL protocol does not support more than 65535 parameters, but 65536 passed');
  });

  test('stream', async () => {
    await driver.uploadTable(
      'test.streaming_test',
      [
        { name: 'id', type: 'bigint' },
        { name: 'created', type: 'date' },
        { name: 'price', type: 'decimal' }
      ],
      {
        rows: [
          { id: 1, created: '2020-01-01', price: '100' },
          { id: 2, created: '2020-01-02', price: '200' },
          { id: 3, created: '2020-01-03', price: '300' }
        ]
      }
    );

    const tableData = await driver.stream('select * from test.streaming_test', [], {
      highWaterMark: 1000,
    });

    try {
      expect(await tableData.types).toEqual([
        {
          name: 'id',
          type: 'bigint'
        },
        {
          name: 'created',
          type: 'date'
        },
        {
          name: 'price',
          type: 'decimal'
        },
      ]);
      expect(await streamToArray(tableData.rowStream)).toEqual([
        { id: '1', created: '2020-01-01T00:00:00.000', price: '100' },
        { id: '2', created: '2020-01-02T00:00:00.000', price: '200' },
        { id: '3', created: '2020-01-03T00:00:00.000', price: '300' }
      ]);
    } finally {
      await (<any> tableData).release();
    }
  });

  test('stream (exception)', async () => {
    try {
      await driver.stream('select * from test.random_name_for_table_that_doesnot_exist_sql_must_fail', [], {
        highWaterMark: 1000,
      });

      throw new Error('stream must throw an exception');
    } catch (e: any) {
      expect(e.message).toEqual(
        'relation "test.random_name_for_table_that_doesnot_exist_sql_must_fail" does not exist'
      );
    }
  });

  test('stream (too many params)', async () => {
    try {
      await driver.stream('select * from test.streaming_test', largeParams(), {
        highWaterMark: 1000,
      });

      throw new Error('stream must throw an exception');
    } catch (e: any) {
      expect(e.message).toEqual(
        'PostgreSQL protocol does not support more than 65535 parameters, but 65536 passed'
      );
    }
  });

  test('table name check', async () => {
    const tblName = 'really-really-really-looooooooooooooooooooooooooooooooooooooooooooooooooooong-table-name';
    try {
      await driver.createTable(tblName, [{ name: 'id', type: 'bigint' }]);

      throw new Error('createTable must throw an exception');
    } catch (e: any) {
      expect(e.message).toEqual(
        'PostgreSQL can not work with table names longer than 63 symbols. ' +
        `Consider using the 'sqlAlias' attribute in your cube definition for ${tblName}.`
      );
    }
  });

  // Note: This test MUST be the last in the list.
  test('release', async () => {
    expect(async () => {
      await driver.release();
    }).not.toThrowError(
      /Called end on pool more than once/
    );

    expect(async () => {
      await driver.release();
    }).not.toThrowError(
      /Called end on pool more than once/
    );
  });
});
