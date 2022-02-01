import { BigQueryDriver } from '../src';

import * as streamToArray from 'stream-to-array';

describe('BigQueryDriver', () => {
  let driver: BigQueryDriver;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    driver = new BigQueryDriver({});
  });

  afterAll(async () => {
    await driver.release();
  });

  test('type coercion', async () => {
    // await driver.query('CREATE TYPE CUBEJS_TEST_ENUM AS ENUM (\'FOO\');', []);

    const data = await driver.query(
      `
        SELECT
          CAST('2020-01-01' as DATE) as date,
          CAST('2020-01-01 00:00:00' as TIMESTAMP) as timestamp,
          CAST('2020-01-01 00:00:00+02' as TIMESTAMPTZ) as timestamptz,
          CAST('1.0' as DECIMAL(10,2)) as decimal,
      `,
      // CAST('FOO' as CUBEJS_TEST_ENUM) as enum
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
        // enum: 'FOO',
      }
    ]);
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

    const tableData = await driver.stream('select * from test.streaming_test', []);

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
      await driver.stream('select * from test.random_name_for_table_that_doesnot_exist_sql_must_fail', []);

      throw new Error('stream must throw an exception');
    } catch (e) {
      expect(e.message).toEqual(
        'relation "test.random_name_for_table_that_doesnot_exist_sql_must_fail" does not exist'
      );
    }
  });
});
