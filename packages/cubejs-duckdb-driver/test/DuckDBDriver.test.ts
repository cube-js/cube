import { streamToArray } from '@cubejs-backend/shared';
import { DuckDBDriver } from '../src';

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

  test('query', async () => {
    const result = await driver.query('select * from test.select_test ORDER BY id ASC', []);
    expect(result).toEqual([
      { id: '1', created: '2020-01-01T01:01:01.111Z', created_date: '2020-01-01T00:00:00.000Z', price: '100' },
      { id: '2', created: '2020-02-02T02:02:02.222Z', created_date: '2020-02-02T00:00:00.000Z', price: '200' },
      { id: '3', created: '2020-03-03T03:03:03.333Z', created_date: '2020-03-03T00:00:00.000Z', price: '300' }
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
  });
});
