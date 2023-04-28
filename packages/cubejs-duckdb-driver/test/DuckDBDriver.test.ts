import { streamToArray } from '@cubejs-backend/shared';
import { DuckDBDriver } from '../src';

describe('DuckDBDriver', () => {
  let driver: DuckDBDriver;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    driver = new DuckDBDriver({});
    await driver.query('CREATE SCHEMA IF NOT EXISTS test;', []);
  });

  afterAll(async () => {
    await driver.release();
  });

  test('query', async () => {
    await driver.uploadTable(
      'test.querying_test',
      [
        { name: 'id', type: 'bigint' },
        { name: 'created', type: 'date' },
        { name: 'price', type: 'decimal' },
      ],
      {
        rows: [
          { id: 1, created: '2020-01-01', price: '100' },
          { id: 2, created: '2020-01-02', price: '200' },
          { id: 3, created: '2020-01-03', price: '300' }
        ]
      }
    );

    const result = await driver.query('select * from test.querying_test', []);
    expect(result).toEqual([
      { id: '1', created: '2020-01-01T00:00:00.000Z', price: '100' },
      { id: '2', created: '2020-01-02T00:00:00.000Z', price: '200' },
      { id: '3', created: '2020-01-03T00:00:00.000Z', price: '300' }
    ]);
  });

  test('stream', async () => {
    await driver.uploadTable(
      'test.streaming_test',
      [
        { name: 'id', type: 'bigint' },
        { name: 'created', type: 'date' },
        { name: 'price', type: 'decimal' },
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

    expect(await tableData.types).toEqual(undefined);
    expect(await streamToArray(tableData.rowStream as any)).toEqual([
      { id: '1', created: '2020-01-01T00:00:00.000Z', price: '100' },
      { id: '2', created: '2020-01-02T00:00:00.000Z', price: '200' },
      { id: '3', created: '2020-01-03T00:00:00.000Z', price: '300' }
    ]);
  });
});
