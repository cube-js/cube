import { PostgresDBRunner } from '@cubejs-backend/testing';

import { StartedTestContainer } from 'testcontainers';

import { PostgresDriver } from '../src';

const streamToArray = require('stream-to-array');

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
    await driver.release();

    if (container) {
      await container.stop();
    }
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
          type: 'numeric'
        },
      ]);
      expect(await streamToArray(tableData.rowStream)).toEqual([
        { id: '1', created: expect.any(Date), price: '100' },
        { id: '2', created: expect.any(Date), price: '200' },
        { id: '3', created: expect.any(Date), price: '300' }
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
    } catch (e) {
      expect(e.message).toEqual(
        'relation "test.random_name_for_table_that_doesnot_exist_sql_must_fail" does not exist'
      );
    }
  });
});
