import { QuestDBRunner } from '@cubejs-backend/testing-shared';

import { StartedTestContainer } from 'testcontainers';

import { QuestDriver } from '../src';

describe('QuestDriver', () => {
  let container: StartedTestContainer;
  let driver: QuestDriver;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    container = await QuestDBRunner.startContainer({ volumes: [] });
    driver = new QuestDriver({
      host: container.getHost(),
      port: container.getMappedPort(8812),
      user: 'admin',
      password: 'quest',
      database: 'qdb',
    });
  });

  afterAll(async () => {
    await driver.release();

    if (container) {
      await container.stop();
    }
  });

  test('query', async () => {
    await driver.uploadTable(
      'query_test',
      [
        { name: 'id', type: 'long' },
        { name: 'created', type: 'date' },
        { name: 'price', type: 'double' }
      ],
      {
        rows: [
          { id: 1, created: '2020-01-01T00:00:00.000Z', price: 100.5 },
          { id: 2, created: '2020-01-02T00:00:00.000Z', price: 200.5 },
          { id: 3, created: '2020-01-03T00:00:00.000Z', price: 300.5 }
        ]
      }
    );

    const tableData = await driver.query('select * from query_test', []);

    expect(tableData).toEqual([
      { id: '1', created: '2020-01-01T00:00:00.000', price: 100.5 },
      { id: '2', created: '2020-01-02T00:00:00.000', price: 200.5 },
      { id: '3', created: '2020-01-03T00:00:00.000', price: 300.5 }
    ]);
  });

  test('query (exception)', async () => {
    try {
      await driver.query('select * from random_name_for_table_that_doesnot_exist_sql_must_fail', []);

      throw new Error('stream must throw an exception');
    } catch (e) {
      expect((e as Error).message).toEqual(
        'table does not exist [name=random_name_for_table_that_doesnot_exist_sql_must_fail]'
      );
    }
  });

  test('schema', async () => {
    const schema = await driver.tablesSchema();

    expect(schema['']).toEqual({
        'query_test': [
          {
            name: 'id',
            type: 'LONG',
          },
          {
            name: 'created',
            type: 'date',
          },
          {
            name: 'price',
            type: 'DOUBLE',
          },
        ],
        'telemetry': [
          {
            name: 'created',
            type: 'TIMESTAMP',
          },
          {
            name: 'event',
            type: 'SHORT',
          },
          {
            name: 'origin',
            type: 'SHORT',
          },
        ],
        'telemetry_config': [
          {
            name: 'id',
            type: 'LONG256',
          },
          {
            name: 'enabled',
            type: 'boolean',
          },
          {
            name: 'version',
            type: 'SYMBOL',
          },
          {
            name: 'os',
            type: 'SYMBOL',
          },
          {
            name: 'package',
            type: 'SYMBOL',
          },
        ],
    });
  });
});
