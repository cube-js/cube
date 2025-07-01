import { MaterializeDBRunner } from '@cubejs-backend/testing-shared';

import { streamToArray } from '@cubejs-backend/shared';

import { StartedTestContainer } from 'testcontainers';

import { MaterializeDriver } from '../src';

/**
 * Pre-requisite:
 * Docker up and running
 *
 * MaterializeDBRunner will use a Materialize container to do the tests.
 * In case the container is not present, it will pull a an image.
 * These three variables can define the pulled image version:
 *    - Environment variables: TEST_MZSQL_VERSION
 *    - Parameter: options.version in startContainer method
 *    - Static version: defined inside MaterializeDBRunner
 */
describe('MaterializeDriver', () => {
  let container: StartedTestContainer;
  let driver: MaterializeDriver;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    container = await MaterializeDBRunner.startContainer({});
    driver = new MaterializeDriver({
      host: container.getHost(),
      port: container.getMappedPort(6875),
      user: 'materialize',
      password: 'materialize',
      database: 'materialize',
      cluster: 'quickstart',
      ssl: false,
    });
    await driver.query('CREATE SCHEMA IF NOT EXISTS test;', []);
  });

  afterAll(async () => {
    await driver.release();

    if (container) {
      await container.stop();
    }
  });

  test('type coercion', async () => {
    const data = await driver.query(
      `
        SELECT
          CAST('2020-01-01' as DATE) as date,
          CAST('2020-01-01 00:00:00' as TIMESTAMP) as timestamp,
          CAST('2020-01-01 00:00:00+02' as TIMESTAMPTZ) as timestamptz,
          CAST('1.0' as DECIMAL(10,2)) as decimal
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
        decimal: '1',
      }
    ]);
  });

  test('schema detection', async () => {
    await Promise.all([
      driver.query('CREATE TABLE A (a INT, b BIGINT, c TEXT, d DOUBLE, e FLOAT);', []),
      driver.query('CREATE VIEW V AS SELECT * FROM A;', []),
      driver.query('CREATE MATERIALIZED VIEW MV AS SELECT * FROM A;', []),
    ]);

    const tablesSchemaData = await driver.tablesSchema();
    const { public: publicSchema } = tablesSchemaData;
    const { a, v, mv } = publicSchema;

    expect(a).toEqual([
      { name: 'c', type: 'text', attributes: [] },
      { name: 'b', type: 'bigint', attributes: [] },
      { name: 'a', type: 'integer', attributes: [] },
      { name: 'd', type: 'double precision', attributes: [] },
      { name: 'e', type: 'double precision', attributes: [] }
    ]);
    expect(mv).toBeDefined();
    expect(v).toBeUndefined();
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
      await (<any>tableData).release();
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
        'unknown catalog item \'test.random_name_for_table_that_doesnot_exist_sql_must_fail\''
      );
    }
  });

  test('cluster', async () => {
    const data = await driver.query(`SHOW CLUSTER;`, []);
    expect(data).toEqual([
      {
        'cluster': 'quickstart',
      }]);
  });

});
