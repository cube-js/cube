import { PostgresDBRunner } from '@cubejs-backend/testing-shared';
import { StartedTestContainer } from 'testcontainers';
import { PostgresDriver } from '../src';

const streamToArray = require('stream-to-array');

function largeParams(): Array<string> {
  return new Array(65536).fill('foo');
}

const pause = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

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

  test('stream (array-typed columns)', async () => {
    // Streaming must not fail when a query returns array-typed columns.
    // Array types are reported as `text` and node-postgres parses them into
    // JS arrays. See CORE-522.
    const tableData = await driver.stream(
      `SELECT
        ARRAY['oops', 'test']::text[] as text_array,
        ARRAY[1, 2, 3]::int[] as int_array`,
      [],
      {
        highWaterMark: 1000,
      }
    );

    try {
      expect(await tableData.types).toEqual([
        {
          name: 'text_array',
          type: 'text'
        },
        {
          name: 'int_array',
          type: 'text'
        },
      ]);
      expect(await streamToArray(tableData.rowStream)).toEqual([
        { text_array: ['oops', 'test'], int_array: [1, 2, 3] },
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

  test('cancels the exact backend query and keeps the connection reusable', async () => {
    const marker = `cube-postgres-cancel-test-${Date.now()}`;
    const runningQuery = driver.query(`SELECT pg_sleep(30) /* ${marker} */`, []);
    const queryResult = runningQuery.then(
      () => null,
      (error: unknown) => error as Error & { code?: string }
    );

    expect(runningQuery.cancel).toEqual(expect.any(Function));

    let observedActiveQuery = false;
    for (let attempt = 0; attempt < 50; attempt++) {
      const [{ active }] = await driver.query<{ active: string }>(
        `SELECT count(*)::text AS active
         FROM pg_stat_activity
         WHERE state = 'active' AND query LIKE $1`,
        [`%${marker}%`]
      );
      if (active === '1') {
        observedActiveQuery = true;
        break;
      }
      await pause(50);
    }
    expect(observedActiveQuery).toBe(true);

    const startedAt = Date.now();
    await Promise.all([
      runningQuery.cancel!(),
      runningQuery.cancel!(),
      runningQuery.cancel!(),
    ]);

    const error = await queryResult;
    expect(error).not.toBeNull();
    expect(error?.code).toBe('57014');
    expect(Date.now() - startedAt).toBeLessThan(5000);

    const [{ active }] = await driver.query<{ active: string }>(
      `SELECT count(*)::text AS active
       FROM pg_stat_activity
       WHERE state = 'active' AND query LIKE $1`,
      [`%${marker}%`]
    );
    expect(active).toBe('0');

    await expect(driver.query('SELECT 1 AS value', [])).resolves.toEqual([{ value: 1 }]);
  });

  test('cancels a streaming backend query and releases its connection', async () => {
    const marker = `cube-postgres-stream-cancel-test-${Date.now()}`;
    const streamPromise = driver.stream(`SELECT pg_sleep(30) /* ${marker} */`, [], {
      highWaterMark: 100,
    });
    const streamResult = streamPromise.then(
      () => null,
      (error: unknown) => error as Error & { code?: string }
    );

    expect(streamPromise.cancel).toEqual(expect.any(Function));

    let observedActiveQuery = false;
    for (let attempt = 0; attempt < 50; attempt++) {
      const [{ active }] = await driver.query<{ active: string }>(
        `SELECT count(*)::text AS active
         FROM pg_stat_activity
         WHERE state = 'active' AND query LIKE $1`,
        [`%${marker}%`]
      );
      if (active === '1') {
        observedActiveQuery = true;
        break;
      }
      await pause(50);
    }
    expect(observedActiveQuery).toBe(true);

    await Promise.all([streamPromise.cancel!(), streamPromise.cancel!()]);
    expect((await streamResult)?.code).toBe('57014');
    await expect(driver.query('SELECT 1 AS value', [])).resolves.toEqual([{ value: 1 }]);
  });

  test('cancel before pool acquisition prevents query submission', async () => {
    const singleConnectionDriver = new PostgresDriver({
      host: container.getHost(),
      port: container.getMappedPort(5432),
      user: 'test',
      password: 'test',
      database: 'test',
      maxPoolSize: 1,
    });
    const marker = `cube-postgres-cancel-before-submit-${Date.now()}`;

    try {
      const blocker = singleConnectionDriver.query('SELECT pg_sleep(1)', []);
      const queued = singleConnectionDriver.query(`SELECT 1 /* ${marker} */`, []);
      const queuedResult = queued.then(
        () => null,
        (error: unknown) => error as Error & { code?: string }
      );

      await Promise.all([queued.cancel!(), queued.cancel!()]);
      await blocker;

      expect((await queuedResult)?.code).toBe('57014');
      const [{ seen }] = await singleConnectionDriver.query<{ seen: boolean }>(
        `SELECT EXISTS(
           SELECT 1 FROM pg_stat_activity WHERE query LIKE $1
         ) AS seen`,
        [`%${marker}%`]
      );
      expect(seen).toBe(false);
      await expect(singleConnectionDriver.query('SELECT 1 AS value', [])).resolves.toEqual([{ value: 1 }]);
    } finally {
      await singleConnectionDriver.release();
    }
  });

  test('cancellation is isolated between concurrent queries', async () => {
    const cancelledMarker = `cube-postgres-cancel-isolated-${Date.now()}`;
    const cancelledQuery = driver.query(`SELECT pg_sleep(30) /* ${cancelledMarker} */`, []);
    const cancelledResult = cancelledQuery.then(
      () => null,
      (error: unknown) => error as Error & { code?: string }
    );
    const survivingQuery = driver.query('SELECT pg_sleep(1), 42 AS value', []);

    await pause(100);
    await cancelledQuery.cancel!();

    expect((await cancelledResult)?.code).toBe('57014');
    await expect(survivingQuery).resolves.toEqual([{ pg_sleep: '', value: 42 }]);
    await expect(driver.query('SELECT 1 AS value', [])).resolves.toEqual([{ value: 1 }]);
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
