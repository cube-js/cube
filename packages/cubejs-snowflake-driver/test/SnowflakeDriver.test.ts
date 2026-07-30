import { describe, expect, test } from 'vitest';
import { streamToArray } from '@cubejs-backend/shared';

import { SnowflakeDriver } from '../src';

// Long enough that a run to completion is unmistakably distinguishable from a
// successful cancel. SYSTEM$WAIT holds the warehouse without producing data.
const LONG_RUNNING_QUERY = 'SELECT SYSTEM$WAIT(120)';

const pause = (ms: number) => new Promise((resolve) => { setTimeout(resolve, ms); });

const QUERY_TO_TEST_HYDRATION = `
  SELECT
    CAST(1265.88 AS NUMBER(10,2))                  AS "n",
    CAST('2026-04-28 13:07:42.123' AS TIMESTAMP_NTZ)         AS "ts_ntz",
    CAST('2026-04-28 13:07:42.123 +0000' AS TIMESTAMP_TZ)    AS "ts_tz",
    CAST('2026-04-28' AS DATE)                               AS "d"
  UNION ALL
  SELECT
    CAST(0.10 AS NUMBER(10,2)),
    CAST('2000-02-29 00:00:00.007' AS TIMESTAMP_NTZ),
    CAST('2000-02-29 00:00:00.007 +0000' AS TIMESTAMP_TZ),
    CAST('2000-02-29' AS DATE);
`;

function assertHydrationResults(rows: any[]) {
  expect(rows).toEqual([
    {
      n: '1265.88',
      ts_ntz: '2026-04-28T13:07:42.123',
      ts_tz: '2026-04-28T13:07:42.123',
      d: '2026-04-28T00:00:00.000',
    },
    {
      n: '0.10',
      ts_ntz: '2000-02-29T00:00:00.007',
      ts_tz: '2000-02-29T00:00:00.007',
      d: '2000-02-29T00:00:00.000',
    },
  ]);
}

describe('SnowflakeDriver', () => {
  test('query', async () => {
    const driver = new SnowflakeDriver({});
    try {
      const rows = await driver.query<any[]>(QUERY_TO_TEST_HYDRATION, []);
      assertHydrationResults(rows);
    } finally {
      await driver.release();
    }
  }, 2 * 60 * 1000);

  test('stream', async () => {
    const driver = new SnowflakeDriver({});
    try {
      const tableData = await driver.stream(QUERY_TO_TEST_HYDRATION, [], { highWaterMark: 100 });
      try {
        const rows = await streamToArray(tableData.rowStream as any);
        assertHydrationResults(rows as any[]);
      } finally {
        await tableData.release?.();
      }
    } finally {
      await driver.release();
    }
  }, 2 * 60 * 1000);

  test('query() exposes cancel synchronously', async () => {
    const driver = new SnowflakeDriver({});
    try {
      // QueryCache reads `resultPromise.cancel` on the very next line after
      // calling the driver, so it must be there without awaiting anything.
      // Declaring the method `async` would silently drop it.
      const promise = driver.query(LONG_RUNNING_QUERY, []);
      expect(typeof promise.cancel).toBe('function');

      await promise.cancel();
      await expect(promise).rejects.toThrow(/cancelled/i);
    } finally {
      await driver.release();
    }
  }, 2 * 60 * 1000);

  test('query() cancel aborts a running statement', async () => {
    const driver = new SnowflakeDriver({});
    try {
      const promise = driver.query(LONG_RUNNING_QUERY, []);
      // Let the statement actually reach Snowflake before aborting it.
      await pause(5000);

      const startedAt = Date.now();
      await promise.cancel();
      await expect(promise).rejects.toThrow(/cancelled/i);

      // The query would otherwise hold the warehouse for 120s.
      expect(Date.now() - startedAt).toBeLessThan(30 * 1000);

      // Cancelling one statement must not poison the shared connection.
      expect(await driver.query('SELECT 1 AS "one"', [])).toHaveLength(1);
    } finally {
      await driver.release();
    }
  }, 2 * 60 * 1000);

  test('query() cancel before the connection is established', async () => {
    const driver = new SnowflakeDriver({});
    try {
      // No await in between: the driver is still connecting, so no statement
      // exists yet and there is nothing to abort - it must simply never issue one.
      const promise = driver.query(LONG_RUNNING_QUERY, []);
      await promise.cancel();

      await expect(promise).rejects.toThrow(/cancelled/i);
    } finally {
      await driver.release();
    }
  }, 2 * 60 * 1000);

  test('stream() cancel aborts a running statement', async () => {
    const driver = new SnowflakeDriver({});
    try {
      const promise = driver.stream(LONG_RUNNING_QUERY, [], { highWaterMark: 100 });
      expect(typeof promise.cancel).toBe('function');

      await pause(5000);
      await promise.cancel();

      await expect(promise).rejects.toThrow(/cancelled/i);
    } finally {
      await driver.release();
    }
  }, 2 * 60 * 1000);

  test('stream() release() after normal completion does not abort', async () => {
    const driver = new SnowflakeDriver({});
    try {
      const tableData = await driver.stream(QUERY_TO_TEST_HYDRATION, [], { highWaterMark: 100 });
      const rows = await streamToArray(tableData.rowStream as any);
      assertHydrationResults(rows as any[]);

      // release() also fires on the normal-completion path (QueryCache calls it
      // from rowStream 'end'), so it must be a no-op rather than a stray abort.
      await tableData.release?.();
      await tableData.release?.();

      expect(await driver.query('SELECT 1 AS "one"', [])).toHaveLength(1);
    } finally {
      await driver.release();
    }
  }, 2 * 60 * 1000);
});
