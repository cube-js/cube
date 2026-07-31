import { describe, expect, test } from 'vitest';
import { streamToArray } from '@cubejs-backend/shared';
import { Readable } from 'stream';
import type { RowStatement } from 'snowflake-sdk';

import { CANCEL_ACK_TIMEOUT, SnowflakeDriver } from '../src/SnowflakeDriver';
import type { HydrationMap } from '../src/HydrationStream';

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

  test('downloadQueryResults() cancel aborts a running statement (memory)', async () => {
    const driver = new SnowflakeDriver({});
    try {
      const promise = driver.downloadQueryResults(LONG_RUNNING_QUERY, [], { highWaterMark: 100 });
      expect(typeof promise.cancel).toBe('function');

      await pause(5000);
      await promise.cancel();

      await expect(promise).rejects.toThrow(/cancelled/i);
    } finally {
      await driver.release();
    }
  }, 2 * 60 * 1000);

  test('downloadQueryResults() cancel aborts a running statement (stream)', async () => {
    const driver = new SnowflakeDriver({});
    try {
      const promise = driver.downloadQueryResults(
        LONG_RUNNING_QUERY,
        [],
        { highWaterMark: 100, streamImport: true },
      );
      expect(typeof promise.cancel).toBe('function');

      await pause(5000);
      await promise.cancel();

      await expect(promise).rejects.toThrow(/cancelled/i);
    } finally {
      await driver.release();
    }
  }, 2 * 60 * 1000);

  test('downloadQueryResults() returns memory data when not streaming', async () => {
    const driver = new SnowflakeDriver({});
    try {
      const tableData = <any> await driver.downloadQueryResults(
        QUERY_TO_TEST_HYDRATION,
        [],
        { highWaterMark: 100 },
      );
      assertHydrationResults(tableData.rows);
    } finally {
      await driver.release();
    }
  }, 2 * 60 * 1000);

  // These need no Snowflake connection: the point is what the driver does when the
  // SDK never answers, which no live server reproduces on demand.
  describe('cancelStatement()', () => {
    class TestSnowflakeDriver extends SnowflakeDriver {
      public logged: [string, any][] = [];

      public constructor() {
        super({});
        this.setLogger((msg, params) => this.logged.push([msg, params]));
      }

      public cancelStatementForTest(stmt: RowStatement): Promise<void> {
        return this.cancelStatement(stmt);
      }
    }

    // A statement wedged the way a black-holed socket wedges one: `cancel()` takes
    // the callback and only calls it if the test says so.
    const wedgedStatement = (onCancel?: (cb: (err: any) => void) => void) => <RowStatement>(<unknown>{
      cancel: (cb: (err: any) => void) => onCancel?.(cb),
      getQueryId: () => 'query-id',
    });

    // Waits out the real bound, so it costs CANCEL_ACK_TIMEOUT. Both assertions
    // live here rather than in two tests to pay that once.
    test('gives up waiting when the SDK never acknowledges, and ignores a late one', async () => {
      const driver = new TestSnowflakeDriver();

      let acknowledge: (() => void) | undefined;
      let settled = false;
      const cancelling = driver
        .cancelStatementForTest(
          wedgedStatement((cb) => { acknowledge = () => cb(new Error('too late')); }),
        )
        .then(() => { settled = true; });

      await pause(CANCEL_ACK_TIMEOUT / 2);
      expect(settled).toBe(false);

      await cancelling;
      expect(settled).toBe(true);

      // The wait has expired; a late callback must neither resolve a second time
      // nor report an error nobody can act on any more.
      acknowledge?.();

      expect(driver.logged).toEqual([
        ['Snowflake statement cancel timeout', { queryId: 'query-id' }],
      ]);
    }, CANCEL_ACK_TIMEOUT + 10 * 1000);

    test('reports an acknowledged failure without waiting out the bound', async () => {
      const driver = new TestSnowflakeDriver();

      const startedAt = Date.now();
      await driver.cancelStatementForTest(
        wedgedStatement((cb) => cb(new Error('cancel refused'))),
      );

      expect(Date.now() - startedAt).toBeLessThan(CANCEL_ACK_TIMEOUT);
      expect(driver.logged).toEqual([
        [
          'Snowflake statement cancel error',
          { queryId: 'query-id', error: expect.stringContaining('cancel refused') },
        ],
      ]);
    });

    test('resolves when the SDK acknowledges a successful cancel', async () => {
      const driver = new TestSnowflakeDriver();

      await driver.cancelStatementForTest(wedgedStatement((cb) => cb(undefined)));

      expect(driver.logged).toEqual([]);
    });
  });

  // Also connectionless: the point is what happens when the SDK's row stream
  // fails, which a live server does not do on request.
  describe('stream() row stream wiring', () => {
    class TestSnowflakeDriver extends SnowflakeDriver {
      public constructor() {
        super({});
      }

      public buildRowStreamForTest(sourceStream: Readable, hydrationMap: HydrationMap): Readable {
        return this.buildRowStream(sourceStream, hydrationMap);
      }
    }

    // The SDK's RowStream reports failures with a bare emit('error') instead of
    // destroy(err), so an unlistened 'error' throws right at the emit call.
    class FakeRowStream extends Readable {
      public constructor() {
        super({ objectMode: true });
      }

      public _read(): void {
        // rows are pushed by the test
      }

      public fail(err: Error): void {
        this.emit('error', err);
      }
    }

    const hydrationMap: HydrationMap = { n: (value: string) => `${value}!` };

    test('hands back the source itself when there is nothing to hydrate', () => {
      const driver = new TestSnowflakeDriver();
      const sourceStream = new FakeRowStream();

      expect(driver.buildRowStreamForTest(sourceStream, {})).toBe(sourceStream);
    });

    test('surfaces a source failure to the consumer of the hydrated stream', async () => {
      const driver = new TestSnowflakeDriver();
      const sourceStream = new FakeRowStream();
      const rowStream = driver.buildRowStreamForTest(sourceStream, hydrationMap);

      expect(rowStream).not.toBe(sourceStream);

      const rows = streamToArray(rowStream as any);
      sourceStream.push({ n: '1' });
      // A chunk download failing mid-stream: it must reach the consumer rather
      // than escape as an uncaught exception.
      sourceStream.fail(new Error('chunk download failed'));

      await expect(rows).rejects.toThrow('chunk download failed');
    });

    test('stays quiet when the source fails after release() tore it down', async () => {
      const driver = new TestSnowflakeDriver();
      const sourceStream = new FakeRowStream();
      const rowStream = driver.buildRowStreamForTest(sourceStream, hydrationMap);

      const seen: unknown[] = [];
      rowStream.on('error', (err) => seen.push(err));

      // What release() does when the consumer bailed out early.
      sourceStream.destroy();
      rowStream.destroy();

      // An in-flight chunk can still fail after that; nobody is left to act on it.
      sourceStream.fail(new Error('too late'));
      await pause(10);

      expect(seen).toEqual([]);
    });
  });

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
