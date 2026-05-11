// eslint-disable-next-line import/no-extraneous-dependencies
import { DriverTests, smartStringTrim } from '@cubejs-backend/testing-shared';
import { pausePromise } from '@cubejs-backend/shared';

import { AthenaDriver } from '../src';

class AthenaDriverTest extends DriverTests {
  protected getExpectedCsvRows() {
    // Athena uses \N for null values
    return smartStringTrim`
      orders__status,orders__amount
      new,300
      processed,400
      \N,500
    `;
  }
}

// A SQL query that is guaranteed to take long enough on Athena for the
// test to call `.cancel()` before it finishes. 100M-row aggregation is
// reliably in the 5-30s range on a small workgroup.
const SLOW_QUERY = `
  SELECT count(*) AS c
  FROM unnest(sequence(1, 100000000)) AS t(i)
  WHERE i % 2 = 0
`;

describe('AthenaDriver', () => {
  let tests: AthenaDriverTest;
  let driver: AthenaDriver;

  jest.setTimeout(3 * 60 * 1000);

  beforeAll(async () => {
    driver = new AthenaDriver({});
    tests = new AthenaDriverTest(
      driver,
      {
        expectStringFields: true,
        csvNoHeader: true,
        wrapLoadQueryWithCtas: true,
        delimiter: '\x01',
      }
    );
  });

  afterAll(async () => {
    await tests.release();
  });

  test('query', async () => {
    await tests.testQuery();
  });

  test('stream', async () => {
    await tests.testStream();
  });

  test('unload CSV escape symbol', async () => {
    await tests.testUnloadEscapeSymbolOp1(AthenaDriver);
    await tests.testUnloadEscapeSymbolOp2(AthenaDriver);
    await tests.testUnloadEscapeSymbolOp3(AthenaDriver);
  });

  test('unload empty', async () => {
    await tests.testUnloadEmpty();
  });

  // Verifies that the driver propagates orchestrator-side cancellation
  // to Athena via StopQueryExecution. The query is captured by spying
  // on startQueryExecution; after .cancel() resolves we poll Athena's
  // own state for that execution id and assert it landed in a terminal
  // non-success state (CANCELLED or FAILED), never RUNNING/QUEUED.
  const expectQueryCancelled = async (queryExecutionId: string) => {
    const athena = (driver as any).athena;
    for (let i = 0; i < 30; i++) {
      const exec = await athena.getQueryExecution({ QueryExecutionId: queryExecutionId });
      const state = exec.QueryExecution?.Status?.State;
      if (state === 'CANCELLED' || state === 'FAILED') {
        return;
      }
      if (state === 'SUCCEEDED') {
        throw new Error(`Athena query ${queryExecutionId} succeeded before cancel took effect`);
      }
      await pausePromise(500);
    }
    throw new Error(`Athena query ${queryExecutionId} did not reach a terminal state within 15s of cancel`);
  };

  const captureStartedQueryId = () => {
    const athena = (driver as any).athena;
    const original = athena.startQueryExecution.bind(athena);
    const captured = { id: '' };
    athena.startQueryExecution = async (input: any) => {
      const result = await original(input);
      if (!captured.id && result.QueryExecutionId) {
        captured.id = result.QueryExecutionId;
      }
      return result;
    };
    const restore = () => {
      athena.startQueryExecution = original;
    };
    return { captured, restore };
  };

  test('query cancel propagates StopQueryExecution to Athena', async () => {
    const { captured, restore } = captureStartedQueryId();
    try {
      const promise = driver.query(SLOW_QUERY, []) as Promise<unknown> & { cancel: () => Promise<void> };
      expect(typeof promise.cancel).toBe('function');

      // Wait briefly so startQueryExecution has returned a query id.
      while (!captured.id) {
        await pausePromise(100);
      }

      await promise.cancel();
      await expect(promise).rejects.toBeDefined();
      await expectQueryCancelled(captured.id);
    } finally {
      restore();
    }
  });

  test('stream cancel propagates StopQueryExecution to Athena', async () => {
    const { captured, restore } = captureStartedQueryId();
    try {
      // stream() awaits the underlying query to succeed before
      // returning the row stream, so an in-flight cancel maps to
      // calling .cancel() on the returned promise (which the
      // orchestrator does via cancelCombinator's saved cancels) — not
      // to the released() callback, which only runs after stream()
      // has already resolved.
      const promise = driver.stream(SLOW_QUERY, [], { highWaterMark: 100 }) as
        Promise<unknown> & { cancel: () => Promise<void> };
      expect(typeof promise.cancel).toBe('function');

      while (!captured.id) {
        await pausePromise(100);
      }

      await promise.cancel();
      await expect(promise).rejects.toBeDefined();
      await expectQueryCancelled(captured.id);
    } finally {
      restore();
    }
  });
});
