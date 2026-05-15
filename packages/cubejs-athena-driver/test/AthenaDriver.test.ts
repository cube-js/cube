// eslint-disable-next-line import/no-extraneous-dependencies
import { DriverTests, smartStringTrim } from '@cubejs-backend/testing-shared';
import { pausePromise } from '@cubejs-backend/shared';
import { QueryExecutionState } from '@aws-sdk/client-athena';

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

// A row-by-row regex+concat over a 49999×49999 cross-join forces full
// materialization (no aggregate pushdown), so Athena cannot finish
// before the driver's pollTimeout fires.
const SLOW_QUERY = `
  SELECT count(*) AS c
  FROM (
    SELECT length(regexp_replace(
      CONCAT(CAST(a.i * b.j + 7919 AS VARCHAR), '-', CAST(a.i AS VARCHAR)),
      '[0-9]', 'd'
    )) AS n
    FROM unnest(sequence(1, 49999)) AS a(i)
    CROSS JOIN unnest(sequence(1, 49999)) AS b(j)
  )
  WHERE n > 0
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

  test('pollTimeout cancels the in-flight Athena query', async () => {
    // Aggressive pollTimeout (5s) so the test doesn't depend on the
    // ambient CUBEJS_DB_QUERY_TIMEOUT. Constructor multiplies by 1000.
    const cancelDriver = new AthenaDriver({ pollTimeout: 5 });
    const athena = (cancelDriver as any).athena;

    const startOriginal = athena.startQueryExecution.bind(athena);
    let queryExecutionId = '';
    athena.startQueryExecution = async (input: any) => {
      const result = await startOriginal(input);
      queryExecutionId = result.QueryExecutionId;
      return result;
    };

    try {
      await expect(cancelDriver.query(SLOW_QUERY, [])).rejects.toThrow(/Athena job timeout/);
      expect(queryExecutionId).toBeTruthy();

      // Verify Athena's own view of the query: must be CANCELLED (or
      // FAILED, if a cancel raced with completion) — never SUCCEEDED.
      for (let i = 0; i < 30; i++) {
        const exec = await athena.getQueryExecution({ QueryExecutionId: queryExecutionId });
        const state = exec.QueryExecution?.Status?.State;
        if (state === QueryExecutionState.CANCELLED || state === QueryExecutionState.FAILED) {
          return;
        }
        if (state === QueryExecutionState.SUCCEEDED) {
          throw new Error(`Athena query ${queryExecutionId} succeeded before cancel took effect`);
        }
        await pausePromise(500);
      }
      throw new Error(`Athena query ${queryExecutionId} did not reach a terminal state within 15s of cancel`);
    } finally {
      await cancelDriver.release();
    }
  });
});
