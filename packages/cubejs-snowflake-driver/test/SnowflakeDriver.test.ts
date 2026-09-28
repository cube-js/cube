import { describe, expect, test } from 'vitest';
import { streamToArray } from '@cubejs-backend/shared';

import { SnowflakeDriver } from '../src';

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
});
