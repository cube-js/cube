import { Readable } from 'stream';
import { describe, expect, test } from 'vitest';
import type { Connection, StatementOption } from 'snowflake-sdk';

import { SnowflakeDriver } from '../../src/SnowflakeDriver';

// Records what the driver hands to the SDK instead of talking to Snowflake.
class TestSnowflakeDriver extends SnowflakeDriver {
  public statements: StatementOption[] = [];

  protected override async getConnection(): Promise<Connection> {
    return <Connection>(<unknown>{
      execute: (options: StatementOption) => {
        this.statements.push(options);
        const stmt = { getColumns: () => [], streamRows: () => Readable.from([]) };
        // `COPY INTO` reports zero unloaded rows, so unload() skips listing the bucket.
        const rows = options.sqlText.includes('COPY INTO') ? [{ rows_unloaded: '0' }] : [];
        options.complete?.(<any>undefined, <any>stmt, rows);
        return stmt;
      },
    });
  }
}

const REQUEST_ID = '3f2e1b8a-5c3d-4e2f-9a1b-7c6d5e4f3a2b-span-1';
const REQUEST_UUID = '3f2e1b8a-5c3d-4e2f-9a1b-7c6d5e4f3a2b';
const EXPORT_BUCKET = { bucketType: 's3', bucketName: 'bucket', region: 'us-east-1' } as const;

describe('QUERY_TAG from requestId', () => {
  test('tags query, memory and column types statements with the request UUID', async () => {
    const driver = new TestSnowflakeDriver({});

    await driver.query('SELECT 1', [], { requestId: REQUEST_ID });
    await driver.downloadQueryResults('SELECT 1', [], { highWaterMark: 1, requestId: REQUEST_ID });
    await driver.queryColumnTypes('SELECT 1', [], { requestId: REQUEST_ID });

    expect(driver.statements.map((s) => s.parameters)).toEqual([
      { QUERY_TAG: REQUEST_UUID },
      { QUERY_TAG: REQUEST_UUID },
      { QUERY_TAG: REQUEST_UUID },
    ]);
  });

  test('tags stream statements with the request UUID', async () => {
    const driver = new TestSnowflakeDriver({});

    const result = await driver.stream('SELECT 1', [], { highWaterMark: 1, requestId: REQUEST_ID });
    await result.release?.();

    expect(driver.statements.map((s) => s.parameters)).toEqual([{ QUERY_TAG: REQUEST_UUID }]);
  });

  test('tags every statement of a query unload with the request UUID', async () => {
    const driver = new TestSnowflakeDriver({ exportBucket: EXPORT_BUCKET });

    await driver.unload('schema.table', {
      maxFileSize: 64,
      query: { sql: 'SELECT 1', params: [] },
      requestId: REQUEST_ID,
    });

    expect(driver.statements.map((s) => [s.sqlText.includes('COPY INTO'), s.parameters])).toEqual([
      [false, { QUERY_TAG: REQUEST_UUID }],
      [true, { QUERY_TAG: REQUEST_UUID }],
    ]);
  });

  test('tags every statement of a table unload with the request UUID', async () => {
    const driver = new TestSnowflakeDriver({ exportBucket: EXPORT_BUCKET });

    await driver.unload('schema.table', { maxFileSize: 64, requestId: REQUEST_ID });

    expect(driver.statements.map((s) => [s.sqlText.includes('COPY INTO'), s.parameters])).toEqual([
      [false, { QUERY_TAG: REQUEST_UUID }],
      [true, { QUERY_TAG: REQUEST_UUID }],
    ]);
  });

  test('sends no parameters without a requestId', async () => {
    const driver = new TestSnowflakeDriver({});

    await driver.query('SELECT 1', []);

    expect(driver.statements[0].parameters).toBeUndefined();
  });

  test('leaves a configured queryTag in charge', async () => {
    const driver = new TestSnowflakeDriver({ queryTag: 'static-tag' });

    await driver.query('SELECT 1', [], { requestId: REQUEST_ID });

    expect(driver.statements[0].parameters).toBeUndefined();
  });
});
