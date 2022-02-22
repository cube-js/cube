// eslint-disable-next-line import/no-extraneous-dependencies
import { expect } from '@jest/globals';
import { DriverInterface, PreAggregations } from '@cubejs-backend/query-orchestrator';
import { downloadAndGunzip, streamToArray } from '@cubejs-backend/shared';
import crypto from 'crypto';
import dedent from 'dedent';
import dotenv from '@cubejs-backend/dotenv';
import { Readable } from 'stream';

export interface DriverTestsOptions {
  // Athena driver treats all fields as strings.
  expectStringFields?: boolean
  // Athena does not write csv headers.
  // BigQuery writes csv headers.
  csvNoHeader?: boolean
  // Some drivers unload from a CTAS query, others unload from a stream.
  wrapLoadQueryWithCtas?: boolean
}

export class DriverTests {
  public constructor(
    public readonly driver: DriverInterface,
    private readonly options: DriverTestsOptions = {}
  ) {
  }

  public static config() {
    if ('CUBEJS_TEST_ENV' in process.env) {
      dotenv.config({ path: process.env.CUBEJS_TEST_ENV });
    }
  }

  public release(): Promise<void> {
    return this.driver.release();
  }

  public static QUERY = `
    SELECT id, amount, status
    FROM (
      SELECT 1 AS id, 100 AS amount, 'new' AS status
      UNION ALL
      SELECT 2 AS id, 200 AS amount, 'new' AS status
      UNION ALL
      SELECT 3 AS id, 400 AS amount, 'processed' AS status
    )
    ORDER BY 1
  `;

  public static ROWS = [
    { id: 1, amount: 100, status: 'new' },
    { id: 2, amount: 200, status: 'new' },
    { id: 3, amount: 400, status: 'processed' },
  ];

  public static CSV_ROWS = dedent`
    orders__status,orders__amount
    new,300
    processed,400
  `;

  public async testQuery() {
    const rows = await this.driver.query(DriverTests.QUERY, []);
    const expectedRows = this.options.expectStringFields ? this.rowsToString(DriverTests.ROWS) : DriverTests.ROWS;
    expect(rows).toEqual(expectedRows);
  }

  public async testStream() {
    expect(this.driver.stream).toBeDefined();
    const tableData = await this.driver.stream!(DriverTests.QUERY, [], { highWaterMark: 100 });
    expect(tableData.rowStream instanceof Readable);
    const rows = await streamToArray(tableData.rowStream as Readable);
    const expectedRows = this.options.expectStringFields ? this.rowsToString(DriverTests.ROWS) : DriverTests.ROWS;
    expect(rows).toEqual(expectedRows);
  }

  public async testUnload() {
    expect(this.driver.unload).toBeDefined();
    const versionEntry = {
      table_name: 'test.orders_order_status',
      structure_version: crypto.randomBytes(10).toString('hex'),
      content_version: crypto.randomBytes(10).toString('hex'),
      last_updated_at: new Date().getTime(),
      naming_version: 2
    };
    const tableName = PreAggregations.targetTableName(versionEntry);
    const query = `
      SELECT orders.status AS orders__status, sum(orders.amount) AS orders__amount        
      FROM (${DriverTests.QUERY}) AS orders
      GROUP BY 1
      ORDER BY 1
    `;
    const loadQuery = this.options.wrapLoadQueryWithCtas ? `CREATE TABLE ${tableName} AS ${query}` : query;
    await this.driver.loadPreAggregationIntoTable(
      tableName,
      loadQuery,
      [],
      {
        newVersionEntry: versionEntry,
        targetTableName: tableName,
      }
    );
    const data = await this.driver.unload!(tableName, { maxFileSize: 64 });
    expect(data.csvFile.length).toEqual(1);
    const string = await downloadAndGunzip(data.csvFile[0]);
    const expectedRows = this.options.csvNoHeader
      ? this.skipFirstLine(DriverTests.CSV_ROWS)
      : DriverTests.CSV_ROWS;
    expect(string.trim()).toEqual(expectedRows);
  }

  private skipFirstLine(text: string): string {
    return text.split('\n').slice(1).join('\n');
  }

  private rowsToString(rows: Record<string, any>[]): Record<string, string>[] {
    const result: Record<string, string>[] = [];
    for (const row of rows) {
      const newRow: Record<string, string> = {};
      for (const k of Object.keys(row)) {
        newRow[k] = row[k].toString();
      }
      result.push(newRow);
    }
    return result;
  }
}
