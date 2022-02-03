// eslint-disable-next-line import/no-extraneous-dependencies
import { expect } from '@jest/globals';
import { DriverInterface, PreAggregations } from '@cubejs-backend/query-orchestrator';
import { downloadAndGunzip, streamToArray } from '@cubejs-backend/shared';
import crypto from 'crypto';
import dedent from 'dedent';
import dotenv from '@cubejs-backend/dotenv';
import { BaseQuery } from "@cubejs-backend/schema-compiler";

export interface DriverTestsOptions {
  // Athena driver treats all fields as strings.
  expectStringFields?: boolean
  // Athena does not write csv headers.
  // BigQuery writes csv headers.
  expectCsvHeader?: boolean
  // Similar to BaseQuery.preAggregationLoadSql, but only wrapping the sql without also generating the sql from a cube.
  // Tradeoff betweeen code duplication and minimization of the amount of input data required to write a test.
  // TODO(cristipp) Figure out how to create a simple cube and simply delegate to BaseQuery.preAggregationLoadSql.
  preAggregationWrapLoadSql?: (tableName: string, query: string) => string
}

export class DriverTests {
  public constructor(
    private readonly driver: DriverInterface,
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

  public readonly QUERY = `
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

  public readonly ROWS = [
    { id: 1, amount: 100, status: 'new' },
    { id: 2, amount: 200, status: 'new' },
    { id: 3, amount: 400, status: 'processed' },
  ];

  public async testQuery() {
    const rows = await this.driver.query(this.QUERY, []);
    if (this.options.expectStringFields) {
      expect(rows).toEqual(this.rowsToString(this.ROWS));
    } else {
      expect(rows).toEqual(this.ROWS);
    }
  }

  public async testStream() {
    expect(this.driver.stream).toBeDefined();
    const tableData = await this.driver.stream!(this.QUERY, [], { highWaterMark: 100 });
    const rows = await streamToArray(tableData.rowStream);
    if (this.options.expectStringFields) {
      expect(rows).toEqual(this.rowsToString(this.ROWS));
    } else {
      expect(rows).toEqual(this.ROWS);
    }
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
      FROM (${this.QUERY}) AS orders
      GROUP BY 1
      ORDER BY 1
    `;
    const loadQuery = (this.options.preAggregationWrapLoadSql ?? ((t: string, q: string) => q))(tableName, query);
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
    if (this.options.expectCsvHeader) {
      expect(string.trim()).toEqual(dedent`
        orders__status,orders__amount
        new,300
        processed,400
      `);
    } else {
      expect(string.trim()).toEqual(dedent`
        new,300
        processed,400
      `);
    }
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
