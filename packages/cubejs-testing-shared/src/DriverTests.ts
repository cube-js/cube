import { DriverInterface, PreAggregations } from '@cubejs-backend/query-orchestrator';
import { downloadAndGunzip, streamToArray } from '@cubejs-backend/shared';
import crypto from 'crypto';
import dedent from 'dedent';
import { Readable } from 'stream';
import assert from 'assert';

export interface DriverTestsOptions {
  // Athena driver treats all fields as strings.
  expectStringFields?: boolean
  // Athena does not write csv headers.
  // BigQuery writes csv headers.
  csvNoHeader?: boolean
  // Some drivers unload from a CTAS query, others unload from a stream.
  wrapLoadQueryWithCtas?: boolean
  delimiter?: string
}

export class DriverTests {
  public constructor(
    public readonly driver: DriverInterface,
    private readonly options: DriverTestsOptions = {}
  ) {
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
      UNION ALL
      SELECT 4 AS id, 500 AS amount, NULL AS status
    ) AS data
    ORDER BY 1
  `;

  public static ROWS = [
    { id: 1, amount: 100, status: 'new' },
    { id: 2, amount: 200, status: 'new' },
    { id: 3, amount: 400, status: 'processed' },
    { id: 4, amount: 500, status: null },
  ];

  protected getExpectedCsvRows() {
    return dedent`
      orders__status,orders__amount
      new,300
      processed,400
      ,500
    `;
  }

  public async testQuery() {
    const rows = await this.driver.query(DriverTests.QUERY, []);
    const expectedRows = this.options.expectStringFields ? DriverTests.rowsToString(DriverTests.ROWS) : DriverTests.ROWS;
    expect(rows).toEqual(expectedRows);
  }

  public async testStream() {
    expect(this.driver.stream).toBeDefined();
    const tableData = await this.driver.stream!(DriverTests.QUERY, [], { highWaterMark: 100 });
    expect(tableData.rowStream instanceof Readable);
    const rows = await streamToArray(tableData.rowStream as Readable);
    const expectedRows = this.options.expectStringFields ? DriverTests.rowsToString(DriverTests.ROWS) : DriverTests.ROWS;
    expect(rows).toEqual(expectedRows);
  }

  // We might use the tableName to build the unload SQL for some drivers
  protected unloadOptions(tableName: string) {
    return { maxFileSize: 64 };
  }

  public async testUnload() {
    const query = `
      SELECT orders.status AS orders__status, sum(orders.amount) AS orders__amount        
      FROM (${DriverTests.QUERY}) AS orders
      GROUP BY 1
      ORDER BY 2
    `;
    const tableName = await this.createUnloadTable(query);
    assert(this.driver.unload);
    const data = await this.driver.unload(tableName, this.unloadOptions(tableName));
    expect(data.csvFile.length).toEqual(1);
    // The order of the rows can be changed by some drivers
    const string = (await downloadAndGunzip(data.csvFile[0])).split('\n').sort().join('\n');
    let expectedCsvRows = this.getExpectedCsvRows();
    if (this.options.delimiter) {
      expectedCsvRows = expectedCsvRows.replaceAll(/,/g, this.options.delimiter);
    }
    const expectedRows = this.options.csvNoHeader
      ? DriverTests.skipFirstLine(expectedCsvRows)
      : expectedCsvRows;
    expect(string.trim()).toEqual(expectedRows);
  }

  public async testUnloadEscapeSymbolOp1(Driver: any) {
    process.env.CUBEJS_DB_EXPORT_BUCKET_CSV_ESCAPE_SYMBOL = '"';
    const driver = new Driver({}) as DriverInterface;
    const query = `
      SELECT orders.status AS orders__status, sum(orders.amount) AS orders__amount        
      FROM (${DriverTests.QUERY}) AS orders
      GROUP BY 1
      ORDER BY 1
    `;
    const tableName = await this.createUnloadTable(query);
    assert(driver.unload);
    const data = await driver.unload(tableName, { maxFileSize: 64 });
    expect(data.exportBucketCsvEscapeSymbol).toBe('"');
    await driver.release();
  }

  public async testUnloadEscapeSymbolOp2(Driver: any) {
    process.env.CUBEJS_DB_EXPORT_BUCKET_CSV_ESCAPE_SYMBOL = '\'';
    const driver = new Driver({}) as DriverInterface;
    const query = `
      SELECT orders.status AS orders__status, sum(orders.amount) AS orders__amount        
      FROM (${DriverTests.QUERY}) AS orders
      GROUP BY 1
      ORDER BY 1
    `;
    const tableName = await this.createUnloadTable(query);
    assert(driver.unload);
    const data = await driver.unload(tableName, { maxFileSize: 64 });
    expect(data.exportBucketCsvEscapeSymbol).toBe('\'');
    await driver.release();
  }

  public async testUnloadEscapeSymbolOp3(Driver: any) {
    delete process.env.CUBEJS_DB_EXPORT_BUCKET_CSV_ESCAPE_SYMBOL;
    const driver = new Driver({}) as DriverInterface;
    const query = `
      SELECT orders.status AS orders__status, sum(orders.amount) AS orders__amount        
      FROM (${DriverTests.QUERY}) AS orders
      GROUP BY 1
      ORDER BY 1
    `;
    const tableName = await this.createUnloadTable(query);
    assert(driver.unload);
    const data = await driver.unload(tableName, { maxFileSize: 64 });
    expect(data.exportBucketCsvEscapeSymbol).toBeUndefined();
    await driver.release();
  }

  public async testUnloadEmpty() {
    const query = `
      SELECT 'new' AS orders__status, 100 AS orders__amount  
      WHERE FALSE  
    `;
    const tableName = await this.createUnloadTable(query);
    assert(this.driver.unload);
    const data = await this.driver.unload(tableName, this.unloadOptions(tableName));
    expect(data.csvFile.length).toEqual(0);
  }

  // The table name format can be slightly different for some drivers, so we might need to adjust it
  protected unloadTableEntryName() {
    return 'test.orders_order_status';
  }

  // The table name format can be slightly different for some drivers, so we might need to adjust it
  protected ctasUnloadTableNameTransform(tableName: string) {
    return tableName;
  }

  private async createUnloadTable(query: string): Promise<string> {
    const versionEntry = {
      table_name: this.unloadTableEntryName(),
      structure_version: crypto.randomBytes(10).toString('hex'),
      content_version: crypto.randomBytes(10).toString('hex'),
      last_updated_at: new Date().getTime(),
      naming_version: 2
    };
    const tableName = PreAggregations.targetTableName(versionEntry);
    const loadQuery = this.options.wrapLoadQueryWithCtas 
      ? `CREATE TABLE ${this.ctasUnloadTableNameTransform(tableName)} AS ${query}`
      : query;
    await this.driver.loadPreAggregationIntoTable(
      tableName,
      loadQuery,
      [],
      {
        newVersionEntry: versionEntry,
        targetTableName: tableName,
      }
    );
    return tableName;
  }

  private static skipFirstLine(text: string): string {
    return text.split('\n').slice(1).join('\n');
  }

  private static rowsToString(rows: Record<string, any>[]): Record<string, string | null>[] {
    const result: Record<string, string | null>[] = [];

    for (const row of rows) {
      const newRow: Record<string, string> = {};
      for (const k of Object.keys(row)) {
        newRow[k] = row[k] === null ? null : row[k].toString();
      }
      result.push(newRow);
    }

    return result;
  }
}
