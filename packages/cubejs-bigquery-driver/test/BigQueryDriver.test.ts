import { streamToArray } from "@cubejs-backend/shared";
import dotenv from '@cubejs-backend/dotenv';
import { PreAggregations } from "@cubejs-backend/query-orchestrator";
import dedent from 'dedent';
import fetch from 'node-fetch';
import { v4 } from 'uuid';
import { gunzipSync } from 'zlib';

import { BigQueryDriver } from '../src';

dotenv.config({ path: '/Users/cristipp/.env' });

const QUERY = `
  SELECT 1 AS id, 100 AS amount, 'new' AS status
  UNION ALL
  SELECT 2 AS id, 200 AS amount, 'new' AS status
  UNION ALL
  SELECT 3 AS id, 400 AS amount, 'processed' AS status
`;

describe('BigQueryDriver', () => {
  let driver: BigQueryDriver;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    driver = new BigQueryDriver({});
  });

  afterAll(async () => {
    await driver.release();
  });

  test('query', async () => {
    const data = await driver.query(QUERY, []);
    expect(data).toEqual([
      { id: 1, amount: 100, status: 'new' },
      { id: 2, amount: 200, status: 'new' },
      { id: 3, amount: 400, status: 'processed' },
    ]);
  });

  test('stream', async () => {
    const tableData = await driver.stream(QUERY, []);
    expect(await streamToArray(tableData.rowStream)).toEqual([
      { id: 1, amount: 100, status: 'new' },
      { id: 2, amount: 200, status: 'new' },
      { id: 3, amount: 400, status: 'processed' },
    ]);
  });

  test('unload', async () => {
    const versionEntry = {
      table_name: 'test_pre_aggregations.orders_order_status',
      structure_version: v4(),
      content_version: v4(),
      last_updated_at: 160000000000,
      naming_version: 2
    };
    const tableName = PreAggregations.targetTableName(versionEntry);
    await driver.loadPreAggregationIntoTable(
      tableName,
      `
        SELECT orders.status AS orders__status, sum(orders.amount) AS orders__amount        
        FROM (${QUERY}) AS orders
        GROUP BY 1
        ORDER BY 1
      `,
      [],
      {
        newVersionEntry: versionEntry,
        targetTableName: tableName,
      }
    );
    const data = await driver.unload(tableName);
    expect(data.csvFile.length).toEqual(1);
    const response = await fetch(data.csvFile[0]);
    const gz = await response.arrayBuffer();
    const bytes = await gunzipSync(gz);
    const string = bytes.toString();
    expect(string.trim()).toEqual(dedent`
      orders__status,orders__amount
      new,300
      processed,400
    `);
  });
});
