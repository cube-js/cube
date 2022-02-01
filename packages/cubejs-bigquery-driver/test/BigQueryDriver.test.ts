import { streamToArray } from "@cubejs-backend/shared";
import dotenv from '@cubejs-backend/dotenv';
import { BigQueryDriver } from '../src';

dotenv.config({ path: '/Users/cristipp/.env' });

const QUERY = `
  SELECT 1 AS id, 100 AS amount, 'new' AS status
  UNION ALL
  SELECT 2 AS id, 200 AS amount, 'new' AS status
  UNION ALL
  SELECT 3 AS id, 300 AS amount, 'processed' AS status
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
      { id: 3, amount: 300, status: 'processed' },
    ]);
  });

  test('stream', async () => {
    const tableData = await driver.stream(QUERY, []);
    expect(await streamToArray(tableData.rowStream)).toEqual([
      { id: 1, amount: 100, status: 'new' },
      { id: 2, amount: 200, status: 'new' },
      { id: 3, amount: 300, status: 'processed' },
    ]);
  });

  test('unload', async () => {
    const tableData = await driver.stream(QUERY, []);
    expect(await streamToArray(tableData.rowStream)).toEqual([
      { id: 1, amount: 100, status: 'new' },
      { id: 2, amount: 200, status: 'new' },
      { id: 3, amount: 300, status: 'processed' },
    ]);
  });

  // test('stream (exception)', async () => {
  //   try {
  //     await driver.stream('select * from test.random_name_for_table_that_doesnot_exist_sql_must_fail', []);
  //
  //     throw new Error('stream must throw an exception');
  //   } catch (e) {
  //     expect(e.message).toEqual(
  //       'relation "test.random_name_for_table_that_doesnot_exist_sql_must_fail" does not exist'
  //     );
  //   }
  // });
});
