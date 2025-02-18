// eslint-disable-next-line import/no-extraneous-dependencies
import { DriverTests } from '@cubejs-backend/testing-shared';
import { streamToArray } from '@cubejs-backend/shared';

import { BigQueryDriver } from '../src';

describe('BigQueryDriver', () => {
  let tests: DriverTests;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    tests = new DriverTests(new BigQueryDriver({}));
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

  test('unload', async () => {
    await tests.testUnload();
  });

  test('unload CSV escape symbol', async () => {
    await tests.testUnloadEscapeSymbolOp1(BigQueryDriver);
    await tests.testUnloadEscapeSymbolOp2(BigQueryDriver);
    await tests.testUnloadEscapeSymbolOp3(BigQueryDriver);
  });

  const QUERY_TO_TEST_HYDRATION = `
      SELECT CAST(1 as NUMERIC) as numeric
      UNION ALL
      SELECT CAST(255.44 as NUMERIC);
  `;

  function assertHydrationResults(tableData: any) {
    expect(tableData).toEqual([
      {
        numeric: '1',
      },
      {
        numeric: '255.44'
      }
    ]);
  }

  test('query hydration', async () => {
    const driver = new BigQueryDriver({});

    const tableData = await driver.query(QUERY_TO_TEST_HYDRATION, []);
    assertHydrationResults(tableData);
  });

  test('stream hydration', async () => {
    const driver = new BigQueryDriver({});

    const tableData = await driver.stream(QUERY_TO_TEST_HYDRATION, []);

    const result = await streamToArray(tableData.rowStream as any);
    assertHydrationResults(result);
  });
});
