import { DriverTests } from '@cubejs-backend/testing-shared';

import { FireboltDriver } from '../src';
import { getEnv } from '@cubejs-backend/shared';

describe('FireboltDriver', () => {
  let tests: DriverTests;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    tests = new DriverTests(new FireboltDriver({}), { expectStringFields: true });
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

  test('query should fail on timeout', async () => {
    const slowQuery = 'SELECT checksum(*) FROM GENERATE_SERIES(1, 1000000000000)';
    // Set query timeout to 2 seconds
    process.env.CUBEJS_DB_QUERY_TIMEOUT = '2';
    const driver = new FireboltDriver({});

    await expect(driver.query(slowQuery)).rejects.toThrow(/timeout expired \(2000 ms\)/i);
  }, 10000);
});
