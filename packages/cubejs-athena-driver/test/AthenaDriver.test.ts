import { DriverTests } from '@cubejs-backend/testing/dist/src/testing';

import { AthenaDriver } from '../src';

DriverTests.config();

describe('AthenaDriver', () => {
  let tests: DriverTests;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    tests = new DriverTests(
      new AthenaDriver({}),
      {
        expectStringFields: true,
        skipHeader: true,
        wrapLoadQueryWithCtas: true,
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

  test('unload', async () => {
    await tests.testUnload();
  });
});
