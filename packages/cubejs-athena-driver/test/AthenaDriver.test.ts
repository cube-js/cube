import { DriverTests } from '@cubejs-backend/testing-shared';

import { AthenaDriver } from '../src';

describe('AthenaDriver', () => {
  let tests: DriverTests;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    tests = new DriverTests(
      new AthenaDriver({}),
      {
        expectStringFields: true,
        csvNoHeader: true,
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
