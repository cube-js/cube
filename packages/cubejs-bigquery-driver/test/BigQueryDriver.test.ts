import { DriverTests } from '@cubejs-backend/testing';

import { BigQueryDriver } from '../src';

DriverTests.config();

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
    tests.testQuery();
  });

  test('stream', async () => {
    tests.testStream();
  });

  test('unload', async () => {
    tests.testUnload();
  });
});
