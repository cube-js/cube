import { DriverTests } from '@cubejs-backend/testing-shared';

import { FireboltDriver } from '../src';

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
});
