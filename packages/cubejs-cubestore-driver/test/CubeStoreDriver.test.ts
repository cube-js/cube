import { DriverTests } from '@cubejs-backend/testing/dist/src/testing';

import { CubeStoreDriver } from '../src';
import { CubeStoreDBRunner } from '@cubejs-backend/testing';

DriverTests.config();

describe('CubeStoreDriver', () => {
  let tests: DriverTests;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    const container = await CubeStoreDBRunner.startContainer({});
    tests = new DriverTests(
      new CubeStoreDriver({
        host: container.getHost(),
        port: container.getMappedPort(3030)
      }),
      {
        expectStringFields: true
      }
    );
  });

  afterAll(async () => {
    await tests.release();
  });

  test('query', async () => {
    await tests.testQuery();
  });
});
