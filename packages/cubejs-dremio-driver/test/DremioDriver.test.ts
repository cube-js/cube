import { DriverTests } from '@cubejs-backend/testing-shared';

const DremioDriver = require('../../driver/DremioDriver');

describe('DremioDriver', () => {
  let tests: DriverTests;

  jest.setTimeout(10 * 60 * 1000); // Engine needs to spin up

  beforeAll(async () => {
    tests = new DriverTests(new DremioDriver({}), { expectStringFields: false });
  });

  afterAll(async () => {
    await tests.release();
  });

  test('query', async () => {
    await tests.testQuery();
  });
});
