import { DriverTests } from '@cubejs-backend/testing-shared';

import { FireboltDriver } from '../src';

describe('FireboltDriver autostart', () => {
  let tests: DriverTests;
  let driver: any;
  jest.setTimeout(2 * 60 * 1000);

  afterAll(async () => {
    await tests.release();
  });

  beforeAll(async () => {
    driver = new FireboltDriver({});
    driver.connection = {
      execute: jest.fn().mockRejectedValue({
        status: 404
      })
    };
    driver.ensureEngineRunning = jest.fn();
    tests = new DriverTests(driver, { expectStringFields: true });
  });

  test('calls engine start', async () => {
    try {
      await tests.testQuery();
    } catch (error) {
      expect(driver.ensureEngineRunning).toHaveBeenCalled();
    }
  });
});
