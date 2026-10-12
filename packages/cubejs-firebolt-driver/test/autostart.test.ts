import { assertDataSource, getEnv } from '@cubejs-backend/shared';
import { DriverTests } from '@cubejs-backend/testing-shared';

import { EngineStatusSummary, Firebolt } from 'firebolt-sdk';
import { version } from 'firebolt-sdk/package.json';
import { FireboltDriver } from '../src';

// STOP ENGINE returns while the engine is still stopping, and a START ENGINE
// issued before it has fully stopped fails, so wait for a settled status.
async function waitWhileTransitioning(engine: {
  refreshStatus(): Promise<void>;
  current_status_summary: EngineStatusSummary;
}) {
  for (let i = 0; i < 60; i++) {
    await engine.refreshStatus();
    if (![EngineStatusSummary.STOPPING, EngineStatusSummary.DRAINING].includes(engine.current_status_summary)) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 5000));
  }
  throw new Error(`Engine is still ${engine.current_status_summary} after 5 minutes`);
}

describe('FireboltDriver autostart', () => {
  let tests: DriverTests;
  let driver: any;
  jest.setTimeout(10 * 60 * 1000);

  afterAll(async () => {
    await tests.release();
  });

  beforeAll(async () => {
    driver = new FireboltDriver({});
    driver.connection = {
      execute: jest.fn().mockRejectedValue({
        status: 404
      }),
      destroy: jest.fn(),
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
  test('starts the engine after connection', async () => {
    const dataSource = assertDataSource('default');

    const username = getEnv('dbUser', { dataSource });
    const auth = username.includes('@')
      ? { username, password: getEnv('dbPass', { dataSource }) }
      : { client_id: username, client_secret: getEnv('dbPass', { dataSource }) };
    const engineName = getEnv('fireboltEngineName', { dataSource });
    const firebolt = Firebolt({
      apiEndpoint: getEnv('fireboltApiEndpoint', { dataSource }) || 'api.app.firebolt.io',
    });
    await firebolt.connect({
      auth,
      database: getEnv('dbName', { dataSource }),
      account: getEnv('fireboltAccount', { dataSource }),
      engineEndpoint: getEnv('fireboltEngineEndpoint', { dataSource }),
      additionalParameters: {
        userClients: [{
          name: 'CubeDev+Cube',
          version
        }]
      },
    });

    const engine = await firebolt.resourceManager.engine.getByName(engineName);

    try {
      await engine.stop();
      await waitWhileTransitioning(engine);

      driver = new FireboltDriver({});
      await driver.testConnection();
    } finally {
      await waitWhileTransitioning(engine);
      await engine.start();
    }
  });
});
