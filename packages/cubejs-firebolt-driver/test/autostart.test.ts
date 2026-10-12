import { assertDataSource, getEnv } from '@cubejs-backend/shared';
import { DriverTests } from '@cubejs-backend/testing-shared';

import { EngineStatusSummary, Firebolt } from 'firebolt-sdk';
import { version } from 'firebolt-sdk/package.json';
import { FireboltDriver } from '../src';

// STOP ENGINE returns while the engine is still stopping, and a START ENGINE
// issued before it has fully stopped fails, so wait for a settled status.
const STOP_WAIT_INTERVAL_MS = 5000;
const STOP_WAIT_ATTEMPTS = 60;
const STOP_WAIT_MAX_MS = STOP_WAIT_INTERVAL_MS * STOP_WAIT_ATTEMPTS;

async function waitWhileTransitioning(getStatus: () => Promise<EngineStatusSummary>) {
  let status: EngineStatusSummary | undefined;
  for (let i = 0; i < STOP_WAIT_ATTEMPTS; i++) {
    status = await getStatus();
    if (![EngineStatusSummary.STOPPING, EngineStatusSummary.DRAINING].includes(status)) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, STOP_WAIT_INTERVAL_MS));
  }
  throw new Error(`Engine is still ${status} after ${STOP_WAIT_MAX_MS / 1000} s`);
}

describe('FireboltDriver autostart', () => {
  let tests: DriverTests;
  let driver: any;
  // Two waits for the engine to stop, plus the driver's 2 min connection timeout and slack.
  jest.setTimeout(2 * STOP_WAIT_MAX_MS + 5 * 60 * 1000);

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
    const getStatus = async () => (await firebolt.resourceManager.engine.getByName(engineName)).current_status_summary;

    try {
      await engine.stop();
      await waitWhileTransitioning(getStatus);

      driver = new FireboltDriver({});
      await driver.testConnection();
    } finally {
      await waitWhileTransitioning(getStatus);
      await engine.startAndWait();
    }
  });
});
