import { assertDataSource, getEnv } from '@cubejs-backend/shared';
import { DriverTests } from '@cubejs-backend/testing-shared';

import { Firebolt, ConnectionOptions } from 'firebolt-sdk';
import { version } from 'firebolt-sdk/package.json';
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
    if (!username) {
      throw new Error('username is required for Firebolt');
    }

    const password = getEnv('dbPass', { dataSource });
    if (!password) {
      throw new Error('password is required for Firebolt');
    }

    const auth: ConnectionOptions['auth'] = username.includes('@')
      ? { username, password }
      : { client_id: username, client_secret: password };
    const engineName = getEnv('fireboltEngineName', { dataSource });
    if (!engineName) {
      throw new Error('engineName is required for Firebolt');
    }
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

      driver = new FireboltDriver({});
      await driver.testConnection();
    } finally {
      await engine.start();
    }
  });
});
