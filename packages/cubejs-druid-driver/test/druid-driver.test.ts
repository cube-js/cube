// eslint-disable-next-line import/no-extraneous-dependencies
import { DockerComposeEnvironment, StartedDockerComposeEnvironment, Wait } from 'testcontainers';
// eslint-disable-next-line import/no-extraneous-dependencies
import path from 'path';

import { DruidDriver, DruidDriverConfiguration } from '../src/DruidDriver';

describe('DruidDriver', () => {
  let env: StartedDockerComposeEnvironment|null = null;
  let config: DruidDriverConfiguration;

  const doWithDriver = async (callback: (driver: DruidDriver) => Promise<any>) => {
    const driver = new DruidDriver(config);

    await callback(driver);
  };

  // eslint-disable-next-line consistent-return
  beforeAll(async () => {
    if (process.env.TEST_DRUID_HOST) {
      const host = process.env.TEST_DRUID_HOST || 'localhost';
      const port = process.env.TEST_DRUID_PORT || '8888';

      config = {
        url: `http://${host}:${port}`,
      };

      return;
    }

    const dc = new DockerComposeEnvironment(
      path.resolve(path.dirname(__filename), '../../'),
      'docker-compose.yml'
    );

    env = await dc
      .withWaitStrategy('zookeeper', Wait.forLogMessage('binding to port /0.0.0.0:2181'))
      .withWaitStrategy('postgres', Wait.forHealthCheck())
      .withWaitStrategy('router', Wait.forHealthCheck())
      .withWaitStrategy('middlemanager', Wait.forHealthCheck())
      .withWaitStrategy('historical', Wait.forHealthCheck())
      .withWaitStrategy('broker', Wait.forHealthCheck())
      .withWaitStrategy('coordinator', Wait.forHealthCheck())
      .up();

    const host = env.getContainer('router').getHost();
    const port = env.getContainer('router').getMappedPort(8888);

    config = {
      user: 'admin',
      password: 'password1',
      url: `http://${host}:${port}`,
    };
  }, 2 * 60 * 1000);

  // eslint-disable-next-line consistent-return
  afterAll(async () => {
    if (env) {
      await env.down();
    }
  }, 30 * 1000);

  it('should construct', async () => {
    jest.setTimeout(10 * 1000);

    return doWithDriver(async () => {
      //
    });
  });

  it('should test connection', async () => {
    jest.setTimeout(10 * 1000);

    return doWithDriver(async (driver) => {
      await driver.testConnection();
    });
  });

  it('SELECT 1', async () => {
    jest.setTimeout(10 * 1000);

    return doWithDriver(async (driver) => {
      expect(await driver.query('SELECT 1')).toEqual([{
        EXPR$0: 1,
      }]);
    });
  });
});
