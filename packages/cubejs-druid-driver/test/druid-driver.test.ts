// eslint-disable-next-line import/no-extraneous-dependencies
import { DockerComposeEnvironment, StartedDockerComposeEnvironment, Wait } from 'testcontainers';
// eslint-disable-next-line import/no-extraneous-dependencies
import { Duration, TemporalUnit } from 'node-duration';
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
    jest.setTimeout(2 * 60 * 1000);

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
      // https://github.com/testcontainers/testcontainers-node/issues/109
      .withStartupTimeout(new Duration(90, TemporalUnit.SECONDS))
      .withWaitStrategy('zookeeper', Wait.forLogMessage('binding to port /0.0.0.0:2181'))
      .withWaitStrategy('postgres', Wait.forHealthCheck())
      .withWaitStrategy('router', Wait.forHealthCheck())
      .up();

    const host = env.getContainer('router').getContainerIpAddress();
    const port = env.getContainer('router').getMappedPort(8888);

    config = {
      url: `http://${host}:${port}`,
    };
  });

  // eslint-disable-next-line consistent-return
  afterAll(async () => {
    jest.setTimeout(30 * 1000);

    if (env) {
      await env.down();
    }
  });

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
});
