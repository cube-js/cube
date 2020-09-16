/* globals describe, before, after, it */
// eslint-disable-next-line import/no-extraneous-dependencies
import { DockerComposeEnvironment, StartedDockerComposeEnvironment, Wait } from 'testcontainers';
import path from 'path';
import { DruidDriver } from '../src';

// eslint-disable-next-line import/no-extraneous-dependencies
require('should');

describe('DruidDriver', () => {
  let env: StartedDockerComposeEnvironment|null = null;
  let config: any = {};

  const doWithDriver = async (callback: (driver: DruidDriver) => Promise<any>) => {
    const driver = new DruidDriver({
      user: 'test',
    });

    await callback(driver);
  };

  // eslint-disable-next-line consistent-return
  before(async function before() {
    this.timeout(20000);

    if (process.env.TEST_DRUID_HOST) {
      return {
        host: 'localhost',
        port: process.env.TEST_DRUID_HOST,
      };
    }

    const dc = new DockerComposeEnvironment(
      path.resolve(__dirname, '../../'),
      'docker-compose.yml'
    );

    env = await dc
      .withWaitStrategy('router', Wait.forLogMessage('Successfully started lifecycle'))
      .up();

    config = {
      host: 'localhost',
      port: env.getContainer('router').getMappedPort(8888),
    };
  });

  after(async () => {
    if (env) {
      await env.down();
    }
  });

  it('should construct', async () => {
    await doWithDriver(async () => {});
  });

  it('should test connection', async () => {
    await doWithDriver(async (driver) => {
      await driver.testConnection();
    });
  });
});
