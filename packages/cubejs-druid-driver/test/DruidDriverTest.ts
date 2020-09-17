/* globals describe, before, after, it */

// eslint-disable-next-line import/no-extraneous-dependencies
import { DockerComposeEnvironment, StartedDockerComposeEnvironment, Wait } from 'testcontainers';
// eslint-disable-next-line import/no-extraneous-dependencies
import { Duration, TemporalUnit } from 'node-duration';
import path from 'path';

import { DruidDriver } from '../src';
import { DruidClientConfiguration } from '../src/DruidClient';

// eslint-disable-next-line import/no-extraneous-dependencies
require('should');

describe('DruidDriver', () => {
  let env: StartedDockerComposeEnvironment|null = null;
  let config: Partial<DruidClientConfiguration> = {};

  const doWithDriver = async (callback: (driver: DruidDriver) => Promise<any>) => {
    const driver = new DruidDriver(config);

    await callback(driver);
  };

  // eslint-disable-next-line consistent-return
  before(async function before(done) {
    this.timeout(2 * 60 * 1000);

    try {
      if (process.env.TEST_DRUID_HOST) {
        return {
          host: 'localhost',
          port: process.env.TEST_DRUID_HOST,
        };
      }

      const dc = new DockerComposeEnvironment(
        path.resolve(path.dirname(__filename), '../../'),
        'docker-compose.yml'
      );

      env = await dc
        .withWaitStrategy('postgres', Wait.forHealthCheck())
        .withWaitStrategy('router', Wait.forHealthCheck())
        .up();

      config = {
        host: 'localhost',
        port: env.getContainer('router').getMappedPort(8888),
      };

      done();
    } catch (e) {
      done(e);
    }
  });

  // eslint-disable-next-line consistent-return
  after(async function(done) {
    this.timeout(30 * 1000);

    try {
      if (env) {
        await env.down();
      }

      done();
    } catch (e) {
      done(e);
    }
  });

  it('should construct', async () => {
    // @ts-ignore
    this.timeout(10 * 1000);

    return doWithDriver(async () => {});
  });

  it('should test connection', async () => {
    // @ts-ignore
    this.timeout(10 * 1000);

    return doWithDriver(async (driver) => {
      await driver.testConnection();
    });
  });
});
