/* globals describe, before, after, it */
const path = require('path');
const { DockerComposeEnvironment, Wait } = require('testcontainers');

const PrestoDriver = require('../driver/PrestoDriver');

require('should');

describe('PrestoHouseDriver', () => {
  let env;
  let config;

  const doWithDriver = async (callback) => {
    const driver = new PrestoDriver(config);

    await callback(driver);
  };

  // eslint-disable-next-line consistent-return,func-names
  before(async function () {
    this.timeout(6 * 60 * 1000);

    const authOpts = {
      basic_auth: {
        user: 'presto',
        password: ''
      }
    };

    if (process.env.TEST_PRESTO_HOST) {
      config = {
        host: process.env.TEST_PRESTO_HOST || 'localhost',
        port: process.env.TEST_PRESTO_PORT || '8080',
        catalog: process.env.TEST_PRESTO_CATALOG || 'tpch',
        schema: 'sf1',
        ...authOpts
      };

      return;
    }

    const dc = new DockerComposeEnvironment(
      path.resolve(path.dirname(__filename), '../'),
      'docker-compose.yml'
    );

    env = await dc
      .withStartupTimeout(240 * 1000)
      .withWaitStrategy('coordinator', Wait.forHealthCheck())
      .up();

    config = {
      host: env.getContainer('coordinator').getHost(),
      port: env.getContainer('coordinator').getMappedPort(8080),
      catalog: 'tpch',
      schema: 'sf1',
      ...authOpts
    };
  });

  // eslint-disable-next-line consistent-return,func-names
  after(async function () {
    this.timeout(30 * 1000);

    if (env) {
      await env.down();
    }
  });

  it('should construct', async () => {
    await doWithDriver(() => {
      //
    });
  });

  // eslint-disable-next-line func-names
  it('should test connection', async function () {
    // Presto can be slow after starting...
    this.timeout(10 * 1000);

    await doWithDriver(async (driver) => {
      await driver.testConnection();
    });
  });
});
