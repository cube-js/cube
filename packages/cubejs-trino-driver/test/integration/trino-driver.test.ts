import { TrinoDriver } from '../../src/TrinoDriver';

const path = require('path');
const { DockerComposeEnvironment, Wait } = require('testcontainers');

describe('TrinoDriver', () => {
  jest.setTimeout(6 * 60 * 1000);

  let env: any;
  let config: any;

  const doWithDriver = async (callback: any) => {
    const driver = new TrinoDriver(config);

    await callback(driver);
  };

  // eslint-disable-next-line consistent-return,func-names
  beforeAll(async () => {
    const authOpts = {
      user: 'presto',
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
      path.resolve(path.dirname(__filename), '../../../'),
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
  afterAll(async () => {
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
  it('should test connection', async () => {
    await doWithDriver(async (driver: any) => {
      await driver.testConnection();
    });
  });

  // eslint-disable-next-line func-names
  it('should test informationSchemaQuery', async () => {
    await doWithDriver(async (driver: any) => {
      const informationSchemaQuery = driver.informationSchemaQuery();
      expect(informationSchemaQuery).toContain('columns.table_schema = \'sf1\'');
    });
  });

  it('should send a configured session time zone', async () => {
    const driver = new TrinoDriver({ ...config, timezone: 'America/New_York' });

    expect(await driver.query('SELECT current_timezone() AS tz', [])).toEqual([{ tz: 'America/New_York' }]);
  });
});
