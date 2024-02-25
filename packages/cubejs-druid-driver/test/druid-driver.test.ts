// eslint-disable-next-line import/no-extraneous-dependencies
import { DockerComposeEnvironment, StartedDockerComposeEnvironment, Wait } from 'testcontainers';
// eslint-disable-next-line import/no-extraneous-dependencies
import path from 'path';

import { DruidDriver, DruidDriverConfiguration } from '../src/DruidDriver';

describe('DruidDriver', () => {
  let env: StartedDockerComposeEnvironment | null = null;
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
        user: 'admin',
        password: 'password1',
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

  it('downloadQueryResults', async () => {
    jest.setTimeout(10 * 1000);

    return doWithDriver(async (driver) => {
      const result = await driver.downloadQueryResults(
        'SELECT 1 as id, true as finished, \'netherlands\' as country, CAST(\'2020-01-01T01:01:01.111Z\' as timestamp) as created UNION ALL SELECT 2 as id, false as finished, \'spain\' as country, CAST(\'2020-01-01T01:01:01.111Z\' as timestamp) as created',
        [],
        { highWaterMark: 1 }
      );
      expect(result).toEqual({
        rows: [
          { country: 'netherlands', created: '2020-01-01T01:01:01.111Z', finished: true, id: 1 },
          { country: 'spain', created: '2020-01-01T01:01:01.111Z', finished: false, id: 2 }
        ],
        types: [
          { name: 'id', type: 'int' },
          { name: 'finished', type: 'boolean' },
          { name: 'country', type: 'text' },
          { name: 'created', type: 'timestamp' }
        ]
      });
    });
  });
});
