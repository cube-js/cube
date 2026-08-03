// eslint-disable-next-line import/no-extraneous-dependencies
import { Wait, DockerComposeEnvironment, StartedDockerComposeEnvironment } from 'testcontainers';
import { MongoBIDriver, } from '../src';

describe('MongoBiDriver', () => {
  let driver: MongoBIDriver;
  let environment: StartedDockerComposeEnvironment;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    const MONGO_TAG = process.env.TEST_MONGO_TAG || '6.0';
    const MONGOBI_VERSION = process.env.TEST_MONGOBI_VERSION || 'mongodb-bi-linux-x86_64-ubuntu2004-v2.14.8';

    environment = await new DockerComposeEnvironment('./test', 'docker-compose.yml')
      .withEnvironment({ MONGO_TAG, MONGOBI_VERSION })
      .withWaitStrategy('mongosqld', Wait.forLogMessage('obtained initial schema'))
      .up();

    const container = environment.getContainer('mongosqld');

    driver = new MongoBIDriver({
      host: container.getHost(),
      port: container.getMappedPort(3307),
      waitForConnections: true,
      database: 'test',
      dataSource: 'default',
      maxPoolSize: 1,
      testConnectionTimeout: 10,
    });
  });

  afterAll(async () => {
    await driver.release();
    await environment.down();
  });

  test('should test connection', async () => {
    await driver.testConnection();
  });

  test('should select raw sql', async () => {
    const result = await driver.query(`
      SELECT number
      FROM mycol
      LIMIT 1
    `, []);
    expect(result).toEqual([{ number: 1 }]);
  });
});
