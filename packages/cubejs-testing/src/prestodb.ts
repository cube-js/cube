import { DockerComposeEnvironment, Wait } from 'testcontainers';
import { Duration, TemporalUnit } from 'node-duration';
import path = require('path');

interface TestInstance<T = any> {
  configuration: T,
  query: (query: string, value: any[]) => Promise<any>,
  /**
   * Some databases are proxies to the real database, this method provides querying to db exclude proxy
   *
   * @param query
   * @param value
   */
  queryStore: (query: string, value: any[]) => Promise<any>,
  stop: () => Promise<unknown>,
  getQuery: () => BaseQuery,
}

// eslint-disable-next-line import/prefer-default-export
export async function getPrestoTestInstance(): Promise<TestInstance> {
  if (process.env.TEST_PRESTO_HOST) {
    return {
      configuration: {
        host: process.env.TEST_PRESTO_HOST || 'localhost',
        port: process.env.TEST_PRESTO_PORT || '8080',
        catalog: process.env.TEST_PRESTO_CATALOG || 'postgresql',
        schema: 'presto',
      },
      query: async (query: string, value: any[]) => {},
      queryStore: async (query: string, value: any[]) => {},
      stop: async () => {},
    };
  }

  const dc = new DockerComposeEnvironment(
    path.resolve(path.dirname(__filename), '../configuration/'),
    'docker-compose-presto.yml'
  );

  const env = await dc
    .withStartupTimeout(new Duration(90, TemporalUnit.SECONDS))
    .withWaitStrategy('coordinator', Wait.forHealthCheck())
    .withWaitStrategy('worker0', Wait.forLogMessage('Added catalog postgresql'))
    .withWaitStrategy('postgres', Wait.forHealthCheck())
    .up();

  return {
    configuration: {
      host: env.getContainer('coordinator').getContainerIpAddress(),
      port: env.getContainer('coordinator').getMappedPort(8080),
      catalog: 'postgresql',
      schema: 'default'
    },
    query: async (query: string, value: any[]) => {},
    queryStore: async (query: string, value: any[]) => {},
    stop: () => env.down(),
  };
}

module.exports = {
  getPrestoTestInstance
};
