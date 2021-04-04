import { GenericContainer } from 'testcontainers';
import { DbRunnerAbstract } from './db-runner.abstract';

interface PostgresStartOptions {
  version?: string,
}

export class PostgresDBRunner extends DbRunnerAbstract {
  public static startContainer(options: PostgresStartOptions) {
    const version = process.env.TEST_PGSQL_VERSION || options.version || '9.6.8';

    return new GenericContainer(`postgres:${version}`)
      .withEnv('POSTGRES_USER', 'root')
      .withEnv('POSTGRES_DB', 'model_test')
      .withEnv('POSTGRES_PASSWORD', 'test')
      .withExposedPorts(5432)
      // .withHealthCheck({
      //   test: 'pg_isready -U root -d model_test',
      //   interval: 2 * 1000,
      //   timeout: 500,
      //   retries: 3
      // })
      // .withWaitStrategy(Wait.forHealthCheck())
      // Postgresql do fast shutdown on start for db applying
      .withStartupTimeout(10 * 1000)
      .start();
  }
}
