import fetch from 'node-fetch';
import { execSync } from 'child_process';
import { GenericContainer, StartedTestContainer } from 'testcontainers';
import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';

export class PostgresDBRunner extends DbRunnerAbstract {
  public static startContainer(options: DBRunnerContainerOptions) {
    const version = process.env.TEST_PGSQL_VERSION || options.version || '12.22';

    const container = new GenericContainer(`postgres:${version}`)
      .withEnvironment({
        POSTGRES_USER: 'test',
        POSTGRES_DB: 'test',
        POSTGRES_PASSWORD: 'test',
      })
      .withExposedPorts(5432)
      // .withHealthCheck({
      //   test: 'pg_isready -U root -d model_test',
      //   interval: 2 * 1000,
      //   timeout: 500,
      //   retries: 3
      // })
      // .withWaitStrategy(Wait.forHealthCheck())
      // Postgresql do fast shutdown on start for db applying
      .withStartupTimeout(10 * 1000);

    if (options.volumes) {
      const binds = options.volumes.map(v => ({ source: v.source, target: v.target, mode: v.bindMode }));
      container.withBindMounts(binds);
    }

    return container.start();
  }

  public static async loadEcom(db: StartedTestContainer) {
    const ecom = await (await fetch('https://cube.dev/downloads/ecom-dump-d3-example.sql')).text();
    execSync(`psql postgresql://test:test@${db.getHost()}:${db.getMappedPort(5432)}/test`, { input: ecom });
  }
}
