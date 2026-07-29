import { GenericContainer, Wait } from 'testcontainers';

import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';

export class MysqlDBRunner extends DbRunnerAbstract {
  public static startContainer(options: DBRunnerContainerOptions) {
    const version = process.env.TEST_MYSQL_VERSION || options.version || '8.0';

    const container = new GenericContainer(`mysql:${version}`)
      .withEnvironment({
        MYSQL_ROOT_PASSWORD: process.env.TEST_DB_PASSWORD || 'Test1test',
      })
      .withHealthCheck({
        test: ['CMD-SHELL', 'mysqladmin ping -h localhost'],
        interval: 5 * 1000,
        timeout: 2 * 1000,
        retries: 3,
        startPeriod: 10 * 1000,
      })
      .withWaitStrategy(Wait.forHealthCheck())
      .withExposedPorts(3306);

    if (options.volumes) {
      const binds = options.volumes.map(v => ({ source: v.source, target: v.target, mode: v.bindMode }));
      container.withBindMounts(binds);
    }

    return container.start();
  }
}
