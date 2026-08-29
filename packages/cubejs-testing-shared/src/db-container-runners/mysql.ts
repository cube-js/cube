import { GenericContainer, Wait } from 'testcontainers';

import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';

export class MysqlDBRunner extends DbRunnerAbstract {
  public static startContainer(options: DBRunnerContainerOptions) {
    const version = process.env.TEST_MYSQL_VERSION || options.version || '8.0';

    const container = new GenericContainer(`mysql:${version}`)
      .withEnvironment({
        MYSQL_ROOT_PASSWORD: process.env.TEST_DB_PASSWORD || 'Test1test',
      })
      // On a fresh volume MySQL initializes the data directory before it starts
      // listening, which on a loaded CI runner takes well over the 20s the old
      // budget (10s start period + 3 retries * 5s) allowed - Docker then marks
      // the container unhealthy and testcontainers gives up at once, regardless
      // of the startup timeout. Probes that fail inside the start period do not
      // count against `retries`, and the container is reported healthy as soon
      // as one passes, so a generous start period costs nothing when MySQL comes
      // up quickly and only buys time when it does not.
      .withHealthCheck({
        test: ['CMD-SHELL', 'mysqladmin ping -h localhost'],
        interval: 5 * 1000,
        timeout: 5 * 1000,
        retries: 3,
        startPeriod: 60 * 1000,
      })
      .withWaitStrategy(Wait.forHealthCheck())
      // Must outlast the health check budget above, otherwise testcontainers'
      // 60s default would cut the wait short before Docker has given up.
      .withStartupTimeout(120 * 1000)
      .withExposedPorts(3306);

    if (options.volumes) {
      const binds = options.volumes.map(v => ({ source: v.source, target: v.target, mode: v.bindMode }));
      container.withBindMounts(binds);
    }

    return container.start();
  }
}
