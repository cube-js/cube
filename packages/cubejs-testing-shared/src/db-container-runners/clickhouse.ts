import { GenericContainer } from 'testcontainers';

import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';

export class ClickhouseDBRunner extends DbRunnerAbstract {
  public static startContainer(options: DBRunnerContainerOptions) {
    const version = process.env.TEST_CLICKHOUSE_VERSION || options.version || '23.11';

    const container = new GenericContainer(`clickhouse/clickhouse-server:${version}`)
      .withExposedPorts(8123)
      .withStartupTimeout(10 * 1000);

    if (options.volumes) {
      const binds = options.volumes.map(v => ({ source: v.source, target: v.target, mode: v.bindMode }));
      container.withBindMounts(binds);
    }

    return container.start();
  }
}
