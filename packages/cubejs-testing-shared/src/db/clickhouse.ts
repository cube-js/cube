import { GenericContainer, Wait } from 'testcontainers';

import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';

type ClickhouseStartOptions = DBRunnerContainerOptions & {
    version?: string,
};

export class ClickhouseDBRunner extends DbRunnerAbstract {
  public static startContainer(options: ClickhouseStartOptions) {
    const version = process.env.TEST_CLICKHOUSE_VERSION || options.version || '23.11';

    const container = new GenericContainer(`clickhouse/clickhouse-server:${version}`)
      .withExposedPorts(8123)
      .withStartupTimeout(10 * 1000);

    if (options.volumes) {
      // eslint-disable-next-line no-restricted-syntax
      for (const { source, target, bindMode } of options.volumes) {
        container.withBindMount(source, target, bindMode);
      }
    }

    return container.start();
  }
}
