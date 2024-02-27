import { GenericContainer } from 'testcontainers';

import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';

type MaterializeStartOptions = DBRunnerContainerOptions & {
  version?: string,
};

export class MaterializeDBRunner extends DbRunnerAbstract {
  public static startContainer(options: MaterializeStartOptions) {
    const version = process.env.TEST_MZSQL_VERSION || options.version || 'v0.88.0';

    const container = new GenericContainer(`materialize/materialized:${version}`)
      .withExposedPorts(6875)
      // Postgresql do fast shutdown on start for db applying
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
