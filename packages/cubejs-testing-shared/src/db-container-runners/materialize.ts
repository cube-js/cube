import { GenericContainer } from 'testcontainers';

import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';

export class MaterializeDBRunner extends DbRunnerAbstract {
  public static startContainer(options: DBRunnerContainerOptions) {
    const version = process.env.TEST_MZSQL_VERSION || options.version || 'v0.88.0';

    const container = new GenericContainer(`materialize/materialized:${version}`)
      .withExposedPorts(6875)
      // Postgresql do fast shutdown on start for db applying
      .withStartupTimeout(10 * 1000);

    if (options.volumes) {
      const binds = options.volumes.map(v => ({ source: v.source, target: v.target, mode: v.bindMode }));
      container.withBindMounts(binds);
    }

    return container.start();
  }
}
