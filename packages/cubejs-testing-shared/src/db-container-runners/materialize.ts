import { GenericContainer, Wait } from 'testcontainers';

import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';
import { startContainerWithRetry } from './start-with-retry';

export class MaterializeDBRunner extends DbRunnerAbstract {
  public static startContainer(options: DBRunnerContainerOptions) {
    const version = process.env.TEST_MZSQL_VERSION || options.version || 'v0.88.0';

    const container = new GenericContainer(`materialize/materialized:${version}`)
      .withExposedPorts(6875)
      // The image boots CockroachDB and then environmentd before it serves SQL
      .withWaitStrategy(Wait.forSuccessfulCommand('psql -h localhost -p 6875 -U materialize -d materialize -c "SELECT 1"'))
      .withStartupTimeout(60 * 1000);

    if (options.volumes) {
      const binds = options.volumes.map(v => ({ source: v.source, target: v.target, mode: v.bindMode }));
      container.withBindMounts(binds);
    }

    return startContainerWithRetry(container);
  }
}
