import { GenericContainer } from 'testcontainers';

import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';

export class CrateDBRunner extends DbRunnerAbstract {
  public static startContainer(options: DBRunnerContainerOptions) {
    const version = process.env.TEST_CRATE_DB_VERSION || '5.0.0';

    const container = new GenericContainer(`crate/crate:${version}`)
      .withExposedPorts(5432)
      .withStartupTimeout(10 * 1000);

    if (options.volumes) {
      const binds = options.volumes.map(v => ({ source: v.source, target: v.target, mode: v.bindMode }));
      container.withBindMounts(binds);
    }

    return container.start();
  }
}
