import { GenericContainer, Wait } from 'testcontainers';

import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';

const DEFAULT_VERSION = '5.10.16';

export class CrateDBRunner extends DbRunnerAbstract {
  public static startContainer(options: DBRunnerContainerOptions) {
    const version = process.env.TEST_CRATE_DB_VERSION || DEFAULT_VERSION;

    const container = new GenericContainer(`crate/crate:${version}`)
      .withExposedPorts(5432)
      .withWaitStrategy(Wait.forLogMessage('started'))
      .withStartupTimeout(30 * 1000);

    if (process.platform === 'darwin' && process.arch === 'arm64' && version === DEFAULT_VERSION) {
      container.withPlatform('linux/amd64');
    }

    if (options.volumes) {
      const binds = options.volumes.map(v => ({ source: v.source, target: v.target, mode: v.bindMode }));
      container.withBindMounts(binds);
    }

    return container.start();
  }
}
