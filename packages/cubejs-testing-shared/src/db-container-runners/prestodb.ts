import { GenericContainer, Wait } from 'testcontainers';

import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';

export class PrestoDbRunner extends DbRunnerAbstract {
  public static startContainer(options: DBRunnerContainerOptions) {
    const version = process.env.TEST_PRESTO_VERSION || options.version || '0.281';

    const container = new GenericContainer(`ahanaio/prestodb-sandbox:${version}`)
      .withExposedPorts(8080)
      .withWaitStrategy(Wait.forLogMessage('======== SERVER STARTED ========'))
      .withStartupTimeout(30 * 1000);

    if (options.volumes) {
      const binds = options.volumes.map(v => ({ source: v.source, target: v.target, mode: v.bindMode }));
      container.withBindMounts(binds);
    }

    return container.start();
  }
}
