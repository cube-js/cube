import { GenericContainer } from 'testcontainers';

import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';

export class CubeStoreDBRunner extends DbRunnerAbstract {
  public static startContainer(options: DBRunnerContainerOptions) {
    const version = process.env.TEST_CUBESTORE_VERSION || options.version || 'latest';

    const container = new GenericContainer(`cubejs/cubestore:${version}`)
      .withStartupTimeout(10 * 1000)
      .withExposedPorts(3030);

    if (options.volumes) {
      const binds = options.volumes.map(v => ({ source: v.source, target: v.target, mode: v.bindMode }));
      container.withBindMounts(binds);
    }

    return container.start();
  }
}
