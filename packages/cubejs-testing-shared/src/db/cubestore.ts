import { GenericContainer } from 'testcontainers';

import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';

type CubeStoreStartOptions = DBRunnerContainerOptions & {
  version?: string,
};

export class CubeStoreDBRunner extends DbRunnerAbstract {
  public static startContainer(options: CubeStoreStartOptions) {
    const version = process.env.TEST_CUBESTORE_VERSION || options.version || 'latest';

    const builder = new GenericContainer(`cubejs/cubestore:${version}`)
      .withStartupTimeout(10 * 1000)
      .withExposedPorts(3030);

    if (options.volumes) {
      // eslint-disable-next-line no-restricted-syntax
      for (const { source, target, bindMode } of options.volumes) {
        builder.withBindMount(source, target, bindMode);
      }
    }

    return builder.start();
  }
}
