import { GenericContainer, Wait } from 'testcontainers';

import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';

type TrinoStartOptions = DBRunnerContainerOptions & {
    version?: string,
};

export class TrinoDBRunner extends DbRunnerAbstract {
  public static startContainer(options: TrinoStartOptions) {
    const version = process.env.TEST_TRINO_VERSION || options.version || '403';

    const container = new GenericContainer(`trinodb/trino:${version}`)
      .withExposedPorts(8080)
      .withWaitStrategy(Wait.forLogMessage('======== SERVER STARTED ========'))
      .withStartupTimeout(30 * 1000);

    if (options.volumes) {
      // eslint-disable-next-line no-restricted-syntax
      for (const { source, target, bindMode } of options.volumes) {
        container.withBindMount(source, target, bindMode);
      }
    }

    return container.start();
  }
}
