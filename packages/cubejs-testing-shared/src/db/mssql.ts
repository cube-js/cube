import { GenericContainer, Wait } from 'testcontainers';

import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';

type MssqlStartOptions = DBRunnerContainerOptions & {
    version?: string,
};

export class MssqlDbRunner extends DbRunnerAbstract {
  public static startContainer(options: MssqlStartOptions) {
    const version = process.env.TEST_MSSQL_VERSION || options.version || '2017-latest';

    const container = new GenericContainer(`mcr.microsoft.com/mssql/server:${version}`)
      .withEnv('ACCEPT_EULA', 'Y')
      .withEnv('MSSQL_SA_PASSWORD', process.env.TEST_DB_PASSWORD || 'Test1test')
      .withExposedPorts(1433)
      .withWaitStrategy(Wait.forLogMessage('Service Broker manager has started'))
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
