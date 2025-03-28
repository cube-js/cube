import { GenericContainer, Wait } from 'testcontainers';

import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';

export class MssqlDbRunner extends DbRunnerAbstract {
  public static startContainer(options: DBRunnerContainerOptions) {
    const version = process.env.TEST_MSSQL_VERSION || options.version || '2019-latest';

    const container = new GenericContainer(`mcr.microsoft.com/mssql/server:${version}`)
      .withEnvironment({
        ACCEPT_EULA: 'Y',
        MSSQL_SA_PASSWORD: process.env.TEST_DB_PASSWORD || 'Test1test',
      })
      .withExposedPorts(1433)
      .withWaitStrategy(Wait.forLogMessage('Service Broker manager has started'))
      .withStartupTimeout(30 * 1000);

    if (options.volumes) {
      const binds = options.volumes.map(v => ({ source: v.source, target: v.target, mode: v.bindMode }));
      container.withBindMounts(binds);
    }

    return container.start();
  }
}
