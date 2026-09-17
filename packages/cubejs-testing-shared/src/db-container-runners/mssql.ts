import { GenericContainer, Wait } from 'testcontainers';

import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';
import { startContainerWithRetry } from './start-with-retry';

export class MssqlDbRunner extends DbRunnerAbstract {
  public static startContainer(options: DBRunnerContainerOptions) {
    const version = process.env.TEST_MSSQL_VERSION || options.version || '2019-latest';
    const password = process.env.TEST_DB_PASSWORD || 'Test1test';

    const container = new GenericContainer(`mcr.microsoft.com/mssql/server:${version}`)
      .withEnvironment({
        ACCEPT_EULA: 'Y',
        MSSQL_SA_PASSWORD: password,
      })
      .withExposedPorts(1433)
      .withWaitStrategy(Wait.forSuccessfulCommand(
        `/opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "${password}" -Q "SELECT 1" -b -o /dev/null`
      ))
      .withStartupTimeout(120 * 1000);

    if (options.volumes) {
      const binds = options.volumes.map(v => ({ source: v.source, target: v.target, mode: v.bindMode }));
      container.withBindMounts(binds);
    }

    return startContainerWithRetry(container);
  }
}
