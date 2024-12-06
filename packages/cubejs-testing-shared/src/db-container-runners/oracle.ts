import { GenericContainer, Wait } from 'testcontainers';

import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';

export class OracleDBRunner extends DbRunnerAbstract {
  public static startContainer(options: DBRunnerContainerOptions) {
    const version = process.env.TEST_ORACLE_VERSION || options.version || '23.4.0';

    const container = new GenericContainer(`gvenzl/oracle-free:${version}`)
      .withEnvironment({
        ORACLE_PASSWORD: 'test'
      })
      .withWaitStrategy(Wait.forLogMessage('DATABASE IS READY TO USE'))
      .withExposedPorts(1521);

    if (options.volumes) {
      const binds = options.volumes.map(v => ({ source: v.source, target: v.target, mode: v.bindMode }));
      container.withBindMounts(binds);
    }

    return container.start();
  }
}
