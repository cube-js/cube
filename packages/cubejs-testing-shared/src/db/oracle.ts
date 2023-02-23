import { GenericContainer, Wait } from 'testcontainers';
import { isCI } from '@cubejs-backend/shared';

import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';

type OracleStartOptions = DBRunnerContainerOptions & {
  version?: string,
};

export class OracleDBRunner extends DbRunnerAbstract {
  public static startContainer(options: OracleStartOptions) {
    const version = process.env.TEST_ORACLE_VERSION || options.version || '21.3.0';

    const container = new GenericContainer(`gvenzl/oracle-xe:${version}`)
      .withEnv('ORACLE_PASSWORD', 'test')
      .withHealthCheck({
        test: 'healthcheck.sh',
        interval: 2 * 1000,
        timeout: 5 * 1000,
        retries: 5,
        startPeriod: (isCI() ? 45 : 15) * 1000
      })
      .withWaitStrategy(Wait.forHealthCheck())
      .withExposedPorts(1521);

    if (options.volumes) {
      // eslint-disable-next-line no-restricted-syntax
      for (const { source, target, bindMode } of options.volumes) {
        container.withBindMount(source, target, bindMode);
      }
    }

    return container.start();
  }
}
