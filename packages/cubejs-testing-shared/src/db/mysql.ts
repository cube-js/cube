import { GenericContainer, Wait } from 'testcontainers';

import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';

type MySQLStartOptions = DBRunnerContainerOptions & {
  version?: string,
};

export class MysqlDBRunner extends DbRunnerAbstract {
  public static startContainer(options: MySQLStartOptions) {
    const version = process.env.TEST_MYSQL_VERSION || options.version || '5.7';

    const builder = new GenericContainer(`mysql:${version}`)
      .withEnv('MYSQL_ROOT_PASSWORD', process.env.TEST_DB_PASSWORD || 'Test1test')
      .withHealthCheck({
        test: 'mysqladmin ping -h localhost',
        interval: 5 * 1000,
        timeout: 2 * 1000,
        retries: 3,
        startPeriod: 10 * 1000,
      })
      .withWaitStrategy(Wait.forHealthCheck())
      .withExposedPorts(3306);

    if (version.split('.')[0] === '8') {
      /**
       * workaround for MySQL 8 and unsupported auth in mysql package
       * @link https://github.com/mysqljs/mysql/pull/2233
       */
      builder.withCmd(['--default-authentication-plugin=mysql_native_password']);
    }

    if (options.volumes) {
      // eslint-disable-next-line no-restricted-syntax
      for (const { source, target, bindMode } of options.volumes) {
        builder.withBindMount(source, target, bindMode);
      }
    }

    return builder.start();
  }
}
