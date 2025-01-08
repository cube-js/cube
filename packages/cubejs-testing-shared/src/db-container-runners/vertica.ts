import { GenericContainer, Wait } from 'testcontainers';
import { DbRunnerAbstract } from './db-runner.abstract';

export class VerticaDBRunner extends DbRunnerAbstract {
  public static startContainer() {
    const version = process.env.TEST_VERTICA_VERSION || '12.0.4-0';

    const container = new GenericContainer(`vertica/vertica-ce:${version}`)
      .withEnvironment({ TZ: 'Antarctica/Troll', VERTICA_DB_NAME: 'test', VMART_ETL_SCRIPT: '', VMART_ETL_SQL: '' })
      .withExposedPorts(5433)
      .withStartupTimeout(60 * 1000)
      .withWaitStrategy(
        Wait.forLogMessage('Vertica is now running')
      );

    return container.start();
  }
}
