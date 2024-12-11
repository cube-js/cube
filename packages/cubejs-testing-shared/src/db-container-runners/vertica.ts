import { GenericContainer, Wait } from 'testcontainers';
import { DbRunnerAbstract } from './db-runner.abstract';

export class VerticaDBRunner extends DbRunnerAbstract {
  public static startContainer() {
    const version = process.env.TEST_VERTICA_VERSION || '11.1.1-0';

    const container = new GenericContainer(`vertica/vertica-ce:${version}`)
      .withEnvironment({ TZ: 'Antarctica/Troll', VERTICA_DB_NAME: 'test' })
      .withExposedPorts(5433)
      .withStartupTimeout(60 * 1000)
      .withWaitStrategy(
        Wait.forLogMessage('Node Status: v_test_node0001: (UP)')
      );

    return container.start();
  }
}
