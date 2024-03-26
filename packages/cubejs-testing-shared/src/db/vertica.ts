import { GenericContainer } from 'testcontainers';
import { LogWaitStrategy } from 'testcontainers/dist/wait-strategy';
import { DbRunnerAbstract } from './db-runner.abstract';

export class VerticaDBRunner extends DbRunnerAbstract {
  public static startContainer() {
    const version = process.env.TEST_VERTICA_VERSION || '11.1.1-0';

    const container = new GenericContainer(`vertica/vertica-ce:${version}`)
      .withEnv('TZ', 'Antarctica/Troll')
      .withEnv('VERTICA_DB_NAME', 'test')
      .withExposedPorts(5433)
      .withStartupTimeout(60 * 1000)
      .withWaitStrategy(new LogWaitStrategy("Node Status: v_test_node0001: (UP)"));

    return container.start();
  }
}
