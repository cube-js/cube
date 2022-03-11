import { GenericContainer } from 'testcontainers';

import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';

type QuestStartOptions = DBRunnerContainerOptions & {
  version?: string,
};

export class QuestDBRunner extends DbRunnerAbstract {
  public static startContainer(options: QuestStartOptions) {
    const version = process.env.TEST_QUEST_DB_VERSION || options.version || '6.2.1';

    const container = new GenericContainer(`questdb/questdb:${version}`)
      .withExposedPorts(8812)
      .withStartupTimeout(10 * 1000);

    if (options.volumes) {
      // eslint-disable-next-line no-restricted-syntax
      for (const { source, target, bindMode } of options.volumes) {
        container.withBindMount(source, target, bindMode);
      }
    }

    return container.start();
  }
}
