import { GenericContainer } from 'testcontainers';

import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';

export class QuestDBRunner extends DbRunnerAbstract {
  public static startContainer(options: DBRunnerContainerOptions) {
    const version = process.env.TEST_QUEST_DB_VERSION || options.version || '8.0.3';

    const container = new GenericContainer(`questdb/questdb:${version}`)
      .withExposedPorts(8812)
      .withStartupTimeout(10 * 1000);

    if (options.volumes) {
      const binds = options.volumes.map(v => ({ source: v.source, target: v.target, mode: v.bindMode }));
      container.withBindMounts(binds);
    }

    return container.start();
  }
}
