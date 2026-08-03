import { KafkaContainer } from '@testcontainers/kafka';
import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';

export class KafkaDBRunner extends DbRunnerAbstract {
  public static startContainer(options: DBRunnerContainerOptions) {
    const version = process.env.TEST_KAFKA_VERSION || options.version || '7.6.0';

    const container = new KafkaContainer(`confluentinc/cp-kafka:${version}`)
      .withKraft()
      .withEnvironment({
        KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1',
        KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
        KAFKA_NUM_PARTITIONS: '1',
        KAFKA_DEFAULT_REPLICATION_FACTOR: '1',
      })
      .withExposedPorts(9093)
      .withStartupTimeout(10 * 1000);
    
    if (options.network) {
      container.withNetwork(options.network);
      container.withNetworkAliases('kafka');
    }

    if (options.volumes) {
      const binds = options.volumes.map(v => ({ source: v.source, target: v.target, mode: v.bindMode }));
      container.withBindMounts(binds);
    }

    return container.start();
  }
}
