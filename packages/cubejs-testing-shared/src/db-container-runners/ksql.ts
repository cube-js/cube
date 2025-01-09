import fetch from 'node-fetch';
import { GenericContainer, StartedTestContainer } from 'testcontainers';
import { pausePromise } from '@cubejs-backend/shared';
import { DbRunnerAbstract, DBRunnerContainerOptions } from './db-runner.abstract';

export class KsqlDBRunner extends DbRunnerAbstract {
  public static startContainer(options: DBRunnerContainerOptions) {
    const version = process.env.TEST_KSQL_VERSION || options.version || '7.6.0';

    const bootstrapServers = 'kafka:9092';
    const container = new GenericContainer(`confluentinc/cp-ksqldb-server:${version}`)
      .withEnvironment({
        KSQL_BOOTSTRAP_SERVERS: bootstrapServers,
        KSQL_KSQL_STREAMS_BOOTSTRAP_SERVERS: bootstrapServers,
        KSQL_KSQL_SERVICE_ID: 'service-id',
      })
      .withExposedPorts(8088)
      .withStartupTimeout(30 * 1000);

    if (options.network) {
      container.withNetwork(options.network);
      container.withNetworkAliases('ksql');
    }

    if (options.volumes) {
      const binds = options.volumes.map(v => ({ source: v.source, target: v.target, mode: v.bindMode }));
      container.withBindMounts(binds);
    }

    return container.start();
  }

  public static async loadData(db: StartedTestContainer) {
    const ksqlUrl = `http://${db.getHost()}:${db.getMappedPort(8088)}`;

    let attempts = 0;
    while (attempts < 10) {
      const res = await fetch(`${ksqlUrl}/ksql`, {
        method: 'POST',
        headers: { Accept: 'application/json' },
        body: JSON.stringify({
          ksql: 'LIST STREAMS;',
          streamsProperties: {}
        })
      });

      const body = await res.json();
      if (body.message !== 'KSQL is not yet ready to serve requests.') {
        console.log('KSQL ready');
        break;
      }
      console.log('KSQL not ready yet');
      attempts++;

      await pausePromise(300);
    }

    const resCreateStream = await fetch(`${ksqlUrl}/ksql`, {
      method: 'POST',
      headers: { Accept: 'application/json' },
      body: JSON.stringify({
        ksql: 'CREATE OR REPLACE STREAM REQUESTS (ID STRING, TIMESTAMP TIMESTAMP, TENANT_ID INTEGER, REQUEST_ID STRING) WITH (KAFKA_TOPIC = \'REQUESTS\', KEY_FORMAT = \'JSON\', PARTITIONS = 1, REPLICAS = 1, VALUE_FORMAT = \'JSON\');',
        streamsProperties: {}
      })
    });

    console.log('KSQL CREATE STREAM', await resCreateStream.json());

    const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toJSON();
    const today = new Date(Date.now() - 1000).toJSON();
    const resInsertYesterday = await fetch(`${ksqlUrl}/ksql`, {
      method: 'POST',
      headers: { Accept: 'application/json' },
      body: JSON.stringify({
        ksql: `INSERT INTO REQUESTS VALUES ('1', '${yesterday}', 1, 'req-stream-1');INSERT INTO REQUESTS VALUES ('1', '${today}', 1, 'req-stream-2');`,
        streamsProperties: {}
      })
    });

    console.log('KSQL INSERT', await resInsertYesterday.json());
  }
}
