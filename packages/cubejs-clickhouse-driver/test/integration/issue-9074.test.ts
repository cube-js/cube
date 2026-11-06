import fs from 'fs';
import os from 'os';
import path from 'path';

import { ClickhouseDBRunner } from '@cubejs-backend/testing-shared';

import { ClickHouseDriver } from '../../src';
import type { ClickHouseDriverOptions } from '../../src';

// https://github.com/cube-js/cube/issues/9074
// Non-external pre-aggregation DDL used to fail on both Replicated (TabSeparated DDL status) and Atomic (empty body) databases.
const KEEPER_CONFIG = `<clickhouse>
  <keeper_server>
    <tcp_port>9181</tcp_port>
    <enable_ipv6>false</enable_ipv6>
    <server_id>1</server_id>
    <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
    <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
    <raft_configuration><server><id>1</id><hostname>127.0.0.1</hostname><port>9234</port></server></raft_configuration>
  </keeper_server>
  <zookeeper><node><host>127.0.0.1</host><port>9181</port></node></zookeeper>
  <macros><shard>s1</shard><replica>r1</replica></macros>
</clickhouse>
`;

describe('issue #9074: non-external pre-aggregation build on ClickHouse', () => {
  jest.setTimeout(60 * 1000);

  let container: any;
  let config: ClickHouseDriverOptions;
  let configDir: string;

  const doWithDriver = async <T>(cb: (driver: ClickHouseDriver) => Promise<T>): Promise<T> => {
    const driver = new ClickHouseDriver(config);

    try {
      return await cb(driver);
    } finally {
      await driver.release();
    }
  };

  beforeAll(async () => {
    configDir = fs.mkdtempSync(path.join(os.tmpdir(), 'issue-9074-'));
    fs.writeFileSync(path.join(configDir, 'keeper.xml'), KEEPER_CONFIG);
    fs.chmodSync(configDir, 0o755);
    fs.chmodSync(path.join(configDir, 'keeper.xml'), 0o644);

    container = await ClickhouseDBRunner.startContainer({
      // The runner's 23.11 default only creates Replicated databases behind an experimental setting
      version: '24.8',
      volumes: [{
        source: path.join(configDir, 'keeper.xml'),
        target: '/etc/clickhouse-server/config.d/keeper.xml',
        bindMode: 'ro',
      }],
    });
    config = {
      host: 'localhost',
      port: container.getMappedPort(8123),
    };

    await doWithDriver(async (driver) => {
      await driver.command('CREATE DATABASE repl_preaggs ENGINE = Replicated(\'/clickhouse/databases/repl_preaggs\', \'{shard}\', \'{replica}\')');
      await driver.command('CREATE DATABASE atomic_preaggs');
    });
  });

  afterAll(async () => {
    if (container) {
      await container.stop();
    }
    if (configDir) {
      fs.rmSync(configDir, { recursive: true, force: true });
    }
  });

  // The shape `ClickHouseQuery.preAggregationLoadSql` and `createIndexSql` produce
  const loadSql = (schema: string) => `CREATE TABLE ${schema}.users_props_batch ENGINE = MergeTree() ORDER BY (\`users_props__uid\`) AS SELECT toString(number) AS users_props__uid, number AS amount FROM numbers(10) WHERE number < ?`;
  const indexSql = (schema: string) => `ALTER TABLE ${schema}.users_props_batch ADD INDEX users_props_batch_amount (\`amount\`) TYPE minmax GRANULARITY 1`;

  it.each(['repl_preaggs', 'atomic_preaggs'])('builds and indexes a pre-aggregation in %s', async (schema) => {
    await doWithDriver(async (driver) => {
      await driver.loadPreAggregationIntoTable(`${schema}.users_props_batch`, loadSql(schema), [10], {});
      expect(await driver.query(indexSql(schema), [])).toEqual([]);

      expect(await driver.query(`SELECT count() AS c FROM ${schema}.users_props_batch`, [])).toEqual([{ c: '10' }]);
      expect(await driver.query(
        'SELECT name FROM system.data_skipping_indices WHERE database = ? AND table = ?',
        [schema, 'users_props_batch'],
      )).toEqual([{ name: 'users_props_batch_amount' }]);
    });
  });
});
