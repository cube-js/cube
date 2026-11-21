import fs from 'fs';
import os from 'os';
import path from 'path';

import { ClickhouseDBRunner } from '@cubejs-backend/testing-shared';

import { ClickHouseDriver } from '../src';
import type { ClickHouseDriverOptions } from '../src';

// https://github.com/cube-js/cube/issues/9074
// Non-external (`original_sql`) pre-aggregations are built in ClickHouse with
// `CREATE TABLE ... ENGINE = MergeTree() ORDER BY (...) AS SELECT ...`, executed through
// BaseDriver.loadPreAggregationIntoTable -> ClickHouseDriver.query(), which asks for JSONCompact
// output. DDL has no result set: on a Replicated database ClickHouse answers with the distributed
// DDL status as TabSeparated, on an Atomic database with an empty body. Both are rejected after
// ClickHouse has already created the table.
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

describe('issue #9074: original_sql pre-aggregation build on ClickHouse', () => {
  jest.setTimeout(60 * 1000);

  let container: any;
  let config: ClickHouseDriverOptions;
  let configDir: string;

  const doWithDriver = async (cb: (driver: ClickHouseDriver) => Promise<any>) => {
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
      // Replicated databases are no longer experimental since 24.x
      version: '25.3',
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

  const loadSql = (schema: string) => `CREATE TABLE ${schema}.users_props_batch ENGINE = MergeTree() ORDER BY (\`users_props__uid\`) AS SELECT toString(number) AS users_props__uid, number AS amount FROM numbers(10)`;

  it('builds an original_sql pre-aggregation in a Replicated database', async () => {
    await doWithDriver(async (driver) => {
      await driver.loadPreAggregationIntoTable('repl_preaggs.users_props_batch', loadSql('repl_preaggs'), [], {});
      expect(await driver.query('SELECT count() AS c FROM repl_preaggs.users_props_batch', [])).toEqual([{ c: '10' }]);
    });
  });

  it('builds an original_sql pre-aggregation in an Atomic database', async () => {
    await doWithDriver(async (driver) => {
      await driver.loadPreAggregationIntoTable('atomic_preaggs.users_props_batch', loadSql('atomic_preaggs'), [], {});
      expect(await driver.query('SELECT count() AS c FROM atomic_preaggs.users_props_batch', [])).toEqual([{ c: '10' }]);
    });
  });
});
