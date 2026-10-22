// https://github.com/cube-js/cube/issues/10315
// With multiple data sources and a driverFactory that returns driver instances,
// the dialect of a non-default data source must not fall back to the global
// CUBEJS_DB_TYPE. Today every data source gets the default one, so a Postgres
// data source receives MySQL SQL (backticks, CONVERT_TZ, DATE_FORMAT).
import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import type { DriverContext, ServerCoreInitializedOptions } from '../../src/core/types';
import { CubejsServerCore } from '../../src/core/server';
import { CreateOptions } from '../../src/core/types';

class CubejsServerCoreExposed extends CubejsServerCore {
  public declare options: ServerCoreInitializedOptions;

  public constructor(opts: CreateOptions = {}) {
    super({ ...opts, telemetry: false });
  }

  public startScheduledRefreshTimer() {
    return null;
  }
}

class MockDriver extends BaseDriver {
  public constructor(public readonly name: string) {
    super();
  }

  public async testConnection() {
    // nothing
  }

  public async query() {
    return [];
  }
}

describe('dbType with driver instances and multiple data sources (#10315)', () => {
  afterEach(() => {
    delete process.env.CUBEJS_DB_TYPE;
    delete process.env.CUBEJS_DATASOURCES;
    delete process.env.CUBEJS_DS_POSTGRES_DB_TYPE;
  });

  test('uses the decorated CUBEJS_DS_<NAME>_DB_TYPE for a non-default data source', async () => {
    process.env.CUBEJS_DB_TYPE = 'mysql';
    process.env.CUBEJS_DATASOURCES = 'default,postgres';
    process.env.CUBEJS_DS_POSTGRES_DB_TYPE = 'postgres';

    const core = new CubejsServerCoreExposed({
      apiSecret: 'testApiSecretToSuppressWarning',
      logger: () => undefined,
      driverFactory: ({ dataSource }) => new MockDriver(dataSource),
    });

    expect(await core.options.dbType({ dataSource: 'default' } as DriverContext)).toEqual('mysql');
    expect(await core.options.dbType({ dataSource: 'postgres' } as DriverContext)).toEqual('postgres');
  });
});
