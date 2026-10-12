import { MySqlDriver } from '@cubejs-backend/mysql-driver';
import { PostgresDriver } from '@cubejs-backend/postgres-driver';

import { CubejsServerCore } from '../../src';

// https://github.com/cube-js/cube/issues/10315
// Multi-datasource setup: `default` is MySQL, `postgres` is PostgreSQL. When
// `driverFactory` returns driver *instances*, `OptsHandler.getDbType()` resolves the
// dbType of every data source to the raw `process.env.CUBEJS_DB_TYPE` (here `mysql`),
// ignoring the decorated `CUBEJS_DS_POSTGRES_DB_TYPE` suggested as a workaround on the
// issue, so SQL for the `postgres` cube is generated with the MySQL dialect.
describe('issue #10315: dialect of a non-default data source with driver instances', () => {
  const envKeys = ['CUBEJS_DB_TYPE', 'CUBEJS_DATASOURCES', 'CUBEJS_DS_POSTGRES_DB_TYPE'];
  const savedEnv: Record<string, string | undefined> = {};

  const model = `cubes:
  - name: pg_orders
    sql_table: public.orders
    data_source: postgres
    measures:
      - name: count
        type: count
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: created_at
        sql: created_at
        type: time
`;

  const query: any = {
    measures: ['pg_orders.count'],
    timeDimensions: [{
      dimension: 'pg_orders.created_at',
      granularity: 'month',
      dateRange: ['2024-01-01', '2024-12-31'],
    }],
    timezone: 'Europe/Berlin',
  };

  const drivers: Array<{ release: () => Promise<void> }> = [];

  beforeEach(() => {
    for (const key of envKeys) {
      savedEnv[key] = process.env[key];
    }
    process.env.CUBEJS_DB_TYPE = 'mysql';
    process.env.CUBEJS_DATASOURCES = 'default,postgres';
    process.env.CUBEJS_DS_POSTGRES_DB_TYPE = 'postgres';
  });

  afterEach(async () => {
    for (const key of envKeys) {
      if (savedEnv[key] === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = savedEnv[key];
      }
    }
    await Promise.all(drivers.splice(0).map(d => d.release()));
  });

  async function sqlFor(driverFactory: (ctx: { dataSource: string }) => any) {
    const core = new CubejsServerCore(<any>{
      apiSecret: 'secret',
      devServer: false,
      driverFactory,
      repositoryFactory: () => ({
        localPath: () => __dirname,
        dataSchemaFiles: async () => [{ fileName: 'pg_orders.yml', content: model }],
      }),
    });

    try {
      const compilerApi = await core.getCompilerApi(<any>{ securityContext: {}, authInfo: {} });
      const [sql] = (await compilerApi.getSql(query)).sql;
      return sql as string;
    } finally {
      await core.releaseConnections();
    }
  }

  test('driverFactory returning driver instances: postgres cube gets Postgres SQL', async () => {
    const sql = await sqlFor(({ dataSource }) => {
      const driver = dataSource === 'postgres'
        ? new PostgresDriver({ host: 'localhost', database: 'db', user: 'u', password: 'p' })
        : new MySqlDriver({ host: 'localhost', database: 'db', user: 'u', password: 'p' });
      drivers.push(driver);
      return driver;
    });

    expect(sql).not.toMatch(/`/);
    expect(sql).not.toMatch(/CONVERT_TZ|DATE_FORMAT|@@session/i);
    expect(sql).toMatch(/AT TIME ZONE/);
  });

  // Control: the config-object form handled by resolveDriver() already works
  test('driverFactory returning driver configs: postgres cube gets Postgres SQL', async () => {
    const sql = await sqlFor(({ dataSource }) => (
      dataSource === 'postgres' ? { type: 'postgres' } : { type: 'mysql' }
    ));

    expect(sql).not.toMatch(/`/);
    expect(sql).not.toMatch(/CONVERT_TZ|DATE_FORMAT|@@session/i);
    expect(sql).toMatch(/AT TIME ZONE/);
  });
});
