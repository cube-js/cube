import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import { SchemaFileRepository, testLogger } from '@cubejs-backend/shared';
import { CubejsServerCore, CompilerApi, RefreshScheduler } from '../../src';

// https://github.com/cube-js/cube/issues/9132
// In a multi-tenant deployment one tenant's data source lacked a column used by
// the time dimension of a partitioned pre-aggregation. Its build range query fails,
// and from then on the refresh worker never builds pre-aggregations scheduled after
// it for that tenant, even though they are valid.
const repository: SchemaFileRepository = {
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    {
      fileName: 'broken.yml',
      content: `
cubes:
  - name: broken
    sql_table: broken
    measures:
      - name: count
        type: count
    dimensions:
      - name: created_at
        sql: missing_column
        type: time
    pre_aggregations:
      - name: main
        measures: [CUBE.count]
        time_dimension: CUBE.created_at
        granularity: day
        partition_granularity: month
`,
    },
    {
      fileName: 'sub_folder/healthy.yml',
      content: `
cubes:
  - name: healthy
    sql_table: healthy
    measures:
      - name: count
        type: count
    dimensions:
      - name: status
        sql: status
        type: string
    pre_aggregations:
      - name: main
        measures: [CUBE.count]
        dimensions: [CUBE.status]
`,
    },
  ]),
};

class MockDriver extends BaseDriver {
  public createdTables: string[] = [];

  public tables: string[] = [];

  public async testConnection() {
    // noop
  }

  public async query(query: string): Promise<any[]> {
    if (query.includes('missing_column')) {
      throw new Error('column broken.missing_column does not exist');
    }
    return [];
  }

  public async getTablesQuery(schema: string) {
    return this.tables.map(t => ({ table_name: t.replace(`${schema}.`, '') }));
  }

  public async createSchemaIfNotExists(): Promise<null> {
    return null;
  }

  public async loadPreAggregationIntoTable(preAggregationTableName: string, loadSql: string): Promise<unknown[]> {
    await this.query(loadSql);
    this.createdTables.push(preAggregationTableName);
    this.tables.push(preAggregationTableName);
    return [];
  }

  public async dropTable(tableName: string) {
    this.tables = this.tables.filter(t => t !== tableName);
  }

  public async tableColumnTypes() {
    return [{ name: 'foo', type: 'int' }];
  }
}

describe('issue #9132: failing pre-aggregation blocks refresh of the following ones', () => {
  jest.setTimeout(60000);

  test('pre-aggregation scheduled after a broken one is still built', async () => {
    process.env.CUBEJS_EXTERNAL_DEFAULT = 'false';
    process.env.CUBEJS_SCHEDULED_REFRESH_DEFAULT = 'true';

    const mockDriver = new MockDriver();

    class CubejsServerCoreDisabledRefreshTimer extends CubejsServerCore {
      public startScheduledRefreshTimer(): null {
        return null;
      }
    }

    const serverCore = new CubejsServerCoreDisabledRefreshTimer({
      apiSecret: 'foo',
      logger: testLogger,
      driverFactory: async () => mockDriver,
      externalDriverFactory: async () => new MockDriver(),
      orchestratorOptions: () => ({
        continueWaitTimeout: 1,
        queryCacheOptions: { queueOptions: () => ({ concurrency: 1 }) },
        preAggregationsOptions: { queueOptions: () => ({ concurrency: 1 }) },
        redisPrefix: 'TEST_ISSUE_9132',
      }),
    });

    const compilerApi = new CompilerApi(repository, async () => 'postgres', { logger: testLogger });
    jest.spyOn(serverCore, 'getCompilerApi').mockImplementation(async () => compilerApi);

    const refreshScheduler = new RefreshScheduler(serverCore);
    const ctx = { authInfo: { tenantId: 'tenant2' }, securityContext: { tenantId: 'tenant2' }, requestId: 'XXX' };
    // Persisted between runs the same way the refresh worker does it
    const queryIteratorState = {};

    try {
      for (let i = 0; i < 20; i++) {
        await refreshScheduler.runScheduledRefresh(ctx, {
          concurrency: 1,
          workerIndices: [0],
          timezones: ['UTC'],
          queryIteratorState,
        });
        if (mockDriver.createdTables.some(t => t.startsWith('stb_pre_aggregations.healthy_main'))) {
          break;
        }
      }

      expect(mockDriver.createdTables).toEqual(
        expect.arrayContaining([expect.stringMatching(/^stb_pre_aggregations\.healthy_main/)])
      );
    } finally {
      await serverCore.shutdown();
    }
  });
});
