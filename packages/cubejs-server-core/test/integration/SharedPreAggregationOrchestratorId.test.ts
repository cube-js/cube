// Two apps refresh their pre-aggregations into one schema, each through its own orchestrator,
// while `contextToPreAggregationOrchestratorId` gives them one pre-aggregation orchestrator id.
// One app's cleanup must keep the tables the other app refreshed.

import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import { pausePromise, SchemaFileRepository, testLogger } from '@cubejs-backend/shared';
import { CubejsServerCore, CompilerApi, RefreshScheduler } from '../../src';
import type { RequestContext } from '../../src/core/types';

const schemaContent = `
cube('Orders', {
  sql: 'select * from orders',

  measures: {
    count: {
      type: 'count'
    },

    total: {
      sql: 'amount',
      type: 'sum'
    },
  },

  dimensions: {
    time: {
      sql: 'timestamp',
      type: 'time'
    }
  },

  preAggregations: {
    forA: {
      type: 'rollup',
      measureReferences: [count],
      timeDimensionReference: time,
      granularity: 'day',
      partitionGranularity: 'day',
      scheduledRefresh: COMPILE_CONTEXT.app === 'a',
      refreshKey: {
        every: '1 hour',
      }
    },
    forB: {
      type: 'rollup',
      measureReferences: [total],
      timeDimensionReference: time,
      granularity: 'day',
      partitionGranularity: 'day',
      scheduledRefresh: COMPILE_CONTEXT.app === 'b',
      refreshKey: {
        every: '1 hour',
      }
    },
  }
});
`;

const repository: SchemaFileRepository = {
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'main.js', content: schemaContent },
  ]),
};

class MockDriver extends BaseDriver {
  public tables: string[] = [];

  public tablesReady: string[] = [];

  // eslint-disable-next-line @typescript-eslint/no-empty-function
  public async testConnection() {}

  public async query<R = unknown>(query: string): Promise<R[]> {
    await pausePromise(20);

    if (query.includes('min(') && query.includes('timestamp')) {
      return [{ min: '2020-12-30T00:00:00.000' }] as R[];
    }

    if (query.includes('max(') && query.includes('timestamp')) {
      return [{ max: '2020-12-31T01:00:00.000' }] as R[];
    }

    if (this.tablesReady.find(t => query.indexOf(t) !== -1)) {
      return [{ tableReady: true }] as R[];
    }

    return [] as R[];
  }

  public async getTablesQuery(schema: string) {
    return this.tables.map(t => ({ table_name: t.replace(`${schema}.`, '') }));
  }

  public async createSchemaIfNotExists(): Promise<null> {
    return null;
  }

  public async loadPreAggregationIntoTable(preAggregationTableName: string, loadSql: string): Promise<unknown[]> {
    this.tables.push(preAggregationTableName);
    const result = await this.query(loadSql);
    this.tablesReady.push(preAggregationTableName);
    return result;
  }

  public async dropTable(tableName: string) {
    this.tables = this.tables.filter(t => t !== tableName);
    return this.query(`DROP TABLE ${tableName}`);
  }

  public async tableColumnTypes() {
    return [{ name: 'foo', type: 'int' }];
  }
}

class CubejsServerCoreDisabledRefreshTimer extends CubejsServerCore {
  public startScheduledRefreshTimer(): null {
    return null;
  }
}

const context = (app: string): RequestContext => ({
  authInfo: { app },
  securityContext: { app },
  requestId: `shared-pre-aggregation-orchestrator-id-${app}`,
});

describe('Shared pre-aggregation orchestrator id', () => {
  jest.setTimeout(60000);

  const env = { ...process.env };

  beforeEach(() => {
    process.env.CUBEJS_EXTERNAL_DEFAULT = 'false';
    process.env.CUBEJS_SCHEDULED_REFRESH_DEFAULT = 'false';
    process.env.CUBEJS_DROP_PRE_AGG_WITHOUT_TOUCH = 'true';
  });

  afterEach(() => {
    process.env = { ...env };
  });

  test('refreshes of two orchestrators keep each other\'s tables', async () => {
    const driver = new MockDriver();
    const testId = Date.now();
    const orchestratorIds = new Set<string>();

    const serverCore = new CubejsServerCoreDisabledRefreshTimer({
      apiSecret: 'foo',
      logger: testLogger,
      // Both apps read the same database, which is what sharing a pre-aggregation id requires
      driverFactory: async () => driver,
      contextToOrchestratorId: ({ securityContext }) => {
        const orchestratorId = `APP_${securityContext.app}_${testId}`;
        orchestratorIds.add(orchestratorId);
        return orchestratorId;
      },
      contextToPreAggregationOrchestratorId: () => `SHARED_${testId}`,
      orchestratorOptions: () => ({
        continueWaitTimeout: 1,
        queryCacheOptions: {
          queueOptions: () => ({ concurrency: 2 }),
        },
        preAggregationsOptions: {
          queueOptions: () => ({ executionTimeout: 2, concurrency: 2 }),
        },
      }),
    });

    // Each app compiles the same data model, but refreshes only its own pre-aggregation
    const compilerApis: Record<string, CompilerApi> = {
      a: new CompilerApi(repository, async () => 'postgres', { compileContext: { app: 'a' }, logger: testLogger }),
      b: new CompilerApi(repository, async () => 'postgres', { compileContext: { app: 'b' }, logger: testLogger }),
    };
    jest.spyOn(serverCore, 'getCompilerApi')
      .mockImplementation(async ({ securityContext }) => compilerApis[securityContext.app]);

    const refreshScheduler = new RefreshScheduler(serverCore);

    const refreshUntilFinished = async (app: string) => {
      for (let i = 0; i < 1000; i++) {
        const { finished } = await refreshScheduler.runScheduledRefresh(
          context(app),
          { concurrency: 1, workerIndices: [0], timezones: ['UTC'] },
        );
        if (finished) {
          return;
        }
      }
      throw new Error(`Scheduled refresh of app ${app} did not finish`);
    };

    const tablesOf = (preAggregation: string) => driver.tables.filter(t => t.includes(`orders_${preAggregation}`));

    try {
      await refreshUntilFinished('a');
      const tablesOfA = tablesOf('for_a');
      expect(tablesOfA.length).toBeGreaterThan(0);
      expect(tablesOf('for_b')).toHaveLength(0);

      await refreshUntilFinished('b');
      expect(tablesOf('for_b').length).toBeGreaterThan(0);

      // A rebuild in app B ends with a cleanup of orphaned tables, after the refresh end was reached
      const tablesOfB = tablesOf('for_b');
      const [partitionOfB] = tablesOfB.map(t => t.replace(/_[0-9a-z]+_[0-9a-z]+_[0-9a-z]+$/, ''));
      // Nothing uses or touches it, so it shows the cleanup ran
      const orphanedTable = 'stb_pre_aggregations.orders_orphaned20201230_aaaaaaaa_bbbbbbbb_cccccc';
      driver.tables.push(orphanedTable);
      let rebuilt = false;

      for (let i = 0; i < 100 && !rebuilt; i++) {
        try {
          await refreshScheduler.buildPreAggregations(context('b'), {
            timezones: ['UTC'],
            preAggregations: [{ id: 'Orders.forB', partitions: [partitionOfB] }],
            forceBuildPreAggregations: true,
            throwErrors: true,
          });
          rebuilt = true;
        } catch (e) {
          if ((<{ error: string }>e).error !== 'Continue wait') {
            throw e;
          }
        }
      }

      expect(rebuilt).toBe(true);
      // The rebuild loaded the partition again
      expect(tablesOf('for_b')).toHaveLength(tablesOfB.length + 1);
      expect(driver.tables).not.toContain(orphanedTable);
      expect(orchestratorIds).toEqual(new Set([`APP_a_${testId}`, `APP_b_${testId}`]));
      expect(tablesOf('for_a')).toEqual(tablesOfA);
    } finally {
      await serverCore.releaseConnections();
    }
  });
});
