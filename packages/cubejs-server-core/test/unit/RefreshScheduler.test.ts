import R from 'ramda';
import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import { CubejsServerCore, DatabaseType, SchemaFileRepository } from '../../src';
import { RefreshScheduler } from '../../src/core/RefreshScheduler';
import { CompilerApi } from '../../src/core/CompilerApi';
import { OrchestratorApi } from '../../src/core/OrchestratorApi';

const schemaContent = `
cube('Foo', {
  sql: \`select * from foo_\${SECURITY_CONTEXT.tenantId.unsafeValue()}\`,
  
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
    first: {
      type: 'rollup',
      measureReferences: [count],
      timeDimensionReference: time,
      granularity: 'day',
      partitionGranularity: 'day',
      scheduledRefresh: true,
      refreshKey: {
        every: '1 hour',
        updateWindow: '1 day',
        incremental: true
      }
    },
    second: {
      type: 'rollup',
      measureReferences: [total],
      timeDimensionReference: time,
      granularity: 'day',
      partitionGranularity: 'day',
      scheduledRefresh: true,
      refreshKey: {
        every: '1 hour',
        updateWindow: '1 day',
        incremental: true
      }
    },
  }
});

cube('Bar', {
  sql: 'select * from bar',
  
  measures: {
    count: {
      type: 'count'
    }
  },
  
  dimensions: {
    time: {
      sql: 'timestamp',
      type: 'time'
    }
  },
  
  preAggregations: {
    first: {
      type: 'rollup',
      measureReferences: [count],
      timeDimensionReference: time,
      granularity: 'day',
      partitionGranularity: 'day',
      scheduledRefresh: true,
      refreshKey: {
        every: '1 hour',
        updateWindow: '1 day',
        incremental: true
      }
    }
  }
});
`;

const repositoryWithPreAggregations: SchemaFileRepository = {
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'main.js', content: schemaContent },
  ]),
};

const repositoryWithoutPreAggregations: SchemaFileRepository = {
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'main.js', content: `
cube('Bar', {
  sql: 'select * from bar',
  
  measures: {
    count: {
      type: 'count'
    }
  },
  
  dimensions: {
    time: {
      sql: 'timestamp',
      type: 'time'
    }
  }
});
` },
  ]),
};

class MockDriver extends BaseDriver {
  public tables: any[] = [];

  public createdTables: any[] = [];

  public tablesReady: any[] = [];

  public executedQueries: any[] = [];

  public cancelledQueries: any[] = [];

  private tablesQueryDelay: any;

  private schema: any;

  public constructor() {
    super();
  }

  public query(query) {
    this.executedQueries.push(query);
    let promise: any = Promise.resolve([query]);
    promise = promise.then((res) => new Promise(resolve => setTimeout(() => resolve(res), 150)));

    if (query.match(/min\(.*timestamp.*foo/)) {
      promise = promise.then(() => [{ min: '2020-12-27T00:00:00.000' }]);
    }

    if (query.match(/max\(.*timestamp.*/)) {
      promise = promise.then(() => [{ max: '2020-12-31T01:00:00.000' }]);
    }

    if (query.match(/min\(.*timestamp.*bar/)) {
      promise = promise.then(() => [{ min: '2020-12-29T00:00:00.000' }]);
    }

    if (query.match(/max\(.*timestamp.*bar/)) {
      promise = promise.then(() => [{ max: '2020-12-31T01:00:00.000' }]);
    }

    if (this.tablesReady.find(t => query.indexOf(t) !== -1)) {
      promise = promise.then(res => res.concat({ tableReady: true }));
    }

    promise.cancel = () => {
      this.cancelledQueries.push(query);
    };
    return promise;
  }

  public async getTablesQuery(schema) {
    if (this.tablesQueryDelay) {
      await this.delay(this.tablesQueryDelay);
    }
    return this.tables.map(t => ({ table_name: t.replace(`${schema}.`, '') }));
  }

  public delay(timeout) {
    return new Promise(resolve => setTimeout(() => resolve(null), timeout));
  }

  public async createSchemaIfNotExists(schema) {
    this.schema = schema;
    return null;
  }

  public loadPreAggregationIntoTable(preAggregationTableName, loadSql) {
    const matchedTableName = preAggregationTableName.match(/^(.*)_([0-9a-z]+)_([0-9a-z]+)_([0-9a-z]+)$/);
    const timezone = loadSql.match(/AT TIME ZONE '(.*?)'/)[1];
    this.createdTables.push({
      tableName: matchedTableName[1],
      timezone,
      fromTable: loadSql.match(/FROM\n(.*?) AS/)[1].trim(),
    });
    this.tables.push(preAggregationTableName.substring(0, 100));
    const promise: any = this.query(loadSql);
    const resPromise: any = promise.then(() => this.tablesReady.push(preAggregationTableName.substring(0, 100)));
    resPromise.cancel = promise.cancel;
    return resPromise;
  }

  public async dropTable(tableName) {
    this.tables = this.tables.filter(t => t !== tableName);
    return this.query(`DROP TABLE ${tableName}`);
  }

  public async tableColumnTypes() {
    return [{ name: 'foo', type: 'int' }];
  }
}

let testCounter = 1;

const setupScheduler = ({ repository }: { repository: SchemaFileRepository }) => {
  const serverCore = new CubejsServerCore({
    dbType: 'postgres',
    apiSecret: 'foo',
  });
  const compilerApi = new CompilerApi(repository, 'postgres', {
    compileContext: {},
    logger: (msg, params) => {
      console.log(msg, params);
    },
  });

  const mockDriver = new MockDriver();

  const orchestratorApi = new OrchestratorApi(() => mockDriver, (msg, params) => console.log(msg, params), {
    contextToDbType(): DatabaseType {
      return 'postgres';
    },
    contextToExternalDbType(): DatabaseType {
      return 'cubestore';
    },
    continueWaitTimeout: 0.1,
    preAggregationsOptions: {
      queueOptions: {
        executionTimeout: 2
      }
    },
    redisPrefix: `TEST_${testCounter++}`
  });

  jest.spyOn(serverCore, 'getCompilerApi').mockImplementation(() => compilerApi);
  jest.spyOn(serverCore, 'getOrchestratorApi').mockImplementation(() => <any>orchestratorApi);

  const refreshScheduler = new RefreshScheduler(serverCore);
  return { refreshScheduler, orchestratorApi, mockDriver };
};

describe('Refresh Scheduler', () => {
  jest.setTimeout(60000);

  test('Round robin pre-aggregation refresh by history priority', async () => {
    const {
      refreshScheduler, mockDriver
    } = setupScheduler({ repository: repositoryWithPreAggregations });
    const result = [
      { tableName: 'stb_pre_aggregations.foo_first20201231', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201231', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.bar_first20201231', timezone: 'UTC', fromTable: 'bar' },
      { tableName: 'stb_pre_aggregations.foo_first20201230', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201230', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.bar_first20201230', timezone: 'UTC', fromTable: 'bar' },
      { tableName: 'stb_pre_aggregations.foo_first20201229', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201229', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.bar_first20201229', timezone: 'UTC', fromTable: 'bar' },
      { tableName: 'stb_pre_aggregations.foo_first20201228', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201228', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_first20201227', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201227', timezone: 'UTC', fromTable: 'foo_tenant1' },
    ];

    const ctx = { authInfo: { tenantId: 'tenant1' }, securityContext: { tenantId: 'tenant1' }, requestId: 'XXX' };
    const queryIteratorState = {};

    for (let i = 0; i < 1000; i++) {
      const refreshResult = await refreshScheduler.runScheduledRefresh(ctx, {
        concurrency: 2, workerIndices: [0], queryIteratorState, preAggregationsWarmup: true
      });
      console.log(mockDriver.createdTables);
      expect(mockDriver.createdTables).toEqual(
        R.take(mockDriver.createdTables.length, result.filter((x, qi) => qi % 2 === 0))
      );
      if (refreshResult.finished) {
        break;
      }
    }

    for (let i = 0; i < 1000; i++) {
      const refreshResult = await refreshScheduler.runScheduledRefresh(ctx, {
        concurrency: 2, workerIndices: [1], queryIteratorState, preAggregationsWarmup: true
      });
      const prevWorkerResult = result.filter((x, qi) => qi % 2 === 0);
      expect(mockDriver.createdTables).toEqual(
        R.take(mockDriver.createdTables.length, prevWorkerResult.concat(result.filter((x, qi) => qi % 2 === 1)))
      );
      if (refreshResult.finished) {
        break;
      }
    }
  });

  test('Round robin pre-aggregation with timezones', async () => {
    const {
      refreshScheduler, mockDriver
    } = setupScheduler({ repository: repositoryWithPreAggregations });
    const result = [
      { tableName: 'stb_pre_aggregations.foo_first20201231', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201231', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.bar_first20201231', timezone: 'UTC', fromTable: 'bar' },
      { tableName: 'stb_pre_aggregations.foo_first20201231', timezone: 'America/Los_Angeles', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201231', timezone: 'America/Los_Angeles', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.bar_first20201231', timezone: 'America/Los_Angeles', fromTable: 'bar' },

      { tableName: 'stb_pre_aggregations.foo_first20201230', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201230', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.bar_first20201230', timezone: 'UTC', fromTable: 'bar' },
      { tableName: 'stb_pre_aggregations.foo_first20201230', timezone: 'America/Los_Angeles', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201230', timezone: 'America/Los_Angeles', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.bar_first20201230', timezone: 'America/Los_Angeles', fromTable: 'bar' },

      { tableName: 'stb_pre_aggregations.foo_first20201229', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201229', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.bar_first20201229', timezone: 'UTC', fromTable: 'bar' },
      { tableName: 'stb_pre_aggregations.foo_first20201229', timezone: 'America/Los_Angeles', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201229', timezone: 'America/Los_Angeles', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.bar_first20201229', timezone: 'America/Los_Angeles', fromTable: 'bar' },

      { tableName: 'stb_pre_aggregations.foo_first20201228', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201228', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_first20201228', timezone: 'America/Los_Angeles', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201228', timezone: 'America/Los_Angeles', fromTable: 'foo_tenant1' },

      { tableName: 'stb_pre_aggregations.foo_first20201227', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201227', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_first20201227', timezone: 'America/Los_Angeles', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201227', timezone: 'America/Los_Angeles', fromTable: 'foo_tenant1' },
    ];

    const ctx = { authInfo: { tenantId: 'tenant1' }, securityContext: { tenantId: 'tenant1' }, requestId: 'XXX' };
    const queryIteratorState = {};

    for (let i = 0; i < 1000; i++) {
      const refreshResult = await refreshScheduler.runScheduledRefresh(
        ctx,
        { concurrency: 2, workerIndices: [0], timezones: ['UTC', 'America/Los_Angeles'], queryIteratorState }
      );
      expect(mockDriver.createdTables).toEqual(
        R.take(mockDriver.createdTables.length, result.filter((x, qi) => qi % 2 === 0))
      );
      if (refreshResult.finished) {
        break;
      }
    }

    for (let i = 0; i < 1000; i++) {
      const refreshResult = await refreshScheduler.runScheduledRefresh(
        ctx,
        { concurrency: 2, workerIndices: [1], timezones: ['UTC', 'America/Los_Angeles'], queryIteratorState }
      );
      const prevWorkerResult = result.filter((x, qi) => qi % 2 === 0);
      expect(mockDriver.createdTables).toEqual(
        R.take(mockDriver.createdTables.length, prevWorkerResult.concat(result.filter((x, qi) => qi % 2 === 1)))
      );
      if (refreshResult.finished) {
        break;
      }
    }

    expect(mockDriver.createdTables).toEqual(
      result.filter((x, qi) => qi % 2 === 0).concat(result.filter((x, qi) => qi % 2 === 1))
    );

    console.log('Running refresh on existing queryIteratorSate');

    const refreshResult = await refreshScheduler.runScheduledRefresh(
      ctx,
      { concurrency: 2, workerIndices: [1], timezones: ['UTC', 'America/Los_Angeles'], queryIteratorState }
    );

    expect(refreshResult.finished).toEqual(true);
  });

  test('Iterator waits before advance', async () => {
    const {
      refreshScheduler, mockDriver
    } = setupScheduler({ repository: repositoryWithPreAggregations });
    const result = [
      { tableName: 'stb_pre_aggregations.foo_first20201231', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201231', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.bar_first20201231', timezone: 'UTC', fromTable: 'bar' },
      { tableName: 'stb_pre_aggregations.foo_first20201230', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201230', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.bar_first20201230', timezone: 'UTC', fromTable: 'bar' },
      { tableName: 'stb_pre_aggregations.foo_first20201229', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201229', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.bar_first20201229', timezone: 'UTC', fromTable: 'bar' },
      { tableName: 'stb_pre_aggregations.foo_first20201228', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201228', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_first20201227', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201227', timezone: 'UTC', fromTable: 'foo_tenant1' },
    ];

    const ctx = { authInfo: { tenantId: 'tenant1' }, securityContext: { tenantId: 'tenant1' }, requestId: 'XXX' };
    const queryIteratorState = {};

    for (let i = 0; i < 5; i++) {
      refreshScheduler.runScheduledRefresh(ctx, { concurrency: 2, workerIndices: [0], queryIteratorState });
    }

    for (let i = 0; i < 1000; i++) {
      const refreshResult = await refreshScheduler.runScheduledRefresh(ctx, {
        concurrency: 2,
        workerIndices: [0],
        queryIteratorState
      });
      expect(mockDriver.createdTables).toEqual(
        R.take(mockDriver.createdTables.length, result.filter((x, qi) => qi % 2 === 0))
      );
      if (refreshResult.finished) {
        break;
      }
    }
  });

  test('Empty pre-aggregations', async () => {
    const { refreshScheduler, mockDriver } = setupScheduler({
      repository: repositoryWithoutPreAggregations
    });

    const queryIteratorState = {};

    for (let i = 0; i < 1000; i++) {
      const refreshResult = await refreshScheduler.runScheduledRefresh(null, {
        concurrency: 1,
        workerIndices: [0],
        queryIteratorState,
      });
      expect(mockDriver.createdTables).toEqual([]);
      if (refreshResult.finished) {
        break;
      }
    }
  });
});
