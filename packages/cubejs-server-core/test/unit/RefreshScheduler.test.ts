import R from 'ramda';
import { CubejsServerCore, SchemaFileRepository } from '../../src';
import { RefreshScheduler } from '../../src/core/RefreshScheduler';
import { CompilerApi } from '../../src/core/CompilerApi';

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

class OrchestratorApiMock {
  public createdTables: any = [];

  public minMaxContinueWait: { [query: string]: boolean } = {};

  public async executeQuery(query) {
    console.log('Executing query', query);

    if (query.query && query.query.match(/min\(.*timestamp.*foo/)) {
      if (!this.minMaxContinueWait[query.query]) {
        this.minMaxContinueWait[query.query] = true;
        // eslint-disable-next-line no-throw-literal
        throw { error: 'Continue wait' };
      }
      return {
        data: [{
          min: '2020-12-27T00:00:00.000',
        }],
      };
    } else if (query.query && query.query.match(/max\(.*timestamp.*/)) {
      if (!this.minMaxContinueWait[query.query]) {
        this.minMaxContinueWait[query.query] = true;
        // eslint-disable-next-line no-throw-literal
        throw { error: 'Continue wait' };
      }
      return {
        data: [{
          max: '2020-12-31T00:00:00.000',
        }],
      };
    } else if (query.query && query.query.match(/min\(.*timestamp.*bar/)) {
      if (!this.minMaxContinueWait[query.query]) {
        this.minMaxContinueWait[query.query] = true;
        // eslint-disable-next-line no-throw-literal
        throw { error: 'Continue wait' };
      }
      return {
        data: [{
          min: '2020-12-29T00:00:00.000',
        }],
      };
    } else if (query.query && query.query.match(/max\(.*timestamp.*bar/)) {
      if (!this.minMaxContinueWait[query.query]) {
        this.minMaxContinueWait[query.query] = true;
        // eslint-disable-next-line no-throw-literal
        throw { error: 'Continue wait' };
      }
      return {
        data: [{
          max: '2020-12-31T00:00:00.000',
        }],
      };
    }

    if (query.preAggregations) {
      await Promise.all(query.preAggregations.map(async p => {
        const timezone = p.loadSql[0].match(/AT TIME ZONE '(.*?)'/)[1];
        if (!this.createdTables.find(t => t.tableName === p.tableName && t.timezone === timezone)) {
          await new Promise((resolve) => setTimeout(() => resolve(null), 200));
          if (!this.createdTables.find(t => t.tableName === p.tableName && t.timezone === timezone)) {
            this.createdTables.push({
              fromTable: p.loadSql[0].match(/FROM\n(.*?) AS/)[1].trim(),
              tableName: p.tableName,
              timezone
            });
          }
          // eslint-disable-next-line no-throw-literal
          throw { error: 'Continue wait' };
        }
      }));
    }

    return {
      data: [],
    };
  }

  public getCompilerApi() {
    //
  }

  public getOrchestratorApi() {
    //
  }
}

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

  const orchestratorApi = new OrchestratorApiMock();

  jest.spyOn(serverCore, 'getCompilerApi').mockImplementation(() => compilerApi);
  jest.spyOn(serverCore, 'getOrchestratorApi').mockImplementation(() => <any>orchestratorApi);

  const refreshScheduler = new RefreshScheduler(serverCore);
  return { refreshScheduler, orchestratorApi };
};

describe('Refresh Scheduler', () => {
  jest.setTimeout(60000);

  test('Round robin pre-aggregation refresh by history priority', async () => {
    const { refreshScheduler, orchestratorApi } = setupScheduler({ repository: repositoryWithPreAggregations });
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
      console.log(orchestratorApi.createdTables);
      expect(orchestratorApi.createdTables).toEqual(
        R.take(orchestratorApi.createdTables.length, result.filter((x, qi) => qi % 2 === 0))
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
      expect(orchestratorApi.createdTables).toEqual(
        R.take(orchestratorApi.createdTables.length, prevWorkerResult.concat(result.filter((x, qi) => qi % 2 === 1)))
      );
      if (refreshResult.finished) {
        break;
      }
    }
  });

  test('Round robin pre-aggregation with timezones', async () => {
    const { refreshScheduler, orchestratorApi } = setupScheduler({ repository: repositoryWithPreAggregations });
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
      expect(orchestratorApi.createdTables).toEqual(
        R.take(orchestratorApi.createdTables.length, result.filter((x, qi) => qi % 2 === 0))
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
      expect(orchestratorApi.createdTables).toEqual(
        R.take(orchestratorApi.createdTables.length, prevWorkerResult.concat(result.filter((x, qi) => qi % 2 === 1)))
      );
      if (refreshResult.finished) {
        break;
      }
    }

    expect(orchestratorApi.createdTables).toEqual(
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
    const { refreshScheduler, orchestratorApi } = setupScheduler({ repository: repositoryWithPreAggregations });
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
      expect(orchestratorApi.createdTables).toEqual(
        R.take(orchestratorApi.createdTables.length, result.filter((x, qi) => qi % 2 === 0))
      );
      if (refreshResult.finished) {
        break;
      }
    }
  });

  test('Empty pre-aggregations', async () => {
    const { refreshScheduler, orchestratorApi } = setupScheduler({
      repository: repositoryWithoutPreAggregations
    });
    const result = [];

    const queryIteratorState = {};

    for (let i = 0; i < 1000; i++) {
      const refreshResult = await refreshScheduler.runScheduledRefresh(null, {
        concurrency: 1,
        workerIndices: [0],
        queryIteratorState,
        throwErrors: true
      });
      expect(orchestratorApi.createdTables).toEqual(R.take((i + 1) * 2, result).filter((x, qi) => qi % 2 === 0));
      if (refreshResult.finished) {
        break;
      }
    }
  });
});
