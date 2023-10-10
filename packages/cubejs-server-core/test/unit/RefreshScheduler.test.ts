import R from 'ramda';
import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import { pausePromise, SchemaFileRepository } from '@cubejs-backend/shared';
import { CubejsServerCore } from '../../src';
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
    main: {
      type: 'originalSql',
      scheduledRefresh: false
    },
    first: {
      type: 'rollup',
      measureReferences: [count],
      timeDimensionReference: time,
      granularity: 'day',
      partitionGranularity: 'day',
      refreshKey: {
        every: '1 hour',
        updateWindow: '1 day',
        incremental: true
      }
    },
    orphaned: {
      type: 'rollup',
      measureReferences: [count],
      timeDimensionReference: time,
      granularity: 'day',
      partitionGranularity: 'day',
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
      refreshKey: {
        every: '1 hour',
        updateWindow: '1 day',
        incremental: true
      },
      useOriginalSqlPreAggregations: COMPILE_CONTEXT.useOriginalSqlPreAggregations
    },
    noRefresh: {
      type: 'rollup',
      measureReferences: [count],
      timeDimensionReference: time,
      granularity: 'hour',
      partitionGranularity: 'day',
      scheduledRefresh: false,
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

const repositoryWithRollupJoin: SchemaFileRepository = {
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'main.js', content: `
      cube(\`Users\`, {
          sql: \`SELECT * FROM public.users\`,
        
          preAggregations: {
            usersRollup: {
              dimensions: [CUBE.id],
            },
          },
        
          measures: {
            count: {
              type: \`count\`,
            },
          },
        
          dimensions: {
            id: {
              sql: \`id\`,
              type: \`string\`,
              primaryKey: true,
            },
            
            name: {
              sql: \`name\`,
              type: \`string\`,
            },
          },
        });
        
        cube('Orders', {
          sql: \`SELECT * FROM orders\`,
        
          preAggregations: {
            ordersRollup: {
              measures: [CUBE.count],
              dimensions: [CUBE.userId, CUBE.status],
            },
            
            ordersRollupJoin: {
              type: \`rollupJoin\`,
              measures: [CUBE.count],
              dimensions: [Users.name],
              rollups: [Users.usersRollup, CUBE.ordersRollup],
            },
          },
        
          joins: {
            Users: {
              relationship: \`belongsTo\`,
              sql: \`\${CUBE.userId} = \${Users.id}\`,
            },
          },
        
          measures: {
            count: {
              type: \`count\`,
            },
          },
        
          dimensions: {
            id: {
              sql: \`id\`,
              type: \`number\`,
              primaryKey: true,
            },
            userId: {
              sql: \`user_id\`,
              type: \`number\`,
            },
            status: {
              sql: \`status\`,
              type: \`string\`,
            },
          },
        });
    ` },
  ]),
};

const repositoryWithoutPreAggregations: SchemaFileRepository = {
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    {
      fileName: 'main.js', content: `
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
`,
    },
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

  // eslint-disable-next-line @typescript-eslint/no-empty-function
  public async testConnection() {}

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
    const timezoneMatch = loadSql.match(/AT TIME ZONE '(.*?)'/);
    const timezone = timezoneMatch && timezoneMatch[1];
    const match = loadSql.match(/FROM\s+(?:(\S+)(?:_(?:[0-9a-z]+)_(?:[0-9a-z]+)_(?:[0-9a-z]+))|(\S+))/i);
    this.createdTables.push({
      tableName: matchedTableName[1],
      timezone,
      fromTable: match[1] ? { preAggTable: match[1] && match[1].trim() } : match[2] && match[2].trim(),
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

const setupScheduler = ({ repository, useOriginalSqlPreAggregations, skipAssertSecurityContext }: { repository: SchemaFileRepository, useOriginalSqlPreAggregations?: boolean, skipAssertSecurityContext?: true }) => {
  const mockDriver = new MockDriver();
  const externalDriver = new MockDriver();

  const serverCore = new CubejsServerCore({
    apiSecret: 'foo',
    logger: (msg, params) => console.log(msg, params),
    driverFactory: async ({ securityContext }) => {
      expect(typeof securityContext).toEqual('object');
      if (!skipAssertSecurityContext) {
        expect(securityContext.hasOwnProperty('tenantId')).toEqual(true);
      }

      return mockDriver;
    },
    externalDriverFactory: async ({ securityContext }) => {
      expect(typeof securityContext).toEqual('object');
      if (!skipAssertSecurityContext) {
        expect(securityContext.hasOwnProperty('tenantId')).toEqual(true);
      }

      return externalDriver;
    },
    orchestratorOptions: () => ({
      continueWaitTimeout: 0.1,
      queryCacheOptions: {
        queueOptions: () => ({
          concurrency: 2,
        }),
      },
      preAggregationsOptions: {
        queueOptions: () => ({
          executionTimeout: 2,
          concurrency: 2,
        }),
      },
      redisPrefix: `TEST_${testCounter++}`,
    })
  });

  const compilerApi = new CompilerApi(
    repository,
    async () => 'postgres',
    {
      compileContext: {
        useOriginalSqlPreAggregations,
      },
      logger: (msg, params) => {
        console.log(msg, params);
      },
    }
  );

  jest.spyOn(serverCore, 'getCompilerApi').mockImplementation(async () => compilerApi);

  const refreshScheduler = new RefreshScheduler(serverCore);
  return { refreshScheduler, compilerApi, mockDriver };
};

describe('Refresh Scheduler', () => {
  jest.setTimeout(60000);

  beforeEach(async () => {
    delete process.env.CUBEJS_DROP_PRE_AGG_WITHOUT_TOUCH;
    delete process.env.CUBEJS_TOUCH_PRE_AGG_TIMEOUT;
    delete process.env.CUBEJS_DB_QUERY_TIMEOUT;
  });

  afterAll(async () => {
    // align logs from STDOUT
    await pausePromise(100);
  });

  test('Round robin pre-aggregation refresh by history priority', async () => {
    process.env.CUBEJS_EXTERNAL_DEFAULT = 'false';
    process.env.CUBEJS_SCHEDULED_REFRESH_DEFAULT = 'true';
    const {
      refreshScheduler, mockDriver,
    } = setupScheduler({ repository: repositoryWithPreAggregations, useOriginalSqlPreAggregations: true });
    const result1 = [
      { tableName: 'stb_pre_aggregations.foo_first20201231', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_main', timezone: null, fromTable: 'foo_tenant1' },
      {
        tableName: 'stb_pre_aggregations.foo_second20201231',
        timezone: 'UTC',
        fromTable: { preAggTable: 'stb_pre_aggregations.foo_main' },
      },
      { tableName: 'stb_pre_aggregations.foo_first20201230', timezone: 'UTC', fromTable: 'foo_tenant1' },
      {
        tableName: 'stb_pre_aggregations.foo_second20201230',
        timezone: 'UTC',
        fromTable: { preAggTable: 'stb_pre_aggregations.foo_main' },
      },
      { tableName: 'stb_pre_aggregations.foo_first20201229', timezone: 'UTC', fromTable: 'foo_tenant1' },
      {
        tableName: 'stb_pre_aggregations.foo_second20201229',
        timezone: 'UTC',
        fromTable: { preAggTable: 'stb_pre_aggregations.foo_main' },
      },
      { tableName: 'stb_pre_aggregations.foo_first20201228', timezone: 'UTC', fromTable: 'foo_tenant1' },
      {
        tableName: 'stb_pre_aggregations.foo_second20201228',
        timezone: 'UTC',
        fromTable: { preAggTable: 'stb_pre_aggregations.foo_main' },
      },
      { tableName: 'stb_pre_aggregations.foo_orphaned20201227', timezone: 'UTC', fromTable: 'foo_tenant1' },
    ];

    const result2 = [
      { tableName: 'stb_pre_aggregations.foo_orphaned20201231', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.bar_first20201231', timezone: 'UTC', fromTable: 'bar' },
      { tableName: 'stb_pre_aggregations.foo_orphaned20201230', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.bar_first20201230', timezone: 'UTC', fromTable: 'bar' },
      { tableName: 'stb_pre_aggregations.foo_orphaned20201229', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.bar_first20201229', timezone: 'UTC', fromTable: 'bar' },
      { tableName: 'stb_pre_aggregations.foo_orphaned20201228', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_first20201227', timezone: 'UTC', fromTable: 'foo_tenant1', },
      {
        tableName: 'stb_pre_aggregations.foo_second20201227',
        timezone: 'UTC',
        fromTable: { preAggTable: 'stb_pre_aggregations.foo_main' },
      },
    ];

    const ctx = { authInfo: { tenantId: 'tenant1' }, securityContext: { tenantId: 'tenant1' }, requestId: 'XXX' };
    const queryIteratorState = {};

    for (let i = 0; i < 1000; i++) {
      const refreshResult = await refreshScheduler.runScheduledRefresh(ctx, {
        concurrency: 2, workerIndices: [0], queryIteratorState, preAggregationsWarmup: true,
      });
      console.log(mockDriver.createdTables);
      expect(mockDriver.createdTables).toEqual(
        R.take(mockDriver.createdTables.length, result1),
      );
      if (refreshResult.finished) {
        break;
      }
    }

    for (let i = 0; i < 1000; i++) {
      const refreshResult = await refreshScheduler.runScheduledRefresh(ctx, {
        concurrency: 2, workerIndices: [1], queryIteratorState, preAggregationsWarmup: true,
      });
      expect(mockDriver.createdTables).toEqual(
        R.take(mockDriver.createdTables.length, result1.concat(result2)),
      );
      if (refreshResult.finished) {
        break;
      }
    }
  });

  test('Manual build', async () => {
    process.env.CUBEJS_EXTERNAL_DEFAULT = 'false';
    process.env.CUBEJS_SCHEDULED_REFRESH_DEFAULT = 'true';
    const {
      refreshScheduler, mockDriver,
    } = setupScheduler({ repository: repositoryWithPreAggregations, useOriginalSqlPreAggregations: true });

    const ctx = { authInfo: { tenantId: 'tenant1' }, securityContext: { tenantId: 'tenant1' }, requestId: 'XXX' };

    for (let i = 0; i < 100; i++) {
      try {
        await refreshScheduler.buildPreAggregations(ctx, {
          timezones: ['UTC'],
          preAggregations: [{
            id: 'Foo.second',
            partitions: ['stb_pre_aggregations.foo_second20201230'],
          }],
          forceBuildPreAggregations: false,
          throwErrors: true,
        });
      } catch (e) {
        if ((<{ error: string }>e).error !== 'Continue wait') {
          throw e;
        } else {
          // eslint-disable-next-line no-continue
          continue;
        }
      }
      break;
    }

    expect(mockDriver.createdTables).toEqual(
      [
        { tableName: 'stb_pre_aggregations.foo_main', timezone: null, fromTable: 'foo_tenant1' },
        {
          tableName: 'stb_pre_aggregations.foo_second20201230',
          timezone: 'UTC',
          fromTable: { preAggTable: 'stb_pre_aggregations.foo_main' },
        },
      ],
    );
  });

  test('Drop without touch', async () => {
    process.env.CUBEJS_EXTERNAL_DEFAULT = 'false';
    process.env.CUBEJS_SCHEDULED_REFRESH_DEFAULT = 'false';
    process.env.CUBEJS_DROP_PRE_AGG_WITHOUT_TOUCH = 'true';
    process.env.CUBEJS_TOUCH_PRE_AGG_TIMEOUT = '3';
    process.env.CUBEJS_DB_QUERY_TIMEOUT = '3';
    const {
      refreshScheduler, mockDriver,
    } = setupScheduler({
      repository: repositoryWithPreAggregations
    });

    const ctx = { authInfo: { tenantId: 'tenant1' }, securityContext: { tenantId: 'tenant1' }, requestId: 'XXX' };

    for (let i = 0; i < 1000; i++) {
      const refreshResult = await refreshScheduler.runScheduledRefresh(
        ctx,
        { concurrency: 1, workerIndices: [0], timezones: ['UTC'] },
      );
      if (refreshResult.finished) {
        break;
      }
    }

    expect(mockDriver.tables).toHaveLength(0);

    for (let i = 0; i < 100; i++) {
      try {
        await refreshScheduler.buildPreAggregations(ctx, {
          timezones: ['UTC'],
          preAggregations: [{
            id: 'Foo.first',
            partitions: ['stb_pre_aggregations.foo_first20201230'],
          }],
          forceBuildPreAggregations: false,
          throwErrors: true,
        });
      } catch (e) {
        if ((<{ error: string }>e).error !== 'Continue wait') {
          throw e;
        } else {
          // eslint-disable-next-line no-continue
          continue;
        }
      }
      break;
    }

    expect(mockDriver.tables).toHaveLength(1);
    expect(mockDriver.tables[0]).toMatch(/^stb_pre_aggregations\.foo_first20201230/);

    await mockDriver.delay(3000);

    for (let i = 0; i < 1000; i++) {
      const refreshResult = await refreshScheduler.runScheduledRefresh(
        ctx,
        { concurrency: 1, workerIndices: [0], timezones: ['UTC'] },
      );
      if (refreshResult.finished) {
        break;
      }
    }

    expect(mockDriver.tables).toHaveLength(1);

    for (let i = 0; i < 100; i++) {
      try {
        await refreshScheduler.buildPreAggregations(ctx, {
          timezones: ['UTC'],
          preAggregations: [{
            id: 'Foo.first',
            partitions: ['stb_pre_aggregations.foo_first20201229'],
          }],
          forceBuildPreAggregations: false,
          throwErrors: true,
        });
      } catch (e) {
        if ((<{ error: string }>e).error !== 'Continue wait') {
          throw e;
        } else {
          // eslint-disable-next-line no-continue
          continue;
        }
      }
      break;
    }

    expect(mockDriver.tables).toHaveLength(1);
    expect(mockDriver.tables[0]).toMatch(/^stb_pre_aggregations\.foo_first20201229/);
  });

  test('Cache only pre-aggregation partitions', async () => {
    process.env.CUBEJS_EXTERNAL_DEFAULT = 'false';
    process.env.CUBEJS_SCHEDULED_REFRESH_DEFAULT = 'true';
    const {
      refreshScheduler,
    } = setupScheduler({ repository: repositoryWithPreAggregations, useOriginalSqlPreAggregations: true });

    const ctx = { authInfo: { tenantId: 'tenant1' }, securityContext: { tenantId: 'tenant1' }, requestId: 'XXX' };

    for (let i = 0; i < 100; i++) {
      try {
        const res = await refreshScheduler.preAggregationPartitions(ctx, {
          timezones: ['UTC'],
          preAggregations: [{
            id: 'Foo.noRefresh',
            cacheOnly: true,
          }],
          throwErrors: true,
        });

        expect(JSON.parse(JSON.stringify(res))).toEqual(
          [{
            timezones: ['UTC'],
            preAggregation: {
              id: 'Foo.noRefresh',
              preAggregationName: 'noRefresh',
              preAggregation: {
                type: 'rollup',
                granularity: 'hour',
                partitionGranularity: 'day',
                scheduledRefresh: false,
                refreshKey: { every: '1 hour', updateWindow: '1 day', incremental: true },
                external: false,
              },
              cube: 'Foo',
              references: {
                dimensions: [],
                measures: ['Foo.count'],
                timeDimensions: [{ dimension: 'Foo.time', granularity: 'hour' }],
                rollups: [],
              },
              refreshKey: { every: '1 hour', updateWindow: '1 day', incremental: true },
            },
            partitions: [],
            errors: ['Waiting for cache'],
            partitionsWithDependencies: [{ dependencies: [], partitions: [] }],
          }],
        );
      } catch (e) {
        if ((<{ error: string }>e).error !== 'Continue wait') {
          throw e;
        } else {
          // eslint-disable-next-line no-continue
          continue;
        }
      }
      break;
    }
  });

  test('Round robin pre-aggregation with timezones', async () => {
    process.env.CUBEJS_EXTERNAL_DEFAULT = 'false';
    process.env.CUBEJS_SCHEDULED_REFRESH_DEFAULT = 'true';
    const {
      refreshScheduler, mockDriver,
    } = setupScheduler({ repository: repositoryWithPreAggregations });
    const result = [
      { tableName: 'stb_pre_aggregations.foo_first20201231', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_orphaned20201231', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201231', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.bar_first20201231', timezone: 'UTC', fromTable: 'bar' },
      {
        tableName: 'stb_pre_aggregations.foo_first20201231',
        timezone: 'America/Los_Angeles',
        fromTable: 'foo_tenant1',
      },
      { tableName: 'stb_pre_aggregations.foo_orphaned20201231', timezone: 'America/Los_Angeles', fromTable: 'foo_tenant1' },
      {
        tableName: 'stb_pre_aggregations.foo_second20201231',
        timezone: 'America/Los_Angeles',
        fromTable: 'foo_tenant1',
      },
      { tableName: 'stb_pre_aggregations.bar_first20201231', timezone: 'America/Los_Angeles', fromTable: 'bar' },

      { tableName: 'stb_pre_aggregations.foo_first20201230', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_orphaned20201230', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201230', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.bar_first20201230', timezone: 'UTC', fromTable: 'bar' },
      {
        tableName: 'stb_pre_aggregations.foo_first20201230',
        timezone: 'America/Los_Angeles',
        fromTable: 'foo_tenant1',
      },
      { tableName: 'stb_pre_aggregations.foo_orphaned20201230', timezone: 'America/Los_Angeles', fromTable: 'foo_tenant1' },
      {
        tableName: 'stb_pre_aggregations.foo_second20201230',
        timezone: 'America/Los_Angeles',
        fromTable: 'foo_tenant1',
      },
      { tableName: 'stb_pre_aggregations.bar_first20201230', timezone: 'America/Los_Angeles', fromTable: 'bar' },

      { tableName: 'stb_pre_aggregations.foo_first20201229', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_orphaned20201229', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201229', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.bar_first20201229', timezone: 'UTC', fromTable: 'bar' },
      {
        tableName: 'stb_pre_aggregations.foo_first20201229',
        timezone: 'America/Los_Angeles',
        fromTable: 'foo_tenant1',
      },
      { tableName: 'stb_pre_aggregations.foo_orphaned20201229', timezone: 'America/Los_Angeles', fromTable: 'foo_tenant1' },
      {
        tableName: 'stb_pre_aggregations.foo_second20201229',
        timezone: 'America/Los_Angeles',
        fromTable: 'foo_tenant1',
      },
      { tableName: 'stb_pre_aggregations.bar_first20201229', timezone: 'America/Los_Angeles', fromTable: 'bar' },

      { tableName: 'stb_pre_aggregations.foo_first20201228', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_orphaned20201228', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201228', timezone: 'UTC', fromTable: 'foo_tenant1' },
      {
        tableName: 'stb_pre_aggregations.foo_first20201228',
        timezone: 'America/Los_Angeles',
        fromTable: 'foo_tenant1',
      },
      { tableName: 'stb_pre_aggregations.foo_orphaned20201228', timezone: 'America/Los_Angeles', fromTable: 'foo_tenant1' },
      {
        tableName: 'stb_pre_aggregations.foo_second20201228',
        timezone: 'America/Los_Angeles',
        fromTable: 'foo_tenant1',
      },

      { tableName: 'stb_pre_aggregations.foo_first20201227', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_orphaned20201227', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201227', timezone: 'UTC', fromTable: 'foo_tenant1' },
      {
        tableName: 'stb_pre_aggregations.foo_first20201227',
        timezone: 'America/Los_Angeles',
        fromTable: 'foo_tenant1',
      },
      { tableName: 'stb_pre_aggregations.foo_orphaned20201227', timezone: 'America/Los_Angeles', fromTable: 'foo_tenant1' },
      {
        tableName: 'stb_pre_aggregations.foo_second20201227',
        timezone: 'America/Los_Angeles',
        fromTable: 'foo_tenant1',
      },
    ];

    const ctx = { authInfo: { tenantId: 'tenant1' }, securityContext: { tenantId: 'tenant1' }, requestId: 'XXX' };
    const queryIteratorState = {};

    for (let i = 0; i < 1000; i++) {
      const refreshResult = await refreshScheduler.runScheduledRefresh(
        ctx,
        { concurrency: 2, workerIndices: [0], timezones: ['UTC', 'America/Los_Angeles'], queryIteratorState },
      );
      expect(mockDriver.createdTables).toEqual(
        R.take(mockDriver.createdTables.length, result.filter((x, qi) => qi % 2 === 0)),
      );
      if (refreshResult.finished) {
        break;
      }
    }

    for (let i = 0; i < 1000; i++) {
      const refreshResult = await refreshScheduler.runScheduledRefresh(
        ctx,
        { concurrency: 2, workerIndices: [1], timezones: ['UTC', 'America/Los_Angeles'], queryIteratorState },
      );
      const prevWorkerResult = result.filter((x, qi) => qi % 2 === 0);
      expect(mockDriver.createdTables).toEqual(
        R.take(mockDriver.createdTables.length, prevWorkerResult.concat(result.filter((x, qi) => qi % 2 === 1))),
      );
      if (refreshResult.finished) {
        break;
      }
    }

    expect(mockDriver.createdTables).toEqual(
      result.filter((x, qi) => qi % 2 === 0).concat(result.filter((x, qi) => qi % 2 === 1)),
    );

    console.log('Running refresh on existing queryIteratorSate');

    const refreshResult = await refreshScheduler.runScheduledRefresh(
      ctx,
      { concurrency: 2, workerIndices: [1], timezones: ['UTC', 'America/Los_Angeles'], queryIteratorState },
    );

    expect(refreshResult.finished).toEqual(true);
  });

  test('Iterator waits before advance', async () => {
    process.env.CUBEJS_EXTERNAL_DEFAULT = 'false';
    process.env.CUBEJS_SCHEDULED_REFRESH_DEFAULT = 'true';
    const {
      refreshScheduler, mockDriver,
    } = setupScheduler({ repository: repositoryWithPreAggregations });
    const result = [
      { tableName: 'stb_pre_aggregations.foo_first20201231', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_orphaned20201231', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201231', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.bar_first20201231', timezone: 'UTC', fromTable: 'bar' },
      { tableName: 'stb_pre_aggregations.foo_first20201230', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_orphaned20201230', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201230', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.bar_first20201230', timezone: 'UTC', fromTable: 'bar' },
      { tableName: 'stb_pre_aggregations.foo_first20201229', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_orphaned20201229', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201229', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.bar_first20201229', timezone: 'UTC', fromTable: 'bar' },
      { tableName: 'stb_pre_aggregations.foo_first20201228', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_orphaned20201228', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_second20201228', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_first20201227', timezone: 'UTC', fromTable: 'foo_tenant1' },
      { tableName: 'stb_pre_aggregations.foo_orphaned20201227', timezone: 'UTC', fromTable: 'foo_tenant1' },
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
        queryIteratorState,
      });
      expect(mockDriver.createdTables).toEqual(
        R.take(mockDriver.createdTables.length, result.filter((x, qi) => qi % 2 === 0)),
      );
      if (refreshResult.finished) {
        break;
      }
    }
  });

  test('Empty pre-aggregations', async () => {
    process.env.CUBEJS_EXTERNAL_DEFAULT = 'false';
    process.env.CUBEJS_SCHEDULED_REFRESH_DEFAULT = 'true';
    const { refreshScheduler, mockDriver } = setupScheduler({
      repository: repositoryWithoutPreAggregations,
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

  test('Empty security context', async () => {
    process.env.CUBEJS_EXTERNAL_DEFAULT = 'false';
    process.env.CUBEJS_SCHEDULED_REFRESH_DEFAULT = 'true';
    const { refreshScheduler } = setupScheduler({
      repository: repositoryWithoutPreAggregations,
      skipAssertSecurityContext: true,
    });

    for (let i = 0; i < 50; i++) {
      await refreshScheduler.runScheduledRefresh({
        securityContext: undefined,
        authInfo: null,
        requestId: 'Empty security context'
      }, {
        concurrency: 1,
        workerIndices: [0],
      });
    }
    await refreshScheduler.runScheduledRefresh({
      securityContext: undefined,
      authInfo: null,
      requestId: 'Empty security context'
    }, {
      concurrency: 1,
      workerIndices: [0],
      throwErrors: true
    });
  });

  test('rollupJoin scheduledRefresh', async () => {
    process.env.CUBEJS_SCHEDULED_REFRESH_DEFAULT = 'true';
    const {
      refreshScheduler
    } = setupScheduler({ repository: repositoryWithRollupJoin, useOriginalSqlPreAggregations: true });
    const ctx = { authInfo: { tenantId: 'tenant1' }, securityContext: { tenantId: 'tenant1' }, requestId: 'XXX' };
    for (let i = 0; i < 1000; i++) {
      try {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const refreshResult = await refreshScheduler.runScheduledRefresh(ctx, {
          concurrency: 1,
          workerIndices: [0],
          throwErrors: true,
        });
        break;
      } catch (e) {
        if ((<{ error: string }>e).error !== 'Continue wait') {
          throw e;
        } else {
          // eslint-disable-next-line no-continue
          continue;
        }
      }
    }
  });
});
