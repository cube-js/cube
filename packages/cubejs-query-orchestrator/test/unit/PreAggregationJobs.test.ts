/* globals jest, describe, beforeEach, afterEach, test, expect */
import { QueryOrchestrator } from '../../src/orchestrator/QueryOrchestrator';

class MockDriver {
  public tablesObj: any[];

  public executedQueries: any[];

  public droppedTables: string[];

  public now: number;

  public schema: string | undefined;

  public constructor() {
    this.tablesObj = [];
    this.executedQueries = [];
    this.droppedTables = [];
    this.now = new Date().getTime();
  }

  public get tables() {
    return this.tablesObj.map(t => t.tableName || t);
  }

  public query(query, _params?: any) {
    this.executedQueries.push(query);

    if (typeof query !== 'string') {
      return Promise.resolve([]);
    }

    let promise: any = Promise.resolve([query]);

    if (query.match(/^SELECT NOW\(\)$/)) {
      promise = promise.then(() => [{ now: new Date().toJSON() }]);
    }

    promise.cancel = () => undefined;
    return promise;
  }

  public async getTablesQuery(schema) {
    return this.tablesObj
      .filter(t => (t.tableName || t).split('.')[0] === schema)
      .map(t => ({ table_name: (t.tableName || t).replace(`${schema}.`, '') }));
  }

  public async createSchemaIfNotExists(schema) {
    this.schema = schema;
    return null;
  }

  public loadPreAggregationIntoTable(preAggregationTableName, loadSql) {
    this.tablesObj.push({ tableName: preAggregationTableName.substring(0, 100) });
    return this.query(loadSql);
  }

  public async dropTable(tableName) {
    this.droppedTables.push(tableName);
    this.tablesObj = this.tablesObj.filter(t => (t.tableName || t) !== tableName);
  }

  public async downloadTable(table) {
    return { rows: await this.query(`SELECT * FROM ${table}`) };
  }

  public async tableColumnTypes() {
    return [{ name: 'foo', type: 'int' }];
  }

  public nowTimestamp() {
    return this.now;
  }

  public capabilities() {
    return {};
  }
}

/**
 * Emulates Cube Store. `importFails` reproduces the shape from the incident:
 * `CREATE TABLE` lands (the versioned table becomes visible/queryable) and
 * then the row import dies without the table being dropped - exactly what
 * happens when the executing process is killed mid-INSERT, or when
 * `importRows` receives an empty row set.
 */
class ExternalMockDriver extends MockDriver {
  public indexes: any[];

  public importFails: boolean;

  public constructor() {
    super();
    this.indexes = [];
    this.importFails = false;
  }

  public async uploadTableWithIndexes(table, columns, tableData, indexesSql) {
    // CREATE TABLE - the versioned table is registered and queryable from now on
    this.tablesObj.push({ tableName: table.substring(0, 100) });

    if (this.importFails) {
      // INSERTs never land, and (process killed / empty result set) the table
      // is left behind as a `ready` 0-row "ghost" partition.
      throw new Error('Import into Cube Store failed after CREATE TABLE');
    }

    for (let i = 0; i < indexesSql.length; i++) {
      const [query, params] = indexesSql[i].sql;
      await this.query(query, params);
    }
    this.indexes = this.indexes.concat(indexesSql);
  }

  public capabilities() {
    return {};
  }
}

const PRE_AGGREGATION = {
  preAggregationsSchema: 'stb_pre_aggregations',
  tableName: 'stb_pre_aggregations.orders_month',
  preAggregationId: 'orders.month',
  loadSql: [
    'CREATE TABLE stb_pre_aggregations.orders_month AS SELECT * FROM public.orders',
    [],
  ],
  invalidateKeyQueries: [['SELECT NOW()', [], { renewalThreshold: 21600 }]],
  indexesSql: [{
    sql: ['CREATE INDEX orders_month_main ON stb_pre_aggregations.orders_month ("orders__created_at")', []],
    indexName: 'orders_month_main',
  }],
  external: true,
  dataSource: 'default',
  timezone: 'UTC',
};

const jobQuery = (requestId) => ({
  // The jobs API posts a build query without `query`
  values: [],
  cacheKeyQueries: { queries: [] },
  preAggregations: [PRE_AGGREGATION],
  continueWait: true,
  renewQuery: true,
  requestId,
  isJob: true,
  forceBuildPreAggregations: true,
  external: true,
});

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

describe('pre-aggregation build jobs (#11609)', () => {
  jest.setTimeout(15000);

  let mockDriver;
  let externalMockDriver;
  let orchestrator;
  let orchestratorExternalRefresh;
  let testCount = 1;

  beforeEach(() => {
    mockDriver = new MockDriver();
    externalMockDriver = new ExternalMockDriver();

    const prefix = `PRE_AGG_JOBS_TEST_${testCount++}`;
    const driverFactory = () => mockDriver;
    const logger = () => undefined;

    const options = {
      externalDriverFactory: () => externalMockDriver,
      queryCacheOptions: {
        queueOptions: () => ({ concurrency: 2, processUid: 'p1' }),
      },
      preAggregationsOptions: {
        maxPartitions: 100,
        queueOptions: () => ({ executionTimeout: 2, concurrency: 2, processUid: 'p1' }),
        usedTablePersistTime: 1,
      },
    };

    orchestrator = new QueryOrchestrator(prefix, driverFactory, logger, options);
    orchestratorExternalRefresh = new QueryOrchestrator(prefix, driverFactory, logger, {
      ...options,
      preAggregationsOptions: {
        ...options.preAggregationsOptions,
        externalRefresh: true,
      },
    });
  });

  afterEach(async () => {
    await orchestrator.cleanup();
    await orchestratorExternalRefresh.cleanup();
  });

  // Defect 4: `isJob` short-circuits `loadPreAggregation` before the
  // `externalRefresh` guard is reached, so an API instance that is configured
  // to never build pre-aggregations still runs jobs-API builds in-process.
  test('jobs-API build bypasses the externalRefresh guard', async () => {
    // Sanity check: a normal query on an externalRefresh instance refuses to build.
    await expect(
      orchestratorExternalRefresh.fetchQuery({
        query: 'SELECT * FROM stb_pre_aggregations.orders_month',
        values: [],
        cacheKeyQueries: { queries: [] },
        preAggregations: [PRE_AGGREGATION],
        requestId: 'externalRefresh regular query',
        external: true,
      })
    ).rejects.toThrow(/refresh worker/);

    expect(externalMockDriver.tables.length).toEqual(0);

    // ...but the very same instance happily builds when the query is a job.
    await orchestratorExternalRefresh.fetchQuery(jobQuery('externalRefresh job query'));
    await delay(1000);

    expect(externalMockDriver.tables.length).toEqual(0);
  });

  // Defects 1 + 2: a failed jobs-API build is reported as `done`, because
  // `isPartitionExist` treats "a table with the target name exists" as
  // "the build succeeded", and the fire-and-forget `.catch` in the
  // `forceBuild && isJob` branch never writes `failure` to the job entry.
  test('failed jobs-API build is not reported as done', async () => {
    externalMockDriver.importFails = true;

    const result = await orchestrator.fetchQuery(jobQuery('failing job query'));
    const [{ targetTableName, queryKey }] = result;

    // Let the fire-and-forget build reject.
    await delay(1500);

    // The ghost partition: registered and queryable, but the import failed.
    expect(externalMockDriver.tables).toContain(targetTableName);
    expect(externalMockDriver.droppedTables).not.toContain(targetTableName);

    const [, status] = await orchestrator.isPartitionExist(
      'failing job query',
      true,
      'default',
      'stb_pre_aggregations',
      targetTableName,
      queryKey,
      'test-token',
    );

    expect(status).not.toEqual('done');
  });
});
