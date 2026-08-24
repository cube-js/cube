/* eslint-disable @typescript-eslint/no-explicit-any */
import { QueryOrchestrator } from '../../src/orchestrator/QueryOrchestrator';

class MockDriver {
  public tablesObj: any[] = [];

  public executedQueries: any[] = [];

  public droppedTables: string[] = [];

  public now: number = Date.now();

  public schema: string | undefined;

  public get tables(): string[] {
    return this.tablesObj.map(t => t.tableName || t);
  }

  public query(query: any, _params?: any): any {
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

  public async getTablesQuery(schema: string) {
    return this.tablesObj
      .filter(t => (t.tableName || t).split('.')[0] === schema)
      .map(t => ({ table_name: (t.tableName || t).replace(`${schema}.`, '') }));
  }

  public async createSchemaIfNotExists(schema: string) {
    this.schema = schema;
    return null;
  }

  public loadPreAggregationIntoTable(preAggregationTableName: string, loadSql: string) {
    this.tablesObj.push({ tableName: preAggregationTableName.substring(0, 100) });
    return this.query(loadSql);
  }

  public async dropTable(tableName: string) {
    this.droppedTables.push(tableName);
    this.tablesObj = this.tablesObj.filter(t => (t.tableName || t) !== tableName);
  }

  public async downloadTable(table: string) {
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

type ImportBehavior = 'ok' | 'fail' | 'hang';

/**
 * Emulates Cube Store. `importBehavior` reproduces the shape from the incident:
 * `CREATE TABLE` lands (the versioned table becomes visible to `getTablesQuery`)
 * and only then rows are imported, so there is a window where an unfinished
 * build already looks like a complete one from the outside.
 */
class ExternalMockDriver extends MockDriver {
  public indexes: any[] = [];

  public importBehavior: ImportBehavior = 'ok';

  public importStarted: Promise<void> = Promise.resolve();

  private importStartedResolve: () => void = () => undefined;

  public constructor() {
    super();
    this.importStarted = new Promise(resolve => {
      this.importStartedResolve = resolve as () => void;
    });
  }

  public async uploadTableWithIndexes(table: string, columns: any, tableData: any, indexesSql: any[]) {
    // CREATE TABLE: the versioned table is registered and queryable from now on
    this.tablesObj.push({ tableName: table.substring(0, 100) });
    this.importStartedResolve();

    if (this.importBehavior === 'fail') {
      // INSERTs never land and the table is left behind as a `ready` 0-row ghost
      throw new Error('Import into Cube Store failed after CREATE TABLE');
    }

    if (this.importBehavior === 'hang') {
      // The import is still running: the process may be killed at any moment here
      await new Promise(() => undefined);
    }

    for (let i = 0; i < indexesSql.length; i++) {
      const [query, params] = indexesSql[i].sql;
      await this.query(query, params);
    }
    this.indexes = this.indexes.concat(indexesSql);
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
  sql: ['SELECT * FROM public.orders', []],
  invalidateKeyQueries: [['SELECT NOW()', [], { renewalThreshold: 21600 }]],
  indexesSql: [{
    sql: ['CREATE INDEX orders_month_main ON stb_pre_aggregations.orders_month ("orders__created_at")', []],
    indexName: 'orders_month_main',
  }],
  external: true,
  dataSource: 'default',
  timezone: 'UTC',
};

const jobQuery = (requestId: string) => ({
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

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const waitFor = async (predicate: () => boolean | Promise<boolean>, timeout = 10000) => {
  const started = Date.now();
  // eslint-disable-next-line no-await-in-loop
  while (!(await predicate())) {
    if (Date.now() - started > timeout) {
      throw new Error('Timeout while waiting for a condition');
    }
    // eslint-disable-next-line no-await-in-loop
    await delay(20);
  }
};

describe('pre-aggregation build jobs', () => {
  jest.setTimeout(20000);

  let mockDriver: MockDriver;
  let externalMockDriver: ExternalMockDriver;
  let orchestrator: QueryOrchestrator;
  let orchestratorExternalRefresh: QueryOrchestrator;
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

    orchestrator = new QueryOrchestrator(prefix, driverFactory as any, logger, options as any);
    orchestratorExternalRefresh = new QueryOrchestrator(prefix, driverFactory as any, logger, {
      ...options,
      preAggregationsOptions: {
        ...options.preAggregationsOptions,
        externalRefresh: true,
      },
    } as any);
  });

  afterEach(async () => {
    await orchestrator.cleanup();
    await orchestratorExternalRefresh.cleanup();
  });

  const pollUntilSettled = async (requestId: string, targetTableName: string, queryKey: any) => {
    let status = 'processing';

    await waitFor(async () => {
      [, status] = await orchestrator.isPartitionExist(
        requestId,
        true,
        'default',
        'stb_pre_aggregations',
        targetTableName,
        queryKey,
        'test-token',
      );
      return status !== 'processing';
    });

    return status;
  };

  // `externalRefresh` stops a query from building a pre-aggregation as a side
  // effect. A build job is an explicit request to build, so it runs anyway —
  // posting jobs to an instance that serves queries only is a supported setup.
  test('jobs-API build runs on an externalRefresh instance', async () => {
    await expect(
      orchestratorExternalRefresh.fetchQuery({
        query: 'SELECT * FROM stb_pre_aggregations.orders_month',
        values: [],
        cacheKeyQueries: { queries: [] },
        preAggregations: [PRE_AGGREGATION],
        requestId: 'externalRefresh regular query',
        external: true,
      } as any)
    ).rejects.toThrow(/refresh worker/);

    expect(externalMockDriver.tables.length).toEqual(0);

    await orchestratorExternalRefresh.fetchQuery(jobQuery('externalRefresh job query') as any);

    await waitFor(() => externalMockDriver.indexes.length > 0);
  });

  test('completed jobs-API build is reported as done', async () => {
    const result = await orchestrator.fetchQuery(jobQuery('successful job query') as any);
    const [{ targetTableName, queryKey }] = result;

    await waitFor(() => externalMockDriver.indexes.length > 0);

    const [, status] = await orchestrator.isPartitionExist(
      'successful job query',
      true,
      'default',
      'stb_pre_aggregations',
      targetTableName,
      queryKey,
      'test-token',
    );

    expect(status).toEqual('done');
  });

  // The queue de-duplicates on the query key, so a job regularly ends up
  // waiting on a build some other request enqueued. That build has to record
  // its outcome as well, or the job falls back to the bare table again.
  test('build started by a regular query records its outcome', async () => {
    externalMockDriver.importBehavior = 'hang';

    orchestrator.fetchQuery({
      query: 'SELECT * FROM stb_pre_aggregations.orders_month',
      values: [],
      cacheKeyQueries: { queries: [] },
      preAggregations: [PRE_AGGREGATION],
      requestId: 'regular query build',
      external: true,
    } as any).catch(() => undefined);

    await externalMockDriver.importStarted;
    const [targetTableName] = externalMockDriver.tables;

    const [, status] = await orchestrator.isPartitionExist(
      'regular query build',
      true,
      'default',
      'stb_pre_aggregations',
      targetTableName,
      ['some key'],
      'test-token',
    );

    expect(status).toEqual('processing');
  });

  // A build that is still importing rows must not be reported as `done` just
  // because the versioned table is already visible.
  test('in-flight jobs-API build is not reported as done', async () => {
    externalMockDriver.importBehavior = 'hang';

    const result = await orchestrator.fetchQuery(jobQuery('in-flight job query') as any);
    const [{ targetTableName, queryKey }] = result;

    await externalMockDriver.importStarted;

    // The table is registered in Cube Store while the import is still running.
    expect(externalMockDriver.tables).toContain(targetTableName);

    const [, status] = await orchestrator.isPartitionExist(
      'in-flight job query',
      true,
      'default',
      'stb_pre_aggregations',
      targetTableName,
      queryKey,
      'test-token',
    );

    expect(status).not.toEqual('done');
  });

  // A failed jobs-API build must surface as `failure`, not as `done`.
  test('failed jobs-API build is reported as failure', async () => {
    externalMockDriver.importBehavior = 'fail';

    const result = await orchestrator.fetchQuery(jobQuery('failing job query') as any);
    const [{ targetTableName, queryKey }] = result;

    await externalMockDriver.importStarted;
    const status = await pollUntilSettled('failing job query', targetTableName, queryKey);

    expect(status).toMatch(/^failure/);
  });

  // A build error must win over the table existence even when the queue result
  // has already been consumed by an earlier status poll.
  test('failure status survives repeated status polls', async () => {
    externalMockDriver.importBehavior = 'fail';

    const result = await orchestrator.fetchQuery(jobQuery('repeated poll job query') as any);
    const [{ targetTableName, queryKey }] = result;

    await externalMockDriver.importStarted;
    await pollUntilSettled('repeated poll job query', targetTableName, queryKey);

    const [, status] = await orchestrator.isPartitionExist(
      'repeated poll job query',
      true,
      'default',
      'stb_pre_aggregations',
      targetTableName,
      queryKey,
      'test-token',
    );

    expect(status).toMatch(/^failure/);
  });

  // A build whose process is gone never reports an outcome of its own, and the
  // job must not be left waiting on it until the record expires.
  test('abandoned build is reported as failure', async () => {
    const result = await orchestrator.fetchQuery(jobQuery('abandoned job query') as any);
    const [{ targetTableName, queryKey }] = result;

    await waitFor(() => externalMockDriver.indexes.length > 0);

    // executionTimeout is 2s for this orchestrator, so this build can't still be running
    await orchestrator.getPreAggregations().setPreAggregationBuildStatus(targetTableName, {
      status: 'building',
      startedAt: new Date().getTime() - 60000,
    });

    const [, status] = await orchestrator.isPartitionExist(
      'abandoned job query',
      true,
      'default',
      'stb_pre_aggregations',
      targetTableName,
      queryKey,
      'test-token',
    );

    expect(status).toMatch(/^failure/);
  });

  // Only the last pre-aggregation of a job build query is force built, so a
  // dependency that is already up to date never runs a build and never records
  // an outcome. Its job still has to report the partition as built.
  test('partition that exists without a recorded build is reported as done', async () => {
    const result = await orchestrator.fetchQuery(jobQuery('dependency job query') as any);
    const [{ queryKey }] = result;

    await waitFor(() => externalMockDriver.indexes.length > 0);

    const dependencyTableName = 'stb_pre_aggregations.orders_day_wd2ap0ny_i2isylsi_1jr2sxb';
    externalMockDriver.tablesObj.push({ tableName: dependencyTableName });

    const [, status] = await orchestrator.isPartitionExist(
      'dependency job query',
      true,
      'default',
      'stb_pre_aggregations',
      dependencyTableName,
      queryKey,
      'test-token',
    );

    expect(status).toEqual('done');
  });

  // A partially built external table must not be left behind: it is the newest
  // version of the partition and would shadow the previous correct table forever.
  test('failed external import drops the partially built table', async () => {
    externalMockDriver.importBehavior = 'fail';

    const result = await orchestrator.fetchQuery(jobQuery('dropping job query') as any);
    const [{ targetTableName }] = result;

    await waitFor(() => externalMockDriver.droppedTables.includes(targetTableName));

    expect(externalMockDriver.tables).not.toContain(targetTableName);
  });
});
