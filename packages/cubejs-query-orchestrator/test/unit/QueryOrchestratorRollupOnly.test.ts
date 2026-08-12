/* eslint-disable @typescript-eslint/no-explicit-any */
import { QueryOrchestrator } from '../../src/orchestrator/QueryOrchestrator';

/**
 * Rollup only mode must fail a pre-aggregation miss on BOTH query entry points.
 * `streamQuery` (the SQL API transport) used to skip the check and fall through
 * to the source database, so losing acceleration was silent.
 */

class MockDriver {
  public tables: string[] = [];

  public executedQueries: string[] = [];

  public now: number = Date.now();

  public schema: string | null = null;

  public query(query: string): Promise<string[]> & { cancel?: () => Promise<void> } {
    this.executedQueries.push(query);
    const promise: Promise<string[]> & { cancel?: () => Promise<void> } = Promise.resolve([query]);
    promise.cancel = async () => undefined;
    return promise;
  }

  public async getTablesQuery(schema: string) {
    return this.tables.map(t => ({ table_name: t.replace(`${schema}.`, '') }));
  }

  public async createSchemaIfNotExists(schema: string) {
    this.schema = schema;
    return null;
  }

  public loadPreAggregationIntoTable(preAggregationTableName: string, loadSql: string) {
    this.tables.push(preAggregationTableName.substring(0, 100));
    return this.query(loadSql);
  }

  public async dropTable(tableName: string) {
    this.tables = this.tables.filter(t => t !== tableName);
    return this.query(`DROP TABLE ${tableName}`);
  }

  public async downloadTable(table: string) {
    return { rows: await this.query(`SELECT * FROM ${table}`) };
  }

  public async tableColumnTypes(_table: string) {
    return [];
  }

  public async uploadTable(table: string, columns: any, _tableData: any) {
    await this.createTable(table, columns);
  }

  public createTable(quotedTableName: string, _columns: any) {
    this.tables.push(quotedTableName);
  }

  public readOnly() {
    return false;
  }

  public nowTimestamp() {
    return this.now;
  }

  public async release() {
    return undefined;
  }
}

const currentHourQuery = [
  'SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour',
  [],
];

// Partial query bodies: these fixtures carry only the fields the orchestrator reads
// on the way to the rollup-only guard, so they're cast rather than fully populated.
// No `preAggregations`, so nothing is loaded and the query hits the source database.
const missQuery = (requestId: string): any => ({
  query: 'SELECT "orders__status" "orders__status", count("orders".id) "orders__count" FROM public.orders AS "orders" GROUP BY 1 ORDER BY 1 ASC LIMIT 10000',
  values: [],
  cacheKeyQueries: {
    renewalThreshold: 21600,
    queries: [currentHourQuery],
  },
  preAggregations: [],
  cacheMode: 'must-revalidate' as const,
  requestId,
});

const hitQuery = (requestId: string): any => ({
  query: 'SELECT "orders__status" "orders__status", sum("orders__count") "orders__count" FROM (SELECT * FROM stb_pre_aggregations.orders_status_and_count) as partition_union GROUP BY 1 ORDER BY 1 ASC LIMIT 10000',
  values: [],
  cacheKeyQueries: {
    renewalThreshold: 21600,
    queries: [currentHourQuery],
  },
  preAggregations: [{
    preAggregationsSchema: 'stb_pre_aggregations',
    tableName: 'stb_pre_aggregations.orders_status_and_count',
    loadSql: ['CREATE TABLE stb_pre_aggregations.orders_status_and_count AS SELECT\n      "orders".status "orders__status", count("orders".id) "orders__count"\n    FROM\n      public.orders AS "orders" GROUP BY 1', []],
    invalidateKeyQueries: [currentHourQuery],
  }],
  cacheMode: 'must-revalidate' as const,
  requestId,
});

const rollupOnlyError = /No pre-aggregation table has been built for this query yet/;

describe('QueryOrchestrator rollup only mode', () => {
  jest.setTimeout(15000);

  let mockDriver: MockDriver;
  let externalMockDriver: MockDriver;
  let rollupOnlyOrchestrator: QueryOrchestrator;
  let orchestrator: QueryOrchestrator;
  let testCount = 1;

  beforeEach(() => {
    mockDriver = new MockDriver();
    externalMockDriver = new MockDriver();

    const redisPrefix = `ROLLUP_ONLY_TEST_${testCount++}`;
    const driverFactory = (() => mockDriver) as any;
    const logger = (msg: string, params: any) => console.log(new Date().toJSON(), msg, params);
    const options = {
      externalDriverFactory: (() => externalMockDriver) as any,
      queryCacheOptions: {
        queueOptions: () => ({ concurrency: 2 }),
      },
      preAggregationsOptions: {
        maxPartitions: 100,
        queueOptions: () => ({ executionTimeout: 2, concurrency: 2 }),
        usedTablePersistTime: 1,
      },
    };

    rollupOnlyOrchestrator = new QueryOrchestrator(
      redisPrefix,
      driverFactory,
      logger,
      { ...options, rollupOnlyMode: true },
    );
    orchestrator = new QueryOrchestrator(
      `${redisPrefix}_OFF`,
      driverFactory,
      logger,
      options,
    );
  });

  afterEach(async () => {
    await rollupOnlyOrchestrator.cleanup();
    await orchestrator.cleanup();
  });

  test('rejects a pre-aggregation miss on streamQuery', async () => {
    await expect(
      rollupOnlyOrchestrator.streamQuery(missQuery('rollup only stream miss'))
    ).rejects.toThrow(rollupOnlyError);

    // The guard runs before the stream exists, so the source database is never queried.
    expect(mockDriver.executedQueries.join('\n')).not.toMatch(/FROM public\.orders AS "orders" GROUP BY 1/);
  });

  test('rejects a pre-aggregation miss on fetchQuery', async () => {
    await expect(
      rollupOnlyOrchestrator.fetchQuery(missQuery('rollup only fetch miss'))
    ).rejects.toThrow(rollupOnlyError);
  });

  // The SQL API's native transport retries an error whose message IS
  // "continue wait" (`scan.rs` compares the whole string, case-insensitively),
  // so a miss worded that way would spin instead of failing. The assertion
  // below is deliberately stricter than that equality: a message merely
  // mentioning a continue wait would be misleading even where it can't retry.
  test('the miss error is not mistaken for a continue wait', async () => {
    await expect(
      rollupOnlyOrchestrator.streamQuery(missQuery('rollup only wording'))
    ).rejects.toThrow(
      expect.objectContaining({
        message: expect.not.stringMatching(/continue wait/i),
      })
    );
  });

  test('serves streamQuery when a pre-aggregation matched', async () => {
    const result = await rollupOnlyOrchestrator.streamQuery(hitQuery('rollup only stream hit'));

    expect(result).toBeDefined();
    expect(
      mockDriver.tables.concat(externalMockDriver.tables).join('\n')
    ).toMatch(/orders_status_and_count/);
  });

  test('lets a pre-aggregation miss fall through when rollup only mode is off', async () => {
    const result = await orchestrator.fetchQuery(missQuery('rollup only disabled'));

    expect(result.data[0]).toMatch(/FROM public\.orders AS "orders"/);
  });
});
