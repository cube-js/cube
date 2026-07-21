/* eslint-disable @typescript-eslint/no-explicit-any */
import { PreAggregationLoadCache, PreAggregationLoader, PreAggregations, QueryCache } from '../../src';

class MockDriver {
  public tables: string[] = [];

  public async query(query: string) {
    return [query];
  }

  public async getTablesQuery(schema: string) {
    return this.tables.map(t => ({ table_name: t.replace(`${schema}.`, '') }));
  }

  public async createSchemaIfNotExists() {
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

  public readOnly() {
    return false;
  }
}

describe('PreAggregations touch/used cache key cleanup', () => {
  let mockDriver: MockDriver;
  let mockDriverFactory: () => Promise<MockDriver>;
  let queryCache: QueryCache;
  let preAggregations: PreAggregations;

  beforeEach(() => {
    mockDriver = new MockDriver();
    mockDriverFactory = async () => mockDriver;

    queryCache = new QueryCache(
      'TEST',
      mockDriverFactory as any,
      // eslint-disable-next-line @typescript-eslint/no-empty-function
      () => {},
      {
        cacheAndQueueDriver: 'memory',
        queueOptions: async () => ({
          executionTimeout: 1,
          concurrency: 2,
        }),
      },
    );

    preAggregations = new PreAggregations(
      'TEST',
      mockDriverFactory as any,
      // eslint-disable-next-line @typescript-eslint/no-empty-function
      () => {},
      queryCache,
      {
        queueOptions: async () => ({
          executionTimeout: 1,
          concurrency: 2,
        }),
      },
    );
  });

  test('removeTableUsed / removeTableTouched drop keys and evict the in-memory LRU', async () => {
    const table = 'stb_pre_aggregations.orders_abc_def_1';

    await preAggregations.addTableUsed(table);
    await preAggregations.updateLastTouch(table);

    expect(await preAggregations.tablesUsed()).toContain(table);
    expect(await preAggregations.tablesTouched()).toContain(table);

    await preAggregations.removeTableUsed(table);
    await preAggregations.removeTableTouched(table);

    expect(await preAggregations.tablesUsed()).not.toContain(table);
    expect(await preAggregations.tablesTouched()).not.toContain(table);

    // Re-adding must succeed. If the LRU guard weren't evicted, the has()
    // short-circuit in addTableUsed/updateLastTouch would suppress the write
    // and the keys would stay absent.
    await preAggregations.addTableUsed(table);
    await preAggregations.updateLastTouch(table);

    expect(await preAggregations.tablesUsed()).toContain(table);
    expect(await preAggregations.tablesTouched()).toContain(table);
  });

  test('failed pre-aggregation build drops its touch and used keys', async () => {
    const preAggregation = {
      preAggregationsSchema: 'stb_pre_aggregations',
      tableName: 'stb_pre_aggregations.orders_number_and_count',
      dataSource: 'default',
      external: false,
      loadSql: ['CREATE TABLE stb_pre_aggregations.orders_number_and_count AS SELECT 1', []],
      invalidateKeyQueries: [],
    };

    const newVersionEntry = {
      table_name: 'stb_pre_aggregations.orders_number_and_count',
      structure_version: 'aaaa1111',
      content_version: 'bbbb2222',
      last_updated_at: 1600000000000,
      naming_version: 2,
    };

    const targetTableName = PreAggregations.targetTableName(newVersionEntry);

    // Simulate a build that fails inside the datasource.
    jest.spyOn(mockDriver, 'loadPreAggregationIntoTable')
      .mockRejectedValue(new Error('build boom'));

    const loadCache = new PreAggregationLoadCache(
      mockDriverFactory as any,
      queryCache,
      preAggregations,
      { dataSource: 'default' },
    );

    const loader = new PreAggregationLoader(
      mockDriverFactory as any,
      // eslint-disable-next-line @typescript-eslint/no-empty-function
      () => {},
      queryCache,
      preAggregations,
      preAggregation,
      [],
      loadCache,
      { requestId: 'failed-build' },
    );

    await expect(loader.refresh(newVersionEntry as any, [] as any, mockDriver))
      .rejects.toThrow('build boom');

    // The failed attempt must leave no touch/used markers behind, otherwise
    // repeated failures accumulate keys in cache until TTL (CORE-646).
    expect(await preAggregations.tablesTouched()).not.toContain(targetTableName);
    expect(await preAggregations.tablesUsed()).not.toContain(targetTableName);
  });
});
