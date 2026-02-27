/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable global-require */
import R from 'ramda';
import {
  BUILD_RANGE_END_LOCAL,
  BUILD_RANGE_START_LOCAL,
  FROM_PARTITION_RANGE,
  TO_PARTITION_RANGE
} from '@cubejs-backend/shared';
import { PreAggregationPartitionRangeLoader, PreAggregations, version } from '../../src';

class MockDriver {
  public tables: string[] = [];

  public executedQueries: string[] = [];

  public cancelledQueries: string[] = [];

  public now: number = Date.now();

  public schema: string | null = null;

  public query(query: string): Promise<string[]> & { cancel?: () => Promise<void> } {
    this.executedQueries.push(query);
    const promise: Promise<string[]> & { cancel?: () => Promise<void> } = query.match('orders_too_big')
      ? new Promise(resolve => setTimeout(() => resolve([query]), 3000))
      : Promise.resolve([query]);
    promise.cancel = async () => {
      this.cancelledQueries.push(query);
    };
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
}

const mockPreAggregation = (overrides: Record<string, any> = {}) => ({
  tableName: 'test_table',
  partitionGranularity: 'day',
  timezone: 'UTC',
  timestampFormat: 'YYYY-MM-DDTHH:mm:ss.SSS',
  timestampPrecision: 3,
  dataSource: 'default',
  partitionInvalidateKeyQueries: [],
  preAggregationStartEndQueries: [
    ['SELECT MIN(ts)', [], {}],
    ['SELECT MAX(ts)', [], {}]
  ],
  loadSql: ['CREATE TABLE test_table AS SELECT * FROM source_table WHERE ts >= $1 and ts <= $2', [FROM_PARTITION_RANGE, TO_PARTITION_RANGE]],
  sql: ['SELECT * FROM source_table WHERE ts >= $1 and ts <= $2', [FROM_PARTITION_RANGE, TO_PARTITION_RANGE]],
  previewSql: ['SELECT * FROM SELECT * FROM dev_pre_aggregations.test_table__daily LIMIT 1000', []],
  ...overrides,
});

const createLoader = (overrides: Record<string, any> = {}, options: Record<string, any> = {}) => {
  const loader = new PreAggregationPartitionRangeLoader(
    {} as any, // driverFactory
    {} as any, // logger
    { options: {} } as any, // queryCache
    {} as any, // preAggregations
    mockPreAggregation(overrides) as any,
    [], // preAggregationsTablesToTempTables
    {} as any, // loadCache
    options as any,
  );

  jest.spyOn(loader as any, 'loadRangeQuery').mockImplementation(async (query: any, _partitionRange: any) => {
    if (query[0].includes('MIN')) {
      return [{ value: '2024-01-01T00:00:00.000' }];
    }
    return [{ value: '2024-01-03T23:59:59.999' }];
  });

  return loader;
};

describe('PreAggregations', () => {
  let mockDriver: MockDriver | null = null;
  let mockExternalDriver: MockDriver | null = null;
  let mockDriverFactory: (() => Promise<MockDriver>) | null = null;
  let mockDriverReadOnlyFactory: (() => Promise<MockDriver>) | null = null;
  let mockExternalDriverFactory: (() => Promise<MockDriver>) | null = null;
  let queryCache: any = null;

  const basicQuery: any = {
    query: 'SELECT "orders__created_at_week" "orders__created_at_week", sum("orders__count") "orders__count" FROM (SELECT * FROM stb_pre_aggregations.orders_number_and_count20191101) as partition_union  WHERE ("orders__created_at_week" >= ($1::timestamptz::timestamptz AT TIME ZONE \'UTC\') AND "orders__created_at_week" <= ($2::timestamptz::timestamptz AT TIME ZONE \'UTC\')) GROUP BY 1 ORDER BY 1 ASC LIMIT 10000',
    values: ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z'],
    cacheKeyQueries: {
      renewalThreshold: 21600,
      queries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', [], {
        renewalThreshold: 10,
        external: false,
      }]]
    },
    preAggregations: [{
      preAggregationsSchema: 'stb_pre_aggregations',
      tableName: 'stb_pre_aggregations.orders_number_and_count20191101',
      loadSql: ['CREATE TABLE stb_pre_aggregations.orders_number_and_count20191101 AS SELECT\n      date_trunc(\'week\', ("orders".created_at::timestamptz AT TIME ZONE \'UTC\')) "orders__created_at_week", count("orders".id) "orders__count", sum("orders".number) "orders__number"\n    FROM\n      public.orders AS "orders"\n  WHERE ("orders".created_at >= $1::timestamptz AND "orders".created_at <= $2::timestamptz) GROUP BY 1', ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z']],
      invalidateKeyQueries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', [], {
        renewalThreshold: 10,
        external: false,
      }]]
    }],
    requestId: 'basic'
  };

  const basicQueryExternal = R.clone(basicQuery);
  basicQueryExternal.preAggregations[0].external = true;
  const basicQueryWithRenew = R.clone(basicQuery);
  basicQueryWithRenew.renewQuery = true;
  const basicQueryExternalWithRenew = R.clone(basicQueryExternal);
  basicQueryExternalWithRenew.renewQuery = true;

  beforeEach(() => {
    mockDriver = new MockDriver();
    mockExternalDriver = new MockDriver();
    mockDriverFactory = async () => mockDriver!;
    mockDriverReadOnlyFactory = async () => {
      const driver = mockDriver!;
      jest.spyOn(driver, 'readOnly').mockImplementation(() => true);
      return driver;
    };
    mockExternalDriverFactory = async () => {
      const driver = mockExternalDriver!;
      driver.createTable('stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il_1593709044209', null);
      return driver;
    };

    jest.resetModules();

    // Dynamic require after resetModules to ensure fresh module state
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { QueryCache } = require('../../src/orchestrator/QueryCache');
    queryCache = new QueryCache(
      'TEST',
      mockDriverFactory as any,
      // eslint-disable-next-line @typescript-eslint/no-empty-function
      () => {},
      {
        cacheAndQueueDriver: 'memory',
        queueOptions: () => ({
          executionTimeout: 1,
          concurrency: 2,
        }),
      },
    );
  });

  describe('loadAllPreAggregationsIfNeeded', () => {
    let preAggregations: PreAggregations | null = null;

    beforeEach(async () => {
      preAggregations = new PreAggregations(
        'TEST',
        mockDriverFactory as any,
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        () => {},
        queryCache!,
        {
          queueOptions: () => ({
            executionTimeout: 1,
            concurrency: 2,
          }),
        },
      );
    });

    test('synchronously create rollup from scratch', async () => {
      mockDriver!.now = 12345000;
      const { preAggregationsTablesToTempTables: result } = await preAggregations!.loadAllPreAggregationsIfNeeded(basicQueryWithRenew);
      expect(result[0][1].targetTableName).toMatch(/stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il/);
      expect(result[0][1].lastUpdatedAt).toEqual(12345000);
    });
  });

  describe('loadAllPreAggregationsIfNeeded with external rollup and writable source', () => {
    let preAggregations: PreAggregations | null = null;

    beforeEach(async () => {
      preAggregations = new PreAggregations(
        'TEST',
        mockDriverFactory as any,
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        () => {},
        queryCache!,
        {
          queueOptions: () => ({
            executionTimeout: 1,
            concurrency: 2,
          }),
          externalDriverFactory: mockExternalDriverFactory as any,
        },
      );
    });

    test('refresh external preaggregation with a writable source (refreshImplTempTableExternalStrategy)', async () => {
      const { preAggregationsTablesToTempTables: result } = await preAggregations!.loadAllPreAggregationsIfNeeded(basicQueryExternal);
      expect(result[0][1].targetTableName).toMatch(/stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il/);
      expect(result[0][1].lastUpdatedAt).toEqual(1593709044209);
    });
  });

  describe('loadAllPreAggregationsIfNeeded with external rollup and readonly source', () => {
    let preAggregations: PreAggregations | null = null;

    beforeEach(async () => {
      preAggregations = new PreAggregations(
        'TEST',
        mockDriverReadOnlyFactory as any,
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        () => {},
        queryCache!,
        {
          queueOptions: () => ({
            executionTimeout: 1,
            concurrency: 2,
          }),
          externalDriverFactory: mockExternalDriverFactory as any,
        },
      );
    });

    test('refresh external preaggregation with a writable source (refreshImplStreamExternalStrategy)', async () => {
      const { preAggregationsTablesToTempTables: result } = await preAggregations!.loadAllPreAggregationsIfNeeded(basicQueryExternal);
      expect(result[0][1].targetTableName).toMatch(/stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il/);
      expect(result[0][1].lastUpdatedAt).toEqual(1593709044209);
    });
  });

  describe('loadAllPreAggregationsIfNeeded with externalRefresh true', () => {
    let preAggregations: PreAggregations | null = null;

    beforeEach(async () => {
      preAggregations = new PreAggregations(
        'TEST',
        mockDriverFactory as any,
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        () => {},
        queryCache!,
        {
          queueOptions: () => ({
            executionTimeout: 1,
            concurrency: 2,
          }),
          externalRefresh: true,
        },
      );
    });

    test('silently degrade waitForRenew when externalRefresh is true', async () => {
      await expect(preAggregations!.loadAllPreAggregationsIfNeeded(basicQueryWithRenew))
        .rejects.toThrowError(/No pre-aggregation partitions were built yet/);
    });

    test('fail if rollup doesn\'t already exist', async () => {
      await expect(preAggregations!.loadAllPreAggregationsIfNeeded(basicQuery))
        .rejects.toThrowError(/No pre-aggregation partitions were built yet/);
    });
  });

  describe('loadAllPreAggregationsIfNeeded with external rollup and externalRefresh true', () => {
    let preAggregations: PreAggregations | null = null;

    beforeEach(async () => {
      preAggregations = new PreAggregations(
        'TEST',
        () => { throw new Error('The source database factory should never be called when externalRefresh is true, as it will trigger testConnection'); },
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        () => {},
        queryCache!,
        {
          queueOptions: () => ({
            executionTimeout: 1,
            concurrency: 2,
          }),
          externalDriverFactory: mockExternalDriverFactory as any,
          externalRefresh: true,
        },
      );
    });

    test('silently degrade waitForRenew when externalRefresh is true', async () => {
      const { preAggregationsTablesToTempTables: result } = await preAggregations!.loadAllPreAggregationsIfNeeded(basicQueryExternalWithRenew);
      expect(result[0][1].targetTableName).toMatch(/stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il/);
      expect(result[0][1].lastUpdatedAt).toEqual(1593709044209);
    });

    test('load external preaggregation without communicating to the source database', async () => {
      const { preAggregationsTablesToTempTables: result } = await preAggregations!.loadAllPreAggregationsIfNeeded(basicQueryExternal);
      expect(result[0][1].targetTableName).toMatch(/stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il/);
      expect(result[0][1].lastUpdatedAt).toEqual(1593709044209);
    });
  });

  describe('naming_version tests', () => {
    let preAggregations: PreAggregations | null = null;

    beforeEach(async () => {
      preAggregations = new PreAggregations(
        'TEST',
        mockDriverFactory as any,
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        () => {},
        queryCache!,
        {
          queueOptions: () => ({
            executionTimeout: 1,
            concurrency: 2,
          }),
          externalDriverFactory: async () => {
            const driver = mockExternalDriver!;
            driver.createTable('stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il_1593709044209', null);
            driver.createTable('stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il_1fm6652', null);
            return driver;
          },
        },
      );
    });

    test('test for function targetTableName', () => {
      let result = PreAggregations.targetTableName({
        table_name: 'orders_number_and_count20191101',
        content_version: 'kjypcoio',
        structure_version: '5yftl5il',
        last_updated_at: 1600329890789,
      });
      expect(result).toEqual('orders_number_and_count20191101_kjypcoio_5yftl5il_1600329890789');

      result = PreAggregations.targetTableName({
        table_name: 'orders_number_and_count20191101',
        content_version: 'kjypcoio',
        structure_version: '5yftl5il',
        last_updated_at: 1600329890789,
        naming_version: 2
      });
      expect(result).toEqual('orders_number_and_count20191101_kjypcoio_5yftl5il_1fm6652');
    });

    test('naming_version and sort by last_updated_at', async () => {
      const { preAggregationsTablesToTempTables: result } = await preAggregations!.loadAllPreAggregationsIfNeeded(basicQueryExternal);
      expect(result[0][1].targetTableName).toMatch(/stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il_1fm6652/);
      expect(result[0][1].lastUpdatedAt).toEqual(1600329890000);
    });
  });

  describe('naming_version sort tests', () => {
    let preAggregations: PreAggregations | null = null;

    beforeEach(async () => {
      preAggregations = new PreAggregations(
        'TEST',
        mockDriverFactory as any,
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        () => {},
        queryCache!,
        {
          queueOptions: () => ({
            executionTimeout: 1,
            concurrency: 2,
          }),
          externalDriverFactory: async () => {
            const driver = mockExternalDriver!;
            driver.createTable('stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il_1893709044209', null);
            driver.createTable('stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il_1fm6652', null);
            return driver;
          },
        },
      );
    });

    test('naming_version and sort by last_updated_at', async () => {
      const { preAggregationsTablesToTempTables: result } = await preAggregations!.loadAllPreAggregationsIfNeeded(basicQueryExternal);
      expect(result[0][1].targetTableName).toMatch(/stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il_1893709044209/);
      expect(result[0][1].lastUpdatedAt).toEqual(1893709044209);
    });
  });

  describe('intersectDateRanges', () => {
    test('6 timestamps - valid intersection', () => {
      expect(PreAggregationPartitionRangeLoader.intersectDateRanges(
        ['2024-01-05T00:00:00.000000', '2024-01-05T23:59:59.999999'],
        ['2024-01-01T00:00:00.000000', '2024-01-31T23:59:59.999999'],
      )).toEqual(
        ['2024-01-05T00:00:00.000000', '2024-01-05T23:59:59.999999']
      );

      expect(PreAggregationPartitionRangeLoader.intersectDateRanges(
        ['2024-01-20T00:00:00.000000', '2024-02-05T23:59:59.999999'],
        ['2024-01-01T00:00:00.000000', '2024-01-31T23:59:59.999999'],
      )).toEqual(
        ['2024-01-20T00:00:00.000000', '2024-01-31T23:59:59.999999']
      );
    });

    test('3 timestamps - valid intersection', () => {
      expect(PreAggregationPartitionRangeLoader.intersectDateRanges(
        ['2024-01-05T00:00:00.000', '2024-01-05T23:59:59.999'],
        ['2024-01-01T00:00:00.000', '2024-01-31T23:59:59.999'],
      )).toEqual(
        ['2024-01-05T00:00:00.000', '2024-01-05T23:59:59.999']
      );
    });

    test('returns null if ranges do not overlap', () => {
      expect(
        PreAggregationPartitionRangeLoader.intersectDateRanges(
          ['2024-01-01T00:00:00.000', '2024-01-31T23:59:59.999'],
          ['2024-02-01T00:00:00.000', '2024-02-28T23:59:59.999']
        )
      ).toBeNull();
    });

    test('returns rangeA if rangeB is null', () => {
      expect(
        PreAggregationPartitionRangeLoader.intersectDateRanges(
          ['2024-01-01T00:00:00.000', '2024-01-31T23:59:59.999'],
          null
        )
      ).toEqual(['2024-01-01T00:00:00.000', '2024-01-31T23:59:59.999']);
    });

    test('returns rangeB if rangeA is null', () => {
      expect(
        PreAggregationPartitionRangeLoader.intersectDateRanges(
          null,
          ['2024-01-01T00:00:00.000', '2024-01-31T23:59:59.999']
        )
      ).toEqual(['2024-01-01T00:00:00.000', '2024-01-31T23:59:59.999']);
    });

    test('throws error if range is not a tuple of two strings', () => {
      expect(() => PreAggregationPartitionRangeLoader.intersectDateRanges(
        ['2024-01-01T00:00:00.000'] as any,
        ['2024-01-01T00:00:00.000', '2024-01-31T23:59:59.999']
      )).toThrow('Date range expected to be an array with 2 elements');

      expect(() => PreAggregationPartitionRangeLoader.intersectDateRanges(
        ['2024-01-01T00:00:00.000', '2024-01-31T23:59:59.999', '2024-01-01T00:00:00.000'] as any,
        ['2024-01-01T00:00:00.000', '2024-01-31T23:59:59.999']
      ))
        .toThrow('Date range expected to be an array with 2 elements');

      expect(() => PreAggregationPartitionRangeLoader.intersectDateRanges(
        ['2024-01-01T00:00:00', '2024-01-31T23:59:59.999'], // incorrect format
        ['2024-01-01T00:00:00.000', '2024-01-31T23:59:59.999']
      )).toThrow('Date range expected to be in YYYY-MM-DDTHH:mm:ss.SSS format');
    });
  });

  describe('partitionTableName', () => {
    test('should generate correct table names for different granularities', () => {
      const testDateRange: [string, string] = ['2024-01-05T12:34:56.789', '2024-01-05T23:59:59.999'];

      // Daily granularity
      expect(PreAggregationPartitionRangeLoader.partitionTableName(
        'test_table',
        'day',
        testDateRange
      )).toBe('test_table20240105');

      // Hourly granularity
      expect(PreAggregationPartitionRangeLoader.partitionTableName(
        'test_table',
        'hour',
        testDateRange
      )).toBe('test_table2024010512');

      // Minute granularity
      expect(PreAggregationPartitionRangeLoader.partitionTableName(
        'test_table',
        'minute',
        testDateRange
      )).toBe('test_table202401051234');
    });
  });

  describe('replaceQueryBuildRangeParams', () => {
    test('should replace BUILD_RANGE params with actual dates', async () => {
      const loader = createLoader();
      jest.spyOn(loader as any, 'loadBuildRange').mockResolvedValue([
        '2023-01-01T00:00:00.000',
        '2023-01-31T23:59:59.999',
      ]);

      const result = await loader.replaceQueryBuildRangeParams([
        'other_param_that_should_not_be_modified',
        BUILD_RANGE_START_LOCAL,
        BUILD_RANGE_END_LOCAL,
      ]);

      expect(result).toEqual([
        'other_param_that_should_not_be_modified',
        '2023-01-01T00:00:00.000',
        '2023-01-31T23:59:59.999',
      ]);
    });

    test('should return null when no BUILD_RANGE params', async () => {
      const loader = createLoader();
      const result = await loader.replaceQueryBuildRangeParams(['param1', 'param2']);
      expect(result).toBeNull();
    });
  });

  describe('PreAggregations without partitions', () => {
    test('should construct correct preAggregation without partitions', async () => {
      const loader = createLoader({
        timezone: 'UTC',
        partitionGranularity: undefined,
      });

      const results = await loader.partitionPreAggregations();
      expect(results.length).toEqual(1);

      const [preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table');
      expect(preAggDesc.buildRangeStart).toBeUndefined();
      expect(preAggDesc.buildRangeEnd).toBeUndefined();
      expect((preAggDesc.loadSql as any)[0].includes('test_table')).toBeTruthy();
      expect((preAggDesc.loadSql as any)[1][0]).toEqual(FROM_PARTITION_RANGE);
      expect((preAggDesc.loadSql as any)[1][1]).toEqual(TO_PARTITION_RANGE);
      expect(preAggDesc.structureVersionLoadSql).toBeUndefined();
    });
  });

  describe('partitionPreAggregations', () => {
    test('should construct correct partitionPreAggregations for dateRange in UTC (Day partitions)', async () => {
      const loader = createLoader({
        timezone: 'UTC',
      });

      const results = await loader.partitionPreAggregations();
      expect(results.length).toEqual(3);

      let [preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table20240101');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-01T00:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-01T23:59:59.999');
      expect((preAggDesc.loadSql as any)[0].includes('test_table20240101')).toBeTruthy();
      expect((preAggDesc.loadSql as any)[1][0]).toEqual('2024-01-01T00:00:00.000');
      expect((preAggDesc.loadSql as any)[1][1]).toEqual('2024-01-01T23:59:59.999');
      expect((preAggDesc.structureVersionLoadSql as any)[0].includes('test_table20240101')).toBeTruthy();
      expect((preAggDesc.structureVersionLoadSql as any)[1][0]).toEqual('2024-01-01T00:00:00.000');
      expect((preAggDesc.structureVersionLoadSql as any)[1][1]).toEqual('2024-01-01T23:59:59.999');

      [, preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table20240102');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-02T00:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-02T23:59:59.999');
      expect((preAggDesc.loadSql as any)[0].includes('test_table20240102')).toBeTruthy();
      expect((preAggDesc.loadSql as any)[1][0]).toEqual('2024-01-02T00:00:00.000');
      expect((preAggDesc.loadSql as any)[1][1]).toEqual('2024-01-02T23:59:59.999');
      expect((preAggDesc.structureVersionLoadSql as any)[0].includes('test_table20240102')).toBeTruthy();
      expect((preAggDesc.structureVersionLoadSql as any)[1][0]).toEqual('2024-01-02T00:00:00.000');
      expect((preAggDesc.structureVersionLoadSql as any)[1][1]).toEqual('2024-01-02T23:59:59.999');

      [,, preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table20240103');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-03T00:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-03T23:59:59.999');
      expect((preAggDesc.loadSql as any)[0].includes('test_table20240103')).toBeTruthy();
      expect((preAggDesc.loadSql as any)[1][0]).toEqual('2024-01-03T00:00:00.000');
      expect((preAggDesc.loadSql as any)[1][1]).toEqual('2024-01-03T23:59:59.999');
      expect((preAggDesc.structureVersionLoadSql as any)[0].includes('test_table20240103')).toBeTruthy();
      expect((preAggDesc.structureVersionLoadSql as any)[1][0]).toEqual('2024-01-03T00:00:00.000');
      expect((preAggDesc.structureVersionLoadSql as any)[1][1]).toEqual('2024-01-03T23:59:59.999');
    });

    test('should construct correct partitionPreAggregations for dateRange in America/New_York (Day partitions)', async () => {
      const loader = createLoader({
        timezone: 'America/New_York', // UTC-5
      });

      const results = await loader.partitionPreAggregations();
      expect(results.length).toEqual(4);

      let [preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table20231231');
      expect(preAggDesc.buildRangeStart).toEqual('2023-12-31T00:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2023-12-31T23:59:59.999');
      expect((preAggDesc.loadSql as any)[0].includes('test_table20231231')).toBeTruthy();
      expect((preAggDesc.loadSql as any)[1][0]).toEqual('2023-12-31T05:00:00.000');
      expect((preAggDesc.loadSql as any)[1][1]).toEqual('2024-01-01T04:59:59.999');
      expect((preAggDesc.structureVersionLoadSql as any)[0].includes('test_table20231231')).toBeTruthy();
      expect((preAggDesc.structureVersionLoadSql as any)[1][0]).toEqual('2023-12-31T05:00:00.000');
      expect((preAggDesc.structureVersionLoadSql as any)[1][1]).toEqual('2024-01-01T04:59:59.999');

      [, preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table20240101');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-01T00:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-01T23:59:59.999');
      expect((preAggDesc.loadSql as any)[0].includes('test_table20240101')).toBeTruthy();
      expect((preAggDesc.loadSql as any)[1][0]).toEqual('2024-01-01T05:00:00.000');
      expect((preAggDesc.loadSql as any)[1][1]).toEqual('2024-01-02T04:59:59.999');
      expect((preAggDesc.structureVersionLoadSql as any)[0].includes('test_table20240101')).toBeTruthy();
      expect((preAggDesc.structureVersionLoadSql as any)[1][0]).toEqual('2024-01-01T05:00:00.000');
      expect((preAggDesc.structureVersionLoadSql as any)[1][1]).toEqual('2024-01-02T04:59:59.999');

      [,,, preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table20240103');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-03T00:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-03T23:59:59.999');
      expect((preAggDesc.loadSql as any)[0].includes('test_table20240103')).toBeTruthy();
      expect((preAggDesc.loadSql as any)[1][0]).toEqual('2024-01-03T05:00:00.000');
      expect((preAggDesc.loadSql as any)[1][1]).toEqual('2024-01-04T04:59:59.999'); // Because DateRangeEnd Mock Query returns it
      expect((preAggDesc.structureVersionLoadSql as any)[0].includes('test_table20240103')).toBeTruthy();
      expect((preAggDesc.structureVersionLoadSql as any)[1][0]).toEqual('2024-01-03T05:00:00.000');
      expect((preAggDesc.structureVersionLoadSql as any)[1][1]).toEqual('2024-01-04T04:59:59.999');
    });

    test('should construct correct partitionPreAggregations for dateRange in Asia/Tokyo (Day partitions)', async () => {
      const loader = createLoader({
        timezone: 'Asia/Tokyo', // UTC+9
      });

      const results = await loader.partitionPreAggregations();
      expect(results.length).toEqual(4);

      let [preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table20240101');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-01T00:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-01T23:59:59.999');
      expect((preAggDesc.loadSql as any)[0].includes('test_table20240101')).toBeTruthy();
      expect((preAggDesc.loadSql as any)[1][0]).toEqual('2023-12-31T15:00:00.000');
      expect((preAggDesc.loadSql as any)[1][1]).toEqual('2024-01-01T14:59:59.999');
      expect((preAggDesc.structureVersionLoadSql as any)[0].includes('test_table20240101')).toBeTruthy();
      expect((preAggDesc.structureVersionLoadSql as any)[1][0]).toEqual('2023-12-31T15:00:00.000');
      expect((preAggDesc.structureVersionLoadSql as any)[1][1]).toEqual('2024-01-01T14:59:59.999');

      [, preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table20240102');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-02T00:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-02T23:59:59.999');
      expect((preAggDesc.loadSql as any)[0].includes('test_table20240102')).toBeTruthy();
      expect((preAggDesc.loadSql as any)[1][0]).toEqual('2024-01-01T15:00:00.000');
      expect((preAggDesc.loadSql as any)[1][1]).toEqual('2024-01-02T14:59:59.999');
      expect((preAggDesc.structureVersionLoadSql as any)[0].includes('test_table20240102')).toBeTruthy();
      expect((preAggDesc.structureVersionLoadSql as any)[1][0]).toEqual('2024-01-01T15:00:00.000');
      expect((preAggDesc.structureVersionLoadSql as any)[1][1]).toEqual('2024-01-02T14:59:59.999');

      [,,, preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table20240104');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-04T00:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-04T23:59:59.999');
      expect((preAggDesc.loadSql as any)[0].includes('test_table20240104')).toBeTruthy();
      expect((preAggDesc.loadSql as any)[1][0]).toEqual('2024-01-03T15:00:00.000');
      expect((preAggDesc.loadSql as any)[1][1]).toEqual('2024-01-04T14:59:59.999');
      expect((preAggDesc.structureVersionLoadSql as any)[0].includes('test_table20240104')).toBeTruthy();
      expect((preAggDesc.structureVersionLoadSql as any)[1][0]).toEqual('2024-01-03T15:00:00.000');
      expect((preAggDesc.structureVersionLoadSql as any)[1][1]).toEqual('2024-01-04T14:59:59.999');
    });

    test('should construct correct partitionPreAggregations for dateRange in UTC (Hour partitions)', async () => {
      const loader = createLoader({
        partitionGranularity: 'hour',
        timezone: 'UTC',
      });

      const results = await loader.partitionPreAggregations();
      expect(results.length).toEqual(72);

      let [preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table2024010100');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-01T00:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-01T00:59:59.999');
      expect((preAggDesc.loadSql as any)[0].includes('test_table2024010100')).toBeTruthy();
      expect((preAggDesc.loadSql as any)[1][0]).toEqual('2024-01-01T00:00:00.000');
      expect((preAggDesc.loadSql as any)[1][1]).toEqual('2024-01-01T00:59:59.999');
      expect((preAggDesc.structureVersionLoadSql as any)[0].includes('test_table2024010100')).toBeTruthy();
      expect((preAggDesc.structureVersionLoadSql as any)[1][0]).toEqual('2024-01-01T00:00:00.000');
      expect((preAggDesc.structureVersionLoadSql as any)[1][1]).toEqual('2024-01-01T00:59:59.999');

      [, preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table2024010101');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-01T01:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-01T01:59:59.999');
      expect((preAggDesc.loadSql as any)[0].includes('test_table2024010101')).toBeTruthy();
      expect((preAggDesc.loadSql as any)[1][0]).toEqual('2024-01-01T01:00:00.000');
      expect((preAggDesc.loadSql as any)[1][1]).toEqual('2024-01-01T01:59:59.999');
      expect((preAggDesc.structureVersionLoadSql as any)[0].includes('test_table2024010101')).toBeTruthy();
      expect((preAggDesc.structureVersionLoadSql as any)[1][0]).toEqual('2024-01-01T01:00:00.000');
      expect((preAggDesc.structureVersionLoadSql as any)[1][1]).toEqual('2024-01-01T01:59:59.999');

      // eslint-disable-next-line prefer-destructuring
      preAggDesc = results[71];
      expect(preAggDesc.tableName).toEqual('test_table2024010323');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-03T23:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-03T23:59:59.999');
      expect((preAggDesc.loadSql as any)[0].includes('test_table2024010323')).toBeTruthy();
      expect((preAggDesc.loadSql as any)[1][0]).toEqual('2024-01-03T23:00:00.000');
      expect((preAggDesc.loadSql as any)[1][1]).toEqual('2024-01-03T23:59:59.999');
      expect((preAggDesc.structureVersionLoadSql as any)[0].includes('test_table2024010323')).toBeTruthy();
      expect((preAggDesc.structureVersionLoadSql as any)[1][0]).toEqual('2024-01-03T23:00:00.000');
      expect((preAggDesc.structureVersionLoadSql as any)[1][1]).toEqual('2024-01-03T23:59:59.999');
    });

    test('should construct correct partitionPreAggregations for dateRange in America/New_York (Hour partitions)', async () => {
      const loader = createLoader({
        partitionGranularity: 'hour',
        timezone: 'America/New_York', // UTC-5
      });

      const results = await loader.partitionPreAggregations();
      expect(results.length).toEqual(72);

      let [preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table2023123119');
      expect(preAggDesc.buildRangeStart).toEqual('2023-12-31T19:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2023-12-31T19:59:59.999');
      expect((preAggDesc.loadSql as any)[0].includes('test_table2023123119')).toBeTruthy();
      expect((preAggDesc.loadSql as any)[1][0]).toEqual('2024-01-01T00:00:00.000');
      expect((preAggDesc.loadSql as any)[1][1]).toEqual('2024-01-01T00:59:59.999');
      expect((preAggDesc.structureVersionLoadSql as any)[0].includes('test_table2023123119')).toBeTruthy();
      expect((preAggDesc.structureVersionLoadSql as any)[1][0]).toEqual('2024-01-01T00:00:00.000');
      expect((preAggDesc.structureVersionLoadSql as any)[1][1]).toEqual('2024-01-01T00:59:59.999');

      [, preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table2023123120');
      expect(preAggDesc.buildRangeStart).toEqual('2023-12-31T20:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2023-12-31T20:59:59.999');
      expect((preAggDesc.loadSql as any)[0].includes('test_table2023123120')).toBeTruthy();
      expect((preAggDesc.loadSql as any)[1][0]).toEqual('2024-01-01T01:00:00.000');
      expect((preAggDesc.loadSql as any)[1][1]).toEqual('2024-01-01T01:59:59.999');
      expect((preAggDesc.structureVersionLoadSql as any)[0].includes('test_table2023123120')).toBeTruthy();
      expect((preAggDesc.structureVersionLoadSql as any)[1][0]).toEqual('2024-01-01T01:00:00.000');
      expect((preAggDesc.structureVersionLoadSql as any)[1][1]).toEqual('2024-01-01T01:59:59.999');

      // eslint-disable-next-line prefer-destructuring
      preAggDesc = results[71];
      expect(preAggDesc.tableName).toEqual('test_table2024010318');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-03T18:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-03T18:59:59.999');
      expect((preAggDesc.loadSql as any)[0].includes('test_table2024010318')).toBeTruthy();
      expect((preAggDesc.loadSql as any)[1][0]).toEqual('2024-01-03T23:00:00.000');
      expect((preAggDesc.loadSql as any)[1][1]).toEqual('2024-01-03T23:59:59.999');
      expect((preAggDesc.structureVersionLoadSql as any)[0].includes('test_table2024010318')).toBeTruthy();
      expect((preAggDesc.structureVersionLoadSql as any)[1][0]).toEqual('2024-01-03T23:00:00.000');
      expect((preAggDesc.structureVersionLoadSql as any)[1][1]).toEqual('2024-01-03T23:59:59.999');
    });

    test('should construct correct partitionPreAggregations for dateRange in Asia/Tokyo (Hour partitions)', async () => {
      const loader = createLoader({
        partitionGranularity: 'hour',
        timezone: 'Asia/Tokyo', // UTC+9
      });

      const results = await loader.partitionPreAggregations();
      expect(results.length).toEqual(72);

      let [preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table2024010109');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-01T09:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-01T09:59:59.999');
      expect((preAggDesc.loadSql as any)[0].includes('test_table2024010109')).toBeTruthy();
      expect((preAggDesc.loadSql as any)[1][0]).toEqual('2024-01-01T00:00:00.000');
      expect((preAggDesc.loadSql as any)[1][1]).toEqual('2024-01-01T00:59:59.999');
      expect((preAggDesc.structureVersionLoadSql as any)[0].includes('test_table2024010109')).toBeTruthy();
      expect((preAggDesc.structureVersionLoadSql as any)[1][0]).toEqual('2024-01-01T00:00:00.000');
      expect((preAggDesc.structureVersionLoadSql as any)[1][1]).toEqual('2024-01-01T00:59:59.999');

      [, preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table2024010110');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-01T10:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-01T10:59:59.999');
      expect((preAggDesc.loadSql as any)[0].includes('test_table2024010110')).toBeTruthy();
      expect((preAggDesc.loadSql as any)[1][0]).toEqual('2024-01-01T01:00:00.000');
      expect((preAggDesc.loadSql as any)[1][1]).toEqual('2024-01-01T01:59:59.999');
      expect((preAggDesc.structureVersionLoadSql as any)[0].includes('test_table2024010110')).toBeTruthy();
      expect((preAggDesc.structureVersionLoadSql as any)[1][0]).toEqual('2024-01-01T01:00:00.000');
      expect((preAggDesc.structureVersionLoadSql as any)[1][1]).toEqual('2024-01-01T01:59:59.999');

      // eslint-disable-next-line prefer-destructuring
      preAggDesc = results[71];
      expect(preAggDesc.tableName).toEqual('test_table2024010408');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-04T08:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-04T08:59:59.999');
      expect((preAggDesc.loadSql as any)[0].includes('test_table2024010408')).toBeTruthy();
      expect((preAggDesc.loadSql as any)[1][0]).toEqual('2024-01-03T23:00:00.000');
      expect((preAggDesc.loadSql as any)[1][1]).toEqual('2024-01-03T23:59:59.999');
      expect((preAggDesc.structureVersionLoadSql as any)[0].includes('test_table2024010408')).toBeTruthy();
      expect((preAggDesc.structureVersionLoadSql as any)[1][0]).toEqual('2024-01-03T23:00:00.000');
      expect((preAggDesc.structureVersionLoadSql as any)[1][1]).toEqual('2024-01-03T23:59:59.999');
    });
  });

  describe('partitionPreAggregations', () => {
    test('should generate partitioned pre-aggregations', async () => {
      const compilerCacheFn = jest.fn((_subKey: any, fn: () => any) => fn());
      const loader = createLoader(
        {
          partitionGranularity: 'day',
          matchedTimeDimensionDateRange: ['2023-01-01T00:00:00.000', '2023-01-02T23:59:59.999'],
        },
        { compilerCacheFn }
      );

      jest.spyOn(loader as any, 'partitionRanges').mockResolvedValue({
        buildRange: ['2023-01-01T00:00:00.000', '2023-01-02T23:59:59.999'],
        partitionRanges: [
          ['2023-01-01T00:00:00.000', '2023-01-01T23:59:59.999'],
          ['2023-01-02T00:00:00.000', '2023-01-02T23:59:59.999'],
        ],
      });

      const result = await loader.partitionPreAggregations();

      expect(result.length).toBe(2);
      expect(result[0].tableName).toMatch(/test_table20230101/);
      expect(result[1].tableName).toMatch(/test_table20230102/);
      expect(compilerCacheFn).toHaveBeenCalledWith(
        ['partitions', JSON.stringify(['2023-01-01T00:00:00.000', '2023-01-02T23:59:59.999'])],
        expect.any(Function)
      );
    });
  });

  describe('version function', () => {
    test('should return a valid version string for simple input', () => {
      const result = version(['test']);
      expect(result).toBeTruthy();
      expect(typeof result).toBe('string');
      expect(result.length).toBe(8);
    });

    test('should not hang on complex cache keys with nested objects and arrays', () => {
      // This test case previously caused an infinite loop due to signed bitwise operations
      const complexCacheKey = [
        [
          "CREATE TABLE prod_pre_aggregations_mxc.m_x_c_actionable_hourly_agg_main_with_index_month120260112 AS SELECT\n      `tags`.`description` `m_x_c_actionable_hourly_agg__description`, `tags`.`deviceName` `m_x_c_actionable_hourly_agg__device_name`, `tags`.`tagName` `m_x_c_actionable_hourly_agg__tag_name`, date_trunc('hour', from_utc_timestamp(`m_x_c_actionable_hourly_agg`.timestamp, 'America/Los_Angeles')) `m_x_c_actionable_hourly_agg__timestamp_hour`, sum(`m_x_c_actionable_hourly_agg`.`avgValue`) `m_x_c_actionable_hourly_agg__avg_value`, sum(`m_x_c_actionable_hourly_agg`.`firstValue`) `m_x_c_actionable_hourly_agg__first_value`, sum(`m_x_c_actionable_hourly_agg`.`lastValue`) `m_x_c_actionable_hourly_agg__last_value`, sum(`m_x_c_actionable_hourly_agg`.`maxValue`) `m_x_c_actionable_hourly_agg__max_value`, sum(`m_x_c_actionable_hourly_agg`.`minValue`) `m_x_c_actionable_hourly_agg__min_value`, sum(`m_x_c_actionable_hourly_agg`.`modeValue`) `m_x_c_actionable_hourly_agg__mode_value`\n    FROM\n      (SELECT *,\n          LAST(lastValue) OVER(PARTITION BY DEVICETAG ORDER BY timestamp ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lastValue2 \n        FROM prodcatalog.litmus.mxc_litmus_agg_zorder_action_hour\n        ) AS `m_x_c_actionable_hourly_agg`\nLEFT JOIN prodcatalog.litmus.mxc_litmus_agg_tagt AS `tags` ON `m_x_c_actionable_hourly_agg`.`DEVICETAG` = `tags`.`DEVICETAG`  WHERE (`m_x_c_actionable_hourly_agg`.timestamp >= from_utc_timestamp(replace(replace(?, 'T', ' '), 'Z', ''), 'UTC') AND `m_x_c_actionable_hourly_agg`.timestamp <= from_utc_timestamp(replace(replace(?, 'T', ' '), 'Z', ''), 'UTC')) GROUP BY 1, 2, 3, 4",
          [
            '2026-01-12T08:00:00.000Z',
            '2026-01-19T07:59:59.999Z'
          ],
          {}
        ],
        [
          {
            indexName: 'm_x_c_actionable_hourly_agg_main_with_index_month1_device_tag_description_index',
            sql: [
              "CREATE INDEX m_x_c_actionable_hourly_agg_main_with_index_month1_device_tag_description_index ON prod_pre_aggregations_mxc.m_x_c_actionable_hourly_agg_main_with_index_month120260112 (`m_x_c_actionable_hourly_agg__device_name`, `m_x_c_actionable_hourly_agg__tag_name`, `m_x_c_actionable_hourly_agg__description`, `m_x_c_actionable_hourly_agg__timestamp_hour`)",
              [],
              {}
            ]
          },
          {
            indexName: 'm_x_c_actionable_hourly_agg_main_with_index_month1_tag_description_device_index',
            sql: [
              "CREATE INDEX m_x_c_actionable_hourly_agg_main_with_index_month1_tag_description_device_index ON prod_pre_aggregations_mxc.m_x_c_actionable_hourly_agg_main_with_index_month120260112 (`m_x_c_actionable_hourly_agg__tag_name`, `m_x_c_actionable_hourly_agg__description`, `m_x_c_actionable_hourly_agg__device_name`)",
              [],
              {}
            ]
          }
        ],
        [
          [
            {
              refresh_key: null
            }
          ]
        ]
      ];

      // The function should complete without hanging (timeout will fail the test if it hangs)
      const result = version(complexCacheKey);
      expect(result).toBeTruthy();
      expect(typeof result).toBe('string');
      expect(result.length).toBe(8);
    });

    test('should handle inputs that produce high byte values in MD5 digest', () => {
      // Test various inputs to ensure unsigned bit operations work correctly
      const testCases = [
        'test',
        { key: 'value' },
        [1, 2, 3],
        'a'.repeat(1000),
        { nested: { deep: { value: 'test' } } },
      ];

      for (const input of testCases) {
        const result = version(input);
        expect(result).toBeTruthy();
        expect(typeof result).toBe('string');
        expect(result.length).toBe(8);
        // Verify the result only contains valid charset characters
        expect(result).toMatch(/^[a-z0-5]+$/);
      }
    });

    test('should produce same results as old implementation for backward compatibility', () => {
      // Old implementation (before the unsigned shift fix)
      // This would hang on certain inputs, but for inputs that don't trigger the bug,
      // it should produce the same results as the new implementation
      const crypto = require('crypto');

      function oldVersion(cacheKey: any): string | null {
        let result = '';

        const hashCharset = 'abcdefghijklmnopqrstuvwxyz012345';
        const digestBuffer = crypto.createHash('md5').update(JSON.stringify(cacheKey)).digest();

        let residue = 0;
        let shiftCounter = 0;

        for (let i = 0; i < 5; i++) {
          const byte = digestBuffer.readUInt8(i);
          shiftCounter += 8;
          // eslint-disable-next-line operator-assignment,no-bitwise
          residue = (byte << (shiftCounter - 8)) | residue;
          // eslint-disable-next-line no-bitwise
          while (residue >> 5) {
            result += hashCharset.charAt(residue % 32);
            shiftCounter -= 5;
            // eslint-disable-next-line operator-assignment,no-bitwise
            residue = residue >> 5;
          }
        }

        result += hashCharset.charAt(residue % 32);

        return result;
      }

      // 20 hard-coded test cases with their expected version strings
      // These are keys that work correctly with both old and new implementations
      const testCases: Array<{ key: any; expected: string }> = [
        { key: 'simple_string', expected: 'lyidb3bl' },
        { key: 'hello_world', expected: 'sz1y5yvi' },
        { key: 'test_key_123', expected: 'tpsualal' },
        { key: ['array', 'of', 'strings'], expected: 'sbll5p55' },
        { key: { name: 'object', value: 42 }, expected: 'sq5wacbz' },
        { key: [1, 2, 3, 4, 5], expected: 'sercayat' },
        { key: { nested: { level: 2 } }, expected: '5hdmsxe4' },
        { key: 'SELECT * FROM users', expected: 'bzasp2ee' },
        { key: ['CREATE TABLE test', ['param1', 'param2']], expected: 'ghze1maw' },
        { key: { sql: 'SELECT 1', params: [] }, expected: 'crhopprj' },
        { key: 'pre_aggregation_key_v1', expected: 'ldkocgfh' },
        { key: ['2024-01-01', '2024-12-31'], expected: 'oojrcwo3' },
        { key: { timezone: 'UTC', granularity: 'day' }, expected: 'es2subt' },
        { key: 'cube_query_cache_key', expected: 'zxeekgd0' },
        { key: [{ id: 1 }, { id: 2 }, { id: 3 }], expected: 'kxoosnjv' },
        { key: { dimensions: ['a', 'b'], measures: ['c'] }, expected: '1ppe4o4c' },
        { key: 'abcdefghijklmnopqrstuvwxyz', expected: 'aj4ij4kb' },
        { key: '0123456789', expected: 'wsidmvgj' },
        { key: { empty: {}, arr: [] }, expected: 'jvhxdtaj' },
        { key: ['mixed', 123, true, null, { x: 'y' }], expected: 'qzfgu32u' },
      ];

      for (const { key, expected } of testCases) {
        // Verify new implementation matches expected
        const newResult = version(key);
        expect(newResult).toBe(expected);

        // Verify old implementation also matches (proving backward compatibility)
        const oldResult = oldVersion(key);
        expect(oldResult).not.toBeNull(); // Should not hang
        expect(oldResult).toBe(expected);
      }
    });
  });
});
