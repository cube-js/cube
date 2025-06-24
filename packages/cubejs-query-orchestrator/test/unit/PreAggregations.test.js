/* eslint-disable global-require */
/* globals describe, jest, beforeEach, test, expect */
import R from 'ramda';
import {
  BUILD_RANGE_END_LOCAL,
  BUILD_RANGE_START_LOCAL,
  FROM_PARTITION_RANGE,
  TO_PARTITION_RANGE
} from '@cubejs-backend/shared';
import { PreAggregationPartitionRangeLoader, PreAggregations } from '../../src';

class MockDriver {
  constructor() {
    this.tables = [];
    this.executedQueries = [];
    this.cancelledQueries = [];
    this.now = new Date().getTime();
  }

  query(query) {
    this.executedQueries.push(query);
    let promise = Promise.resolve([query]);
    if (query.match('orders_too_big')) {
      promise = promise.then((res) => new Promise(resolve => setTimeout(() => resolve(res), 3000)));
    }
    promise.cancel = async () => {
      this.cancelledQueries.push(query);
    };
    return promise;
  }

  async getTablesQuery(schema) {
    return this.tables.map(t => ({ table_name: t.replace(`${schema}.`, '') }));
  }

  async createSchemaIfNotExists(schema) {
    this.schema = schema;
    return null;
  }

  loadPreAggregationIntoTable(preAggregationTableName, loadSql) {
    this.tables.push(preAggregationTableName.substring(0, 100));
    return this.query(loadSql);
  }

  async dropTable(tableName) {
    this.tables = this.tables.filter(t => t !== tableName);
    return this.query(`DROP TABLE ${tableName}`);
  }

  async downloadTable(table) {
    return { rows: await this.query(`SELECT * FROM ${table}`) };
  }

  async tableColumnTypes(_table) {
    return [];
  }

  async uploadTable(table, columns, _tableData) {
    await this.createTable(table, columns);
  }

  createTable(quotedTableName, _columns) {
    this.tables.push(quotedTableName);
  }

  readOnly() {
    return false;
  }

  nowTimestamp() {
    return this.now;
  }
}

const mockPreAggregation = (overrides = {}) => ({
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

const createLoader = (overrides = {}, options = {}) => {
  const loader = new PreAggregationPartitionRangeLoader(
    {}, // driverFactory
    {}, // logger
    { options: {} }, // queryCache
    {}, // preAggregations
    mockPreAggregation(overrides),
    [], // preAggregationsTablesToTempTables
    {}, // loadCache
    options,
  );

  jest.spyOn(loader, 'loadRangeQuery').mockImplementation(async (query, _partitionRange) => {
    if (query[0].includes('MIN')) {
      return [{ value: '2024-01-01T00:00:00.000' }];
    }
    return [{ value: '2024-01-03T23:59:59.999' }];
  });

  return loader;
};

describe('PreAggregations', () => {
  let mockDriver = null;
  let mockExternalDriver = null;
  let mockDriverFactory = null;
  let mockDriverReadOnlyFactory = null;
  let mockExternalDriverFactory = null;
  let queryCache = null;

  const basicQuery = {
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
    mockDriverFactory = async () => mockDriver;
    mockDriverReadOnlyFactory = async () => {
      const driver = mockDriver;
      jest.spyOn(driver, 'readOnly').mockImplementation(() => true);
      return driver;
    };
    mockExternalDriverFactory = async () => {
      const driver = mockExternalDriver;
      driver.createTable('stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il_1593709044209');
      return driver;
    };

    jest.resetModules();
    const { QueryCache } = require('../../src/orchestrator/QueryCache');
    queryCache = new QueryCache(
      'TEST',
      mockDriverFactory,
      // eslint-disable-next-line @typescript-eslint/no-empty-function
      () => {},
      {
        queueOptions: () => ({
          executionTimeout: 1,
          concurrency: 2,
        }),
      },
    );
  });

  describe('loadAllPreAggregationsIfNeeded', () => {
    let preAggregations = null;

    beforeEach(async () => {
      preAggregations = new PreAggregations(
        'TEST',
        mockDriverFactory,
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        () => {},
        queryCache,
        {
          queueOptions: () => ({
            executionTimeout: 1,
            concurrency: 2,
          }),
        },
      );
    });

    test('synchronously create rollup from scratch', async () => {
      mockDriver.now = 12345000;
      const { preAggregationsTablesToTempTables: result } = await preAggregations.loadAllPreAggregationsIfNeeded(basicQueryWithRenew);
      expect(result[0][1].targetTableName).toMatch(/stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il/);
      expect(result[0][1].lastUpdatedAt).toEqual(12345000);
    });
  });

  describe('loadAllPreAggregationsIfNeeded with external rollup and writable source', () => {
    let preAggregations = null;

    beforeEach(async () => {
      preAggregations = new PreAggregations(
        'TEST',
        mockDriverFactory,
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        () => {},
        queryCache,
        {
          queueOptions: () => ({
            executionTimeout: 1,
            concurrency: 2,
          }),
          externalDriverFactory: mockExternalDriverFactory,
        },
      );
    });

    test('refresh external preaggregation with a writable source (refreshImplTempTableExternalStrategy)', async () => {
      const { preAggregationsTablesToTempTables: result } = await preAggregations.loadAllPreAggregationsIfNeeded(basicQueryExternal);
      expect(result[0][1].targetTableName).toMatch(/stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il/);
      expect(result[0][1].lastUpdatedAt).toEqual(1593709044209);
    });
  });

  describe('loadAllPreAggregationsIfNeeded with external rollup and readonly source', () => {
    let preAggregations = null;

    beforeEach(async () => {
      preAggregations = new PreAggregations(
        'TEST',
        mockDriverReadOnlyFactory,
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        () => {},
        queryCache,
        {
          queueOptions: () => ({
            executionTimeout: 1,
            concurrency: 2,
          }),
          externalDriverFactory: mockExternalDriverFactory,
        },
      );
    });

    test('refresh external preaggregation with a writable source (refreshImplStreamExternalStrategy)', async () => {
      const { preAggregationsTablesToTempTables: result } = await preAggregations.loadAllPreAggregationsIfNeeded(basicQueryExternal);
      expect(result[0][1].targetTableName).toMatch(/stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il/);
      expect(result[0][1].lastUpdatedAt).toEqual(1593709044209);
    });
  });

  describe('loadAllPreAggregationsIfNeeded with externalRefresh true', () => {
    let preAggregations = null;

    beforeEach(async () => {
      preAggregations = new PreAggregations(
        'TEST',
        mockDriverFactory,
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        () => {},
        queryCache,
        {
          queueOptions: () => ({
            executionTimeout: 1,
            concurrency: 2,
          }),
          externalRefresh: true,
        },
      );
    });

    test('fail if waitForRenew is also specified', async () => {
      await expect(preAggregations.loadAllPreAggregationsIfNeeded(basicQueryWithRenew))
        .rejects.toThrowError(/Invalid configuration/);
    });

    test('fail if rollup doesn\'t already exist', async () => {
      await expect(preAggregations.loadAllPreAggregationsIfNeeded(basicQuery))
        .rejects.toThrowError(/No pre-aggregation partitions were built yet/);
    });
  });

  describe('loadAllPreAggregationsIfNeeded with external rollup and externalRefresh true', () => {
    let preAggregations = null;

    beforeEach(async () => {
      preAggregations = new PreAggregations(
        'TEST',
        () => { throw new Error('The source database factory should never be called when externalRefresh is true, as it will trigger testConnection'); },
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        () => {},
        queryCache,
        {
          queueOptions: () => ({
            executionTimeout: 1,
            concurrency: 2,
          }),
          externalDriverFactory: mockExternalDriverFactory,
          externalRefresh: true,
        },
      );
    });

    test('fail if waitForRenew is also specified', async () => {
      await expect(preAggregations.loadAllPreAggregationsIfNeeded(basicQueryExternalWithRenew))
        .rejects.toThrowError(/Invalid configuration/);
    });

    test('load external preaggregation without communicating to the source database', async () => {
      const { preAggregationsTablesToTempTables: result } = await preAggregations.loadAllPreAggregationsIfNeeded(basicQueryExternal);
      expect(result[0][1].targetTableName).toMatch(/stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il/);
      expect(result[0][1].lastUpdatedAt).toEqual(1593709044209);
    });
  });

  describe('naming_version tests', () => {
    let preAggregations = null;

    beforeEach(async () => {
      preAggregations = new PreAggregations(
        'TEST',
        mockDriverFactory,
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        () => {},
        queryCache,
        {
          queueOptions: () => ({
            executionTimeout: 1,
            concurrency: 2,
          }),
          externalDriverFactory: async () => {
            const driver = mockExternalDriver;
            driver.createTable('stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il_1593709044209');
            driver.createTable('stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il_1fm6652');
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
      const { preAggregationsTablesToTempTables: result } = await preAggregations.loadAllPreAggregationsIfNeeded(basicQueryExternal);
      expect(result[0][1].targetTableName).toMatch(/stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il_1fm6652/);
      expect(result[0][1].lastUpdatedAt).toEqual(1600329890000);
    });
  });

  describe('naming_version sort tests', () => {
    let preAggregations = null;

    beforeEach(async () => {
      preAggregations = new PreAggregations(
        'TEST',
        mockDriverFactory,
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        () => {},
        queryCache,
        {
          queueOptions: () => ({
            executionTimeout: 1,
            concurrency: 2,
          }),
          externalDriverFactory: async () => {
            const driver = mockExternalDriver;
            driver.createTable('stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il_1893709044209');
            driver.createTable('stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il_1fm6652');
            return driver;
          },
        },
      );
    });

    test('naming_version and sort by last_updated_at', async () => {
      const { preAggregationsTablesToTempTables: result } = await preAggregations.loadAllPreAggregationsIfNeeded(basicQueryExternal);
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
        ['2024-01-01T00:00:00.000'],
        ['2024-01-01T00:00:00.000', '2024-01-31T23:59:59.999']
      )).toThrow('Date range expected to be an array with 2 elements');

      expect(() => PreAggregationPartitionRangeLoader.intersectDateRanges(
        ['2024-01-01T00:00:00.000', '2024-01-31T23:59:59.999', '2024-01-01T00:00:00.000'],
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
      const testDateRange = ['2024-01-05T12:34:56.789', '2024-01-05T23:59:59.999'];

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
      jest.spyOn(loader, 'loadBuildRange').mockResolvedValue([
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
      expect(preAggDesc.loadSql[0].includes('test_table')).toBeTruthy();
      expect(preAggDesc.loadSql[1][0]).toEqual(FROM_PARTITION_RANGE);
      expect(preAggDesc.loadSql[1][1]).toEqual(TO_PARTITION_RANGE);
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
      expect(preAggDesc.loadSql[0].includes('test_table20240101')).toBeTruthy();
      expect(preAggDesc.loadSql[1][0]).toEqual('2024-01-01T00:00:00.000');
      expect(preAggDesc.loadSql[1][1]).toEqual('2024-01-01T23:59:59.999');
      expect(preAggDesc.structureVersionLoadSql[0].includes('test_table20240101')).toBeTruthy();
      expect(preAggDesc.structureVersionLoadSql[1][0]).toEqual('2024-01-01T00:00:00.000');
      expect(preAggDesc.structureVersionLoadSql[1][1]).toEqual('2024-01-01T23:59:59.999');

      [, preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table20240102');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-02T00:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-02T23:59:59.999');
      expect(preAggDesc.loadSql[0].includes('test_table20240102')).toBeTruthy();
      expect(preAggDesc.loadSql[1][0]).toEqual('2024-01-02T00:00:00.000');
      expect(preAggDesc.loadSql[1][1]).toEqual('2024-01-02T23:59:59.999');
      expect(preAggDesc.structureVersionLoadSql[0].includes('test_table20240102')).toBeTruthy();
      expect(preAggDesc.structureVersionLoadSql[1][0]).toEqual('2024-01-02T00:00:00.000');
      expect(preAggDesc.structureVersionLoadSql[1][1]).toEqual('2024-01-02T23:59:59.999');

      [,, preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table20240103');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-03T00:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-03T23:59:59.999');
      expect(preAggDesc.loadSql[0].includes('test_table20240103')).toBeTruthy();
      expect(preAggDesc.loadSql[1][0]).toEqual('2024-01-03T00:00:00.000');
      expect(preAggDesc.loadSql[1][1]).toEqual('2024-01-03T23:59:59.999');
      expect(preAggDesc.structureVersionLoadSql[0].includes('test_table20240103')).toBeTruthy();
      expect(preAggDesc.structureVersionLoadSql[1][0]).toEqual('2024-01-03T00:00:00.000');
      expect(preAggDesc.structureVersionLoadSql[1][1]).toEqual('2024-01-03T23:59:59.999');
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
      expect(preAggDesc.loadSql[0].includes('test_table20231231')).toBeTruthy();
      expect(preAggDesc.loadSql[1][0]).toEqual('2023-12-31T05:00:00.000');
      expect(preAggDesc.loadSql[1][1]).toEqual('2024-01-01T04:59:59.999');
      expect(preAggDesc.structureVersionLoadSql[0].includes('test_table20231231')).toBeTruthy();
      expect(preAggDesc.structureVersionLoadSql[1][0]).toEqual('2023-12-31T05:00:00.000');
      expect(preAggDesc.structureVersionLoadSql[1][1]).toEqual('2024-01-01T04:59:59.999');

      [, preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table20240101');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-01T00:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-01T23:59:59.999');
      expect(preAggDesc.loadSql[0].includes('test_table20240101')).toBeTruthy();
      expect(preAggDesc.loadSql[1][0]).toEqual('2024-01-01T05:00:00.000');
      expect(preAggDesc.loadSql[1][1]).toEqual('2024-01-02T04:59:59.999');
      expect(preAggDesc.structureVersionLoadSql[0].includes('test_table20240101')).toBeTruthy();
      expect(preAggDesc.structureVersionLoadSql[1][0]).toEqual('2024-01-01T05:00:00.000');
      expect(preAggDesc.structureVersionLoadSql[1][1]).toEqual('2024-01-02T04:59:59.999');

      [,,, preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table20240103');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-03T00:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-03T23:59:59.999');
      expect(preAggDesc.loadSql[0].includes('test_table20240103')).toBeTruthy();
      expect(preAggDesc.loadSql[1][0]).toEqual('2024-01-03T05:00:00.000');
      expect(preAggDesc.loadSql[1][1]).toEqual('2024-01-04T04:59:59.999'); // Because DateRangeEnd Mock Query returns it
      expect(preAggDesc.structureVersionLoadSql[0].includes('test_table20240103')).toBeTruthy();
      expect(preAggDesc.structureVersionLoadSql[1][0]).toEqual('2024-01-03T05:00:00.000');
      expect(preAggDesc.structureVersionLoadSql[1][1]).toEqual('2024-01-04T04:59:59.999');
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
      expect(preAggDesc.loadSql[0].includes('test_table20240101')).toBeTruthy();
      expect(preAggDesc.loadSql[1][0]).toEqual('2023-12-31T15:00:00.000');
      expect(preAggDesc.loadSql[1][1]).toEqual('2024-01-01T14:59:59.999');
      expect(preAggDesc.structureVersionLoadSql[0].includes('test_table20240101')).toBeTruthy();
      expect(preAggDesc.structureVersionLoadSql[1][0]).toEqual('2023-12-31T15:00:00.000');
      expect(preAggDesc.structureVersionLoadSql[1][1]).toEqual('2024-01-01T14:59:59.999');

      [, preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table20240102');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-02T00:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-02T23:59:59.999');
      expect(preAggDesc.loadSql[0].includes('test_table20240102')).toBeTruthy();
      expect(preAggDesc.loadSql[1][0]).toEqual('2024-01-01T15:00:00.000');
      expect(preAggDesc.loadSql[1][1]).toEqual('2024-01-02T14:59:59.999');
      expect(preAggDesc.structureVersionLoadSql[0].includes('test_table20240102')).toBeTruthy();
      expect(preAggDesc.structureVersionLoadSql[1][0]).toEqual('2024-01-01T15:00:00.000');
      expect(preAggDesc.structureVersionLoadSql[1][1]).toEqual('2024-01-02T14:59:59.999');

      [,,, preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table20240104');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-04T00:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-04T23:59:59.999');
      expect(preAggDesc.loadSql[0].includes('test_table20240104')).toBeTruthy();
      expect(preAggDesc.loadSql[1][0]).toEqual('2024-01-03T15:00:00.000');
      expect(preAggDesc.loadSql[1][1]).toEqual('2024-01-04T14:59:59.999');
      expect(preAggDesc.structureVersionLoadSql[0].includes('test_table20240104')).toBeTruthy();
      expect(preAggDesc.structureVersionLoadSql[1][0]).toEqual('2024-01-03T15:00:00.000');
      expect(preAggDesc.structureVersionLoadSql[1][1]).toEqual('2024-01-04T14:59:59.999');
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
      expect(preAggDesc.loadSql[0].includes('test_table2024010100')).toBeTruthy();
      expect(preAggDesc.loadSql[1][0]).toEqual('2024-01-01T00:00:00.000');
      expect(preAggDesc.loadSql[1][1]).toEqual('2024-01-01T00:59:59.999');
      expect(preAggDesc.structureVersionLoadSql[0].includes('test_table2024010100')).toBeTruthy();
      expect(preAggDesc.structureVersionLoadSql[1][0]).toEqual('2024-01-01T00:00:00.000');
      expect(preAggDesc.structureVersionLoadSql[1][1]).toEqual('2024-01-01T00:59:59.999');

      [, preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table2024010101');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-01T01:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-01T01:59:59.999');
      expect(preAggDesc.loadSql[0].includes('test_table2024010101')).toBeTruthy();
      expect(preAggDesc.loadSql[1][0]).toEqual('2024-01-01T01:00:00.000');
      expect(preAggDesc.loadSql[1][1]).toEqual('2024-01-01T01:59:59.999');
      expect(preAggDesc.structureVersionLoadSql[0].includes('test_table2024010101')).toBeTruthy();
      expect(preAggDesc.structureVersionLoadSql[1][0]).toEqual('2024-01-01T01:00:00.000');
      expect(preAggDesc.structureVersionLoadSql[1][1]).toEqual('2024-01-01T01:59:59.999');

      // eslint-disable-next-line prefer-destructuring
      preAggDesc = results[71];
      expect(preAggDesc.tableName).toEqual('test_table2024010323');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-03T23:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-03T23:59:59.999');
      expect(preAggDesc.loadSql[0].includes('test_table2024010323')).toBeTruthy();
      expect(preAggDesc.loadSql[1][0]).toEqual('2024-01-03T23:00:00.000');
      expect(preAggDesc.loadSql[1][1]).toEqual('2024-01-03T23:59:59.999');
      expect(preAggDesc.structureVersionLoadSql[0].includes('test_table2024010323')).toBeTruthy();
      expect(preAggDesc.structureVersionLoadSql[1][0]).toEqual('2024-01-03T23:00:00.000');
      expect(preAggDesc.structureVersionLoadSql[1][1]).toEqual('2024-01-03T23:59:59.999');
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
      expect(preAggDesc.loadSql[0].includes('test_table2023123119')).toBeTruthy();
      expect(preAggDesc.loadSql[1][0]).toEqual('2024-01-01T00:00:00.000');
      expect(preAggDesc.loadSql[1][1]).toEqual('2024-01-01T00:59:59.999');
      expect(preAggDesc.structureVersionLoadSql[0].includes('test_table2023123119')).toBeTruthy();
      expect(preAggDesc.structureVersionLoadSql[1][0]).toEqual('2024-01-01T00:00:00.000');
      expect(preAggDesc.structureVersionLoadSql[1][1]).toEqual('2024-01-01T00:59:59.999');

      [, preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table2023123120');
      expect(preAggDesc.buildRangeStart).toEqual('2023-12-31T20:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2023-12-31T20:59:59.999');
      expect(preAggDesc.loadSql[0].includes('test_table2023123120')).toBeTruthy();
      expect(preAggDesc.loadSql[1][0]).toEqual('2024-01-01T01:00:00.000');
      expect(preAggDesc.loadSql[1][1]).toEqual('2024-01-01T01:59:59.999');
      expect(preAggDesc.structureVersionLoadSql[0].includes('test_table2023123120')).toBeTruthy();
      expect(preAggDesc.structureVersionLoadSql[1][0]).toEqual('2024-01-01T01:00:00.000');
      expect(preAggDesc.structureVersionLoadSql[1][1]).toEqual('2024-01-01T01:59:59.999');

      // eslint-disable-next-line prefer-destructuring
      preAggDesc = results[71];
      expect(preAggDesc.tableName).toEqual('test_table2024010318');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-03T18:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-03T18:59:59.999');
      expect(preAggDesc.loadSql[0].includes('test_table2024010318')).toBeTruthy();
      expect(preAggDesc.loadSql[1][0]).toEqual('2024-01-03T23:00:00.000');
      expect(preAggDesc.loadSql[1][1]).toEqual('2024-01-03T23:59:59.999');
      expect(preAggDesc.structureVersionLoadSql[0].includes('test_table2024010318')).toBeTruthy();
      expect(preAggDesc.structureVersionLoadSql[1][0]).toEqual('2024-01-03T23:00:00.000');
      expect(preAggDesc.structureVersionLoadSql[1][1]).toEqual('2024-01-03T23:59:59.999');
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
      expect(preAggDesc.loadSql[0].includes('test_table2024010109')).toBeTruthy();
      expect(preAggDesc.loadSql[1][0]).toEqual('2024-01-01T00:00:00.000');
      expect(preAggDesc.loadSql[1][1]).toEqual('2024-01-01T00:59:59.999');
      expect(preAggDesc.structureVersionLoadSql[0].includes('test_table2024010109')).toBeTruthy();
      expect(preAggDesc.structureVersionLoadSql[1][0]).toEqual('2024-01-01T00:00:00.000');
      expect(preAggDesc.structureVersionLoadSql[1][1]).toEqual('2024-01-01T00:59:59.999');

      [, preAggDesc] = results;
      expect(preAggDesc.tableName).toEqual('test_table2024010110');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-01T10:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-01T10:59:59.999');
      expect(preAggDesc.loadSql[0].includes('test_table2024010110')).toBeTruthy();
      expect(preAggDesc.loadSql[1][0]).toEqual('2024-01-01T01:00:00.000');
      expect(preAggDesc.loadSql[1][1]).toEqual('2024-01-01T01:59:59.999');
      expect(preAggDesc.structureVersionLoadSql[0].includes('test_table2024010110')).toBeTruthy();
      expect(preAggDesc.structureVersionLoadSql[1][0]).toEqual('2024-01-01T01:00:00.000');
      expect(preAggDesc.structureVersionLoadSql[1][1]).toEqual('2024-01-01T01:59:59.999');

      // eslint-disable-next-line prefer-destructuring
      preAggDesc = results[71];
      expect(preAggDesc.tableName).toEqual('test_table2024010408');
      expect(preAggDesc.buildRangeStart).toEqual('2024-01-04T08:00:00.000');
      expect(preAggDesc.buildRangeEnd).toEqual('2024-01-04T08:59:59.999');
      expect(preAggDesc.loadSql[0].includes('test_table2024010408')).toBeTruthy();
      expect(preAggDesc.loadSql[1][0]).toEqual('2024-01-03T23:00:00.000');
      expect(preAggDesc.loadSql[1][1]).toEqual('2024-01-03T23:59:59.999');
      expect(preAggDesc.structureVersionLoadSql[0].includes('test_table2024010408')).toBeTruthy();
      expect(preAggDesc.structureVersionLoadSql[1][0]).toEqual('2024-01-03T23:00:00.000');
      expect(preAggDesc.structureVersionLoadSql[1][1]).toEqual('2024-01-03T23:59:59.999');
    });
  });

  describe('partitionPreAggregations', () => {
    test('should generate partitioned pre-aggregations', async () => {
      const compilerCacheFn = jest.fn((subKey, fn) => fn());
      const loader = createLoader(
        {
          partitionGranularity: 'day',
          matchedTimeDimensionDateRange: ['2023-01-01T00:00:00.000', '2023-01-02T23:59:59.999'],
        },
        { compilerCacheFn }
      );

      jest.spyOn(loader, 'partitionRanges').mockResolvedValue({
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
});
