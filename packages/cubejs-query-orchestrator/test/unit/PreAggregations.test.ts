/* eslint-disable @typescript-eslint/no-explicit-any */
import {
  BUILD_RANGE_END_LOCAL,
  BUILD_RANGE_START_LOCAL,
  FROM_PARTITION_RANGE,
  TO_PARTITION_RANGE,
  timeSeries,
  QueryDateRange,
} from '@cubejs-backend/shared';
import crypto from 'crypto';

import { PreAggregationLoadCache, PreAggregationLoader, PreAggregationPartitionRangeLoader, PreAggregations, QueryCache, QueryCacheOptions, LocalCacheDriver, version, type QueryWithParams } from '../../src';
import { evaluateLocalRefreshKey } from '../../src/orchestrator/utils';

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

// Widens the protected entry points that the tests drive directly.
class TestPartitionRangeLoader extends PreAggregationPartitionRangeLoader {
  public getInvalidationKeyValues(range: [string, string]) {
    return super.getInvalidationKeyValues(range);
  }

  public partitionPreAggregationDescription(range: QueryDateRange, buildRange: QueryDateRange) {
    return super.partitionPreAggregationDescription(range, buildRange);
  }

  public partitionRanges(ignoreMatchedDateRange?: boolean) {
    return super.partitionRanges(ignoreMatchedDateRange);
  }
}

const createLoader = (overrides: Record<string, any> = {}, options: Record<string, any> = {}, loadCache: Record<string, any> = {}) => {
  const loader = new TestPartitionRangeLoader(
    {} as any, // driverFactory
    {} as any, // logger
    { options: {} } as any, // queryCache
    {} as any, // preAggregations
    mockPreAggregation(overrides) as any,
    [], // preAggregationsTablesToTempTables
    loadCache as any,
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

describe('loadBuildRange', () => {
  const utcDates = {
    longStart: '2021-01-01T12:00:00.000',
    longEnd: '2024-01-05T12:00:00.000',
    springStart: '2024-03-10T06:30:00.000',
    springEnd: '2024-03-10T07:30:00.000',
    fallBeforeTransition: '2024-11-03T04:30:00.000',
    fallStart: '2024-11-03T05:30:00.000',
    fallEnd: '2024-11-03T06:30:00.000',
    renewedStart: '2024-03-11T06:30:00.000',
    renewedEnd: '2024-11-04T06:30:00.000',
    reversedStart: '2024-03-12T06:30:00.000',
    unpartitionedStart: '2024-01-01T00:00:00.000',
    unpartitionedEnd: '2024-01-03T23:59:59.999',
    now: '2024-07-01T12:34:56.789',
  };
  type DateName = keyof typeof utcDates;
  type DatePair = [DateName, DateName];
  type QueryResultPair = [DateName | null, DateName | null];

  afterEach(() => {
    jest.restoreAllMocks();
    jest.useRealTimers();
  });

  describe.each(['UTC', 'America/New_York'])('%s', (timezone) => {
    const localDates: Record<DateName, string> = timezone === 'UTC' ? utcDates : {
      longStart: '2021-01-01T07:00:00.000',
      longEnd: '2024-01-05T07:00:00.000',
      springStart: '2024-03-10T01:30:00.000',
      springEnd: '2024-03-10T03:30:00.000',
      fallBeforeTransition: '2024-11-03T00:30:00.000',
      // Distinct UTC instants intentionally share the repeated local hour (EDT/EST).
      fallStart: '2024-11-03T01:30:00.000',
      fallEnd: '2024-11-03T01:30:00.000',
      renewedStart: '2024-03-11T02:30:00.000',
      renewedEnd: '2024-11-04T01:30:00.000',
      reversedStart: '2024-03-12T02:30:00.000',
      unpartitionedStart: '2023-12-31T19:00:00.000',
      unpartitionedEnd: '2024-01-03T18:59:59.999',
      now: '2024-07-01T08:34:56.789',
    };

    describe.each([3, 6])('precision %i', (timestampPrecision) => {
      const scenarios: { name: string; initial: QueryResultPair; renewed?: QueryResultPair; buildRange: DatePair; result: DatePair }[] = [
        { name: 'long range', initial: ['longStart', 'longEnd'], buildRange: ['longStart', 'longEnd'], result: ['longStart', 'longEnd'] },
        { name: 'spring DST', initial: ['springStart', 'springEnd'], buildRange: ['springStart', 'springEnd'], result: ['springStart', 'springEnd'] },
        { name: 'fall DST', initial: ['fallBeforeTransition', 'fallEnd'], buildRange: ['fallBeforeTransition', 'fallEnd'], result: ['fallBeforeTransition', 'fallEnd'] },
        { name: 'fall DST repeated local hour', initial: ['fallStart', 'fallEnd'], buildRange: ['fallStart', 'fallEnd'], result: ['fallStart', 'fallEnd'] },
        { name: 'renewed dates', initial: ['springStart', 'fallEnd'], renewed: ['renewedStart', 'renewedEnd'], buildRange: ['springStart', 'fallEnd'], result: ['renewedStart', 'renewedEnd'] },
        { name: 'empty', initial: [null, null], buildRange: ['now', 'now'], result: ['now', 'now'] },
        { name: 'empty start', initial: [null, 'springEnd'], buildRange: ['springEnd', 'springEnd'], result: ['springEnd', 'springEnd'] },
        { name: 'empty end', initial: ['springStart', null], buildRange: ['springStart', 'springStart'], result: ['springStart', 'springStart'] },
        { name: 'empty renewal', initial: ['springStart', 'fallEnd'], renewed: [null, null], buildRange: ['springStart', 'fallEnd'], result: ['now', 'now'] },
        { name: 'empty renewed start', initial: ['springStart', 'fallEnd'], renewed: [null, 'renewedEnd'], buildRange: ['springStart', 'fallEnd'], result: ['renewedEnd', 'renewedEnd'] },
        { name: 'reversed', initial: ['reversedStart', 'springStart'], buildRange: ['reversedStart', 'springStart'], result: ['reversedStart', 'springStart'] },
      ];
      // Empty-result fallback uses now()'s millisecond format, even at precision 6.
      const expectedRange = (names: DatePair): QueryDateRange => names.map(
        name => localDates[name] + (name === 'now' ? '' : '0'.repeat(timestampPrecision - 3))
      ) as QueryDateRange;

      it.each(scenarios)('preserves both query stages, renewal keys and dates: $name', async ({ initial, renewed = initial, buildRange, result }) => {
        jest.useFakeTimers({ now: new Date(`${utcDates.now}Z`) });
        const timestampFormat = `YYYY-MM-DDTHH:mm:ss.${'S'.repeat(timestampPrecision)}`;
        const preAggregation = mockPreAggregation({
          timezone,
          timestampPrecision,
          timestampFormat,
          invalidateKeyQueries: [['SELECT key FROM test_table WHERE ts BETWEEN ? AND ?', [FROM_PARTITION_RANGE, TO_PARTITION_RANGE], { renewalThreshold: 60 }]],
        });
        const cacheQueryResult = jest.fn();

        for (const name of [...initial, ...renewed]) {
          cacheQueryResult.mockResolvedValueOnce(name ? [{ value: `${utcDates[name]}Z` }] : []);
        }
        const keyQueryResult = jest.fn().mockImplementation(async query => query[1]);
        const loader = new TestPartitionRangeLoader(
          {} as any, jest.fn(), { options: {}, cacheQueryResult } as any, {} as any,
          preAggregation as any, [], { keyQueryResult } as any,
          { maxPartitions: 10000, maxSourceRowLimit: 10000, waitForRenew: true, requestId: 'range-test' },
        );
        const invalidation = jest.spyOn(loader, 'getInvalidationKeyValues');
        const series = timeSeries('day', expectedRange(buildRange), { timestampPrecision });
        const boundaries = [series[0], series[series.length - 1]];

        expect(await loader.loadBuildRange(timestampFormat)).toEqual(expectedRange(result));
        expect(cacheQueryResult).toHaveBeenCalledTimes(4);
        expect(invalidation.mock.calls).toEqual(boundaries.filter(Boolean).map(range => [range]));
        expect(keyQueryResult).toHaveBeenCalledTimes(boundaries.filter(Boolean).length);

        for (const [i, range] of boundaries.entries()) {
          const [query, values] = preAggregation.preAggregationStartEndQueries[i] as QueryWithParams;
          const initialCall = cacheQueryResult.mock.calls[i];
          const renewedCall = cacheQueryResult.mock.calls[i + 2];
          expect(initialCall.slice(0, 2)).toEqual([query, values]);
          expect(renewedCall.slice(0, 2)).toEqual([query, values]);
          expect(initialCall[4]).toEqual(expect.objectContaining({ renewalKey: null }));
          const utcRange = range?.map(date => PreAggregationPartitionRangeLoader.inDbTimeZone(preAggregation as any, date));
          expect(renewedCall[4]).toEqual(expect.objectContaining({ renewalKey: range ? [utcRange] : null }));
          if (range) {
            expect(keyQueryResult.mock.calls[i][0].slice(0, 2)).toEqual([
              `SELECT key FROM ${PreAggregationPartitionRangeLoader.partitionTableName('test_table', 'day', range)} WHERE ts BETWEEN ? AND ?`, utcRange,
            ]);
          }
        }
      });

      it.each([false, true])('skips renewal queries without partitioning (empty: %s)', async (empty) => {
        jest.useFakeTimers({ now: new Date(`${utcDates.now}Z`) });
        const loader = createLoader({ timezone, timestampPrecision, partitionGranularity: undefined });
        const query = jest.mocked((loader as any).loadRangeQuery);
        query
          .mockResolvedValueOnce(empty ? [] : [{ value: utcDates.unpartitionedStart }])
          .mockResolvedValueOnce(empty ? [] : [{ value: utcDates.unpartitionedEnd }]);
        const invalidation = jest.spyOn(loader, 'getInvalidationKeyValues');
        const result = await loader.loadBuildRange();
        const dates: DatePair = empty ? ['now', 'now'] : ['unpartitionedStart', 'unpartitionedEnd'];
        expect(result).toEqual(dates.map(name => localDates[name]));
        expect(query).toHaveBeenCalledTimes(2);
        expect(query.mock.calls.every(call => call.length === 1)).toBe(true);
        expect(invalidation).not.toHaveBeenCalled();
      });
    });
  });
});

describe('PreAggregations', () => {
  let mockDriver: MockDriver | null = null;
  let mockExternalDriver: MockDriver | null = null;
  let mockDriverFactory: (() => Promise<MockDriver>) | null = null;
  let mockDriverReadOnlyFactory: (() => Promise<MockDriver>) | null = null;
  let mockExternalDriverFactory: (() => Promise<MockDriver>) | null = null;
  let queryCache: any = null;

  const defaultCacheKeyQuery: [string, any[], Record<string, any>] = ['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', [], {
    renewalThreshold: 10,
    external: false,
  }];

  const createBasicQuery = (overrides: Record<string, any> = {}): any => ({
    query: 'SELECT "orders__created_at_week" "orders__created_at_week", sum("orders__count") "orders__count" FROM (SELECT * FROM stb_pre_aggregations.orders_number_and_count20191101) as partition_union  WHERE ("orders__created_at_week" >= ($1::timestamptz::timestamptz AT TIME ZONE \'UTC\') AND "orders__created_at_week" <= ($2::timestamptz::timestamptz AT TIME ZONE \'UTC\')) GROUP BY 1 ORDER BY 1 ASC LIMIT 10000',
    values: ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z'],
    cacheKeyQueries: {
      renewalThreshold: 21600,
      queries: [defaultCacheKeyQuery]
    },
    preAggregations: [{
      preAggregationsSchema: 'stb_pre_aggregations',
      tableName: 'stb_pre_aggregations.orders_number_and_count20191101',
      loadSql: ['CREATE TABLE stb_pre_aggregations.orders_number_and_count20191101 AS SELECT\n      date_trunc(\'week\', ("orders".created_at::timestamptz AT TIME ZONE \'UTC\')) "orders__created_at_week", count("orders".id) "orders__count", sum("orders".number) "orders__number"\n    FROM\n      public.orders AS "orders"\n  WHERE ("orders".created_at >= $1::timestamptz AND "orders".created_at <= $2::timestamptz) GROUP BY 1', ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z']],
      invalidateKeyQueries: [defaultCacheKeyQuery],
    }],
    requestId: 'basic',
    ...overrides,
  });

  const basicQuery: any = createBasicQuery();
  const basicQueryExternal = createBasicQuery({ preAggregations: [{ ...basicQuery.preAggregations[0], external: true }] });
  const basicQueryWithRenew = createBasicQuery({ cacheMode: 'must-revalidate' });
  const basicQueryExternalWithRenew = createBasicQuery({ preAggregations: [{ ...basicQuery.preAggregations[0], external: true }], cacheMode: 'must-revalidate' });

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
        // Only reached by a query carrying `external: true`.
        externalDriverFactory: mockExternalDriverFactory as any,
      },
    );

    // Reset the shared in-memory cache store between tests
    (queryCache.getCacheDriver() as LocalCacheDriver).reset();
  });

  const createPreAggregations = (options: Record<string, any> = {}) => new PreAggregations(
    'TEST',
    mockDriverFactory as any,
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    () => {},
    queryCache!,
    {
      queueOptions: async () => ({
        executionTimeout: 1,
        concurrency: 2,
      }),
      ...options,
    },
  );

  describe('touch/used cache key cleanup', () => {
    let preAggregations: PreAggregations;

    beforeEach(() => {
      preAggregations = new PreAggregations(
        'TEST',
        mockDriverFactory as any,
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        () => {},
        queryCache!,
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
      jest.spyOn(mockDriver!, 'loadPreAggregationIntoTable')
        .mockRejectedValue(new Error('build boom'));

      const loadCache = new PreAggregationLoadCache(
        mockDriverFactory as any,
        queryCache!,
        preAggregations,
        { dataSource: 'default' },
      );

      const loader = new PreAggregationLoader(
        mockDriverFactory as any,
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        () => {},
        queryCache!,
        preAggregations,
        preAggregation,
        [],
        loadCache,
        { requestId: 'failed-build' },
      );

      await expect(loader.refresh(newVersionEntry as any, [] as any, mockDriver!))
        .rejects.toThrow('build boom');

      // The failed attempt must leave no touch/used markers behind, otherwise
      // repeated failures accumulate keys in cache until TTL (CORE-646).
      expect(await preAggregations.tablesTouched()).not.toContain(targetTableName);
      expect(await preAggregations.tablesUsed()).not.toContain(targetTableName);
    });
  });

  describe('isPartitionExist', () => {
    test('initializes a missing data source queue before checking the job result', async () => {
      const preAggregations = new PreAggregations(
        'TEST',
        mockDriverFactory as any,
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        () => {},
        queryCache!,
        {
          cacheAndQueueDriver: 'memory',
          queueOptions: async () => ({
            executionTimeout: 1,
            concurrency: 2,
          }),
        },
      );
      mockDriver!.tables.push('stb_pre_aggregations.orders_main');

      await expect(
        preAggregations.isPartitionExist(
          'request-id',
          false,
          'named_data_source',
          'stb_pre_aggregations',
          'stb_pre_aggregations.orders_main',
          ['job-key'],
          'job-token',
        )
      ).resolves.toEqual([true, 'done']);
    });
  });

  describe('refresh key memoization', () => {
    let preAggregations: PreAggregations;

    const preAggregation = {
      preAggregationsSchema: 'stb_pre_aggregations',
      tableName: 'stb_pre_aggregations.orders_memo',
      dataSource: 'default',
      external: false,
      loadSql: ['CREATE TABLE stb_pre_aggregations.orders_memo AS SELECT 1', []],
      invalidateKeyQueries: [defaultCacheKeyQuery],
    };

    const createLoadCache = (dataSource: string = 'default') => new PreAggregationLoadCache(
      mockDriverFactory as any,
      queryCache!,
      preAggregations,
      { dataSource },
    );

    const createPreAggLoader = (
      loadCache: PreAggregationLoadCache,
      options: Record<string, any> = {},
      preAggOverrides: Record<string, any> = {},
    ) => new PreAggregationLoader(
      mockDriverFactory as any,
      // eslint-disable-next-line @typescript-eslint/no-empty-function
      () => {},
      queryCache!,
      preAggregations,
      { ...preAggregation, ...preAggOverrides },
      [],
      loadCache,
      { requestId: 'refresh-key-memo', ...options },
    );

    beforeEach(() => {
      preAggregations = createPreAggregations();
    });

    test('refresh key identity covers sql, params and engine, but not policy', async () => {
      const loadCache = createLoadCache();
      const [sql] = defaultCacheKeyQuery;

      expect(loadCache.hasKeyQueryResult(defaultCacheKeyQuery)).toBe(false);
      await loadCache.keyQueryResult(defaultCacheKeyQuery, false, 10);

      expect(loadCache.hasKeyQueryResult(defaultCacheKeyQuery)).toBe(true);
      // Missing options element, and differing policy with `external` left absent — see
      // QueryCache.refreshKeyIdentity for why neither may move the key.
      expect(loadCache.hasKeyQueryResult(defaultCacheKeyQuery.slice(0, 2) as any)).toBe(true);
      expect(loadCache.hasKeyQueryResult([sql, [], { renewalThreshold: 1 }])).toBe(true);
      // The engine does change identity — same SQL run against Cube Store is a different query.
      expect(loadCache.hasKeyQueryResult([sql, [], { external: true }])).toBe(false);
      expect(loadCache.hasKeyQueryResult(['SELECT NOW() as unrelated', [], {}])).toBe(false);
    });

    test('a refresh key resolved against each engine gets its own cache entry', async () => {
      const [sql] = defaultCacheKeyQuery;
      const loadCache = createLoadCache();

      await loadCache.keyQueryResult([sql, [], { external: false }], false, 10);
      await loadCache.keyQueryResult([sql, [], { external: true }], false, 10);

      // Same SQL, different engine: conflating them would serve one engine's result for the other.
      expect(mockDriver!.executedQueries.filter(q => q === sql).length).toEqual(1);
      expect(mockExternalDriver!.executedQueries.filter(q => q === sql).length).toEqual(1);
    });

    test('a refresh key resolved against each data source gets its own cache entry', async () => {
      const [sql] = defaultCacheKeyQuery;

      await createLoadCache('default').keyQueryResult(defaultCacheKeyQuery, false, 10);
      await createLoadCache('staging').keyQueryResult(defaultCacheKeyQuery, false, 10);

      // The cache prefix only separates tenants, so without the data source in the key the second
      // load cache would serve the first one's row for a different database.
      expect(mockDriver!.executedQueries.filter(q => q === sql).length).toEqual(2);
    });

    test('an absent data source hashes as default', () => {
      // `loadRefreshKeysFromQuery` forwards `query.dataSource` untouched, so an absent one reaches
      // the same driver as `default` and must share its entry.
      expect(queryCache!.refreshKeyCacheKey(defaultCacheKeyQuery, undefined))
        .toEqual(queryCache!.refreshKeyCacheKey(defaultCacheKeyQuery, 'default'));
    });

    test('both refresh key paths store the same renewal key', async () => {
      const cacheKey = queryCache!.refreshKeyCacheKey(defaultCacheKeyQuery, 'default');
      const storedRenewalKey = async () => (await queryCache!.getCacheDriver().get(cacheKey)).renewalKey;

      await queryCache!.loadRefreshKey(defaultCacheKeyQuery, 3600, { dataSource: 'default', requestId: 'loadRefreshKey' });
      const throughLoadRefreshKey = await storedRenewalKey();

      // Each path has to write the entry itself, otherwise the second one just reads what the
      // first stored and any disagreement stays invisible.
      (queryCache!.getCacheDriver() as LocalCacheDriver).reset();
      await createLoadCache().keyQueryResult(defaultCacheKeyQuery, false, 10);
      const throughKeyQueryResult = await storedRenewalKey();

      // The same SQL can arrive as a cube cacheKeyQuery and as a pre-aggregation
      // invalidateKeyQuery, sharing this entry — disagreeing renewal keys would make each path look
      // stale to the other and re-fetch on every touch.
      expect(throughLoadRefreshKey).toEqual(throughKeyQueryResult);
    });

    test('warm invalidation keys are confirmed synchronously', async () => {
      const loadCache = createLoadCache();
      await loadCache.keyQueryResult(defaultCacheKeyQuery, false, 10);

      const result = await createPreAggLoader(loadCache, { waitForRenew: false }).loadPreAggregation(true);

      // A populated refreshKeyValues is the marker of the inline path; the deferred one reports [].
      expect(result!.refreshKeyValues.length).toEqual(1);
    });

    test('warm invalidation keys do not let externalRefresh build a pre-aggregation', async () => {
      const loadCache = createLoadCache();
      await loadCache.keyQueryResult(defaultCacheKeyQuery, false, 10);

      await expect(createPreAggLoader(loadCache, { externalRefresh: true }).loadPreAggregation(true))
        .rejects.toThrowError(/No pre-aggregation partitions were built yet/);
      expect(mockDriver!.tables).toEqual([]);
    });

    test('a pre-aggregation with no invalidation keys does not let externalRefresh build either', async () => {
      const noKeys = { invalidateKeyQueries: [] };

      // The one combination whose behaviour the guard changes: an empty key list used to leave
      // `notLoadedKey` undefined, which sent even an externalRefresh instance onto the building path.
      await expect(createPreAggLoader(createLoadCache(), { externalRefresh: true }, noKeys).loadPreAggregation(true))
        .rejects.toThrowError(/No pre-aggregation partitions were built yet/);
      await expect(createPreAggLoader(createLoadCache(), { externalRefresh: true }, noKeys).loadPreAggregation(false))
        .resolves.toBeNull();

      expect(mockDriver!.tables).toEqual([]);
    });
  });

  describe('local refresh key', () => {
    const REFRESH_KEY_SQL = 'SELECT FLOOR((UNIX_TIMESTAMP()) / 600) as refresh_key';
    const descriptor = { interval: 600, utcOffset: 0, dayOffset: 0, cron: false };

    const newQueryCache = (additional: Partial<QueryCacheOptions> = {}) => new QueryCache(
      'TEST',
      mockDriverFactory as any,
      // eslint-disable-next-line @typescript-eslint/no-empty-function
      () => {},
      {
        cacheAndQueueDriver: 'memory',
        queueOptions: async () => ({ executionTimeout: 1, concurrency: 2 }),
        ...additional,
      },
    );

    const newLoadCache = (additional: Partial<QueryCacheOptions> = {}) => {
      const cache = newQueryCache(additional);
      (cache.getCacheDriver() as LocalCacheDriver).reset();

      const preAggregations = new PreAggregations(
        'TEST',
        mockDriverFactory as any,
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        () => {},
        cache,
        { queueOptions: async () => ({ executionTimeout: 1, concurrency: 2 }) },
      );

      return new PreAggregationLoadCache(
        mockDriverFactory as any,
        cache,
        preAggregations,
        { dataSource: 'default' },
      );
    };

    test('keyQueryResult evaluates locally without querying the datasource', async () => {
      const loadCache = newLoadCache({ localRefreshKey: true });

      const result = await loadCache.keyQueryResult(
        [REFRESH_KEY_SQL, [], { external: true, renewalThreshold: 60, localRefreshKey: descriptor }],
        false,
        10,
      );

      expect(result).toEqual([{ refresh_key: String(Math.floor(Date.now() / 1000 / descriptor.interval)) }]);
      expect(mockDriver!.executedQueries).toEqual([]);
    });

    test('keyQueryResult evaluates locally under a refreshKeyRenewalThreshold', async () => {
      const day = 24 * 60 * 60;
      const loadCache = newLoadCache({ localRefreshKey: true, refreshKeyRenewalThreshold: day });
      const now = 86_400_000 + 600_000;

      const nowSpy = jest.spyOn(Date, 'now').mockReturnValue(now);
      try {
        const result = await loadCache.keyQueryResult(
          [REFRESH_KEY_SQL, [], { external: true, renewalThreshold: 60, localRefreshKey: descriptor }],
          false,
          10,
        );

        expect(result).toEqual(evaluateLocalRefreshKey(descriptor, now));
        expect(mockDriver!.executedQueries).toEqual([]);
      } finally {
        nowSpy.mockRestore();
      }
    });

    test('keyQueryResult still queries when the flag is off', async () => {
      const loadCache = newLoadCache({ localRefreshKey: false });

      await loadCache.keyQueryResult(
        [REFRESH_KEY_SQL, [], { external: false, renewalThreshold: 60, localRefreshKey: descriptor }],
        false,
        10,
      );

      expect(mockDriver!.executedQueries).toEqual([REFRESH_KEY_SQL]);
    });

    test('keyQueryResult still queries an incremental key that carries no descriptor', async () => {
      const loadCache = newLoadCache({ localRefreshKey: true });
      const incrementalSql = 'SELECT CASE WHEN NOW() < $1 THEN FLOOR((UNIX_TIMESTAMP()) / 3600) END as refresh_key';

      await loadCache.keyQueryResult(
        [incrementalSql, [], {
          external: false,
          renewalThreshold: 300,
          incremental: true,
          renewalThresholdOutsideUpdateWindow: 86400,
        }],
        false,
        10,
      );

      expect(mockDriver!.executedQueries).toEqual([incrementalSql]);
    });

    // A single load reads the invalidation keys several times (contentVersion, the returned
    // refreshKeyValues, the refresh queue key). If the clock were re-read, a load crossing an
    // interval boundary would look a table up under one content version and enqueue it under
    // another.
    test('keyQueryResult is stable across an interval boundary within one load cache', async () => {
      const loadCache = newLoadCache({ localRefreshKey: true });
      const key: [string, any[], Record<string, any>] =
        [REFRESH_KEY_SQL, [], { external: true, renewalThreshold: 60, localRefreshKey: descriptor }];

      const nowSpy = jest.spyOn(Date, 'now').mockReturnValue(600_000);

      try {
        const first = await loadCache.keyQueryResult(key, false, 10);
        expect(first).toEqual([{ refresh_key: '1' }]);

        nowSpy.mockReturnValue(1_200_000);
        const second = await loadCache.keyQueryResult(key, false, 10);

        expect(second).toEqual(first);
      } finally {
        nowSpy.mockRestore();
      }
    });
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
          queueOptions: async () => ({
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

    // A jobed build gets back a flat list of entries and has to tell them apart.
    // https://github.com/cube-js/cube/issues/11615
    test('each entry carries the identity of the descriptor it was built from', async () => {
      const { preAggregationsTablesToTempTables: result } = await preAggregations!.loadAllPreAggregationsIfNeeded(
        createBasicQuery({
          cacheMode: 'must-revalidate',
          preAggregations: [{
            ...basicQuery.preAggregations[0],
            preAggregationId: 'Orders.numberAndCount',
            dataSource: 'orders_ds',
            timezone: 'America/Los_Angeles',
          }],
        })
      );

      expect(result[0][1]).toMatchObject({
        preAggregationId: 'Orders.numberAndCount',
        dataSource: 'orders_ds',
        timezone: 'America/Los_Angeles',
      });
    });

    test('an entry built without a named data source falls back to the default one', async () => {
      const { preAggregationsTablesToTempTables: result } = await preAggregations!.loadAllPreAggregationsIfNeeded(basicQueryWithRenew);

      expect(result[0][1].dataSource).toEqual('default');
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
          queueOptions: async () => ({
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
          queueOptions: async () => ({
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
          queueOptions: async () => ({
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
          queueOptions: async () => ({
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
          queueOptions: async () => ({
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
          queueOptions: async () => ({
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

  // @link https://github.com/cube-js/cube/issues/11682
  describe('lambda source query loading', () => {
    const buildRangeEnd = '2024-01-02T23:59:59.999';

    // The PreAggregationLoader.prototype spy below would otherwise leak into later tests.
    afterEach(() => {
      jest.restoreAllMocks();
    });

    const createLambdaLoader = (matchedTimeDimensionDateRange?: [string, string]) => {
      const loader = new PreAggregationPartitionRangeLoader(
        {} as any, // driverFactory
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        () => {}, // logger
        { options: {} } as any, // queryCache
        {} as any, // preAggregations
        mockPreAggregation({
          preAggregationId: 'Orders.d',
          rollupLambdaId: 'Orders.d_lambda',
          lastRollupLambda: true,
          unionWithSourceData: true,
          matchedTimeDimensionDateRange,
        }) as any,
        [], // preAggregationsTablesToTempTables
        { getTableColumnTypes: jest.fn().mockResolvedValue([{ name: 'ts', type: 'timestamp' }]) } as any,
        {
          lambdaQuery: {
            sqlAndParams: ['SELECT * FROM public.orders WHERE ts > ?', [FROM_PARTITION_RANGE]],
            cacheKeyQueries: [],
          },
        } as any,
      );

      jest.spyOn(loader as any, 'partitionRanges').mockResolvedValue({
        buildRange: ['2024-01-01T00:00:00.000', buildRangeEnd],
        partitionRanges: [['2024-01-02T00:00:00.000', buildRangeEnd]],
      });
      jest.spyOn(PreAggregationLoader.prototype, 'loadPreAggregation').mockResolvedValue({
        targetTableName: 'stb_pre_aggregations.orders_d20240102_abc_def',
        refreshKeyValues: [],
        lastUpdatedAt: 1,
        buildRangeEnd,
      } as any);
      const downloadLambdaTable = jest.spyOn(loader as any, 'downloadLambdaTable').mockResolvedValue({
        name: 'lambda_stb_pre_aggregations_orders_d',
        columns: [],
        csvRows: '',
      });

      return { loader, downloadLambdaTable };
    };

    test('skips the source query when the requested range is inside the built range', async () => {
      const { loader, downloadLambdaTable } = createLambdaLoader(['2024-01-01T00:00:00.000', buildRangeEnd]);

      const result: any = await loader.loadPreAggregations();

      expect(downloadLambdaTable).not.toHaveBeenCalled();
      expect(result.lambdaTable).toBeUndefined();
      expect(result.targetTableName).toEqual('stb_pre_aggregations.orders_d20240102_abc_def');
    });

    test('runs the source query when the requested range extends past the built range', async () => {
      const { loader, downloadLambdaTable } = createLambdaLoader(['2024-01-01T00:00:00.000', '2024-01-05T23:59:59.999']);

      const result: any = await loader.loadPreAggregations();

      expect(downloadLambdaTable).toHaveBeenCalledWith(buildRangeEnd, [{ name: 'ts', type: 'timestamp' }]);
      expect(result.lambdaTable?.name).toEqual('lambda_stb_pre_aggregations_orders_d');
      expect(result.targetTableName).toMatch(/UNION ALL SELECT \* FROM lambda_stb_pre_aggregations_orders_d/);
    });

    test('runs the source query when no date range was requested', async () => {
      const { loader, downloadLambdaTable } = createLambdaLoader(undefined);

      await loader.loadPreAggregations();

      expect(downloadLambdaTable).toHaveBeenCalled();
    });
  });

  describe('lambdaSourceDataCovered', () => {
    const covered = (matchedTimeDimensionDateRange: any, buildRangeEnd: any) => (
      createLoader({ matchedTimeDimensionDateRange }) as any
    ).lambdaSourceDataCovered(buildRangeEnd);

    test('covered when the requested range ends within the built range', () => {
      expect(covered(['2024-01-01T00:00:00.000', '2024-01-02T23:59:59.999'], '2024-01-02T23:59:59.999')).toBe(true);
      expect(covered(['2024-01-01T00:00:00.000', '2024-01-02T23:59:59.999'], '2024-01-03T23:59:59.999')).toBe(true);
    });

    test('not covered when the requested range extends past the built range', () => {
      expect(covered(['2024-01-01T00:00:00.000', '2024-01-05T23:59:59.999'], '2024-01-03T23:59:59.999')).toBe(false);
    });

    test('normalizes a buildRangeEnd read back from the DB with a Z suffix', () => {
      expect(covered(['2024-01-01T00:00:00.000', '2024-01-02T23:59:59.999'], '2024-01-02T23:59:59.999Z')).toBe(true);
    });

    test('keeps the source query when either bound is unknown', () => {
      expect(covered(undefined, '2024-01-02T23:59:59.999')).toBe(false);
      expect(covered(['2024-01-01T00:00:00.000', '2024-01-02T23:59:59.999'], undefined)).toBe(false);
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
      expect((preAggDesc.loadSql as any)[0].includes('test_table')).toBeTruthy();
      expect((preAggDesc.loadSql as any)[1][0]).toEqual(FROM_PARTITION_RANGE);
      expect((preAggDesc.loadSql as any)[1][1]).toEqual(TO_PARTITION_RANGE);
      expect(preAggDesc.structureVersionLoadSql).toBeUndefined();
    });
  });

  describe('partition UTC ranges', () => {
    afterEach(() => {
      jest.restoreAllMocks();
    });

    // America/New_York enters DST on 2024-03-10, so the partition start is EST and its end is EDT.
    const start = '2024-03-10T00:00:00.000';
    const noon = '2024-03-10T12:00:00.000';
    const end = '2024-03-10T23:59:59.999';
    const utcStart = '2024-03-10T05:00:00.000';
    const utcNoon = '2024-03-10T16:00:00.000';
    const utcEnd = '2024-03-11T03:59:59.999';
    // Widens the millisecond fraction of a fixture timestamp: .000 -> .000000, .999 -> .999999.
    const withPrecision = (ts: string, precision: number) => ts + ts.slice(-1).repeat(precision - 3);
    const originalParams = [FROM_PARTITION_RANGE, TO_PARTITION_RANGE, FROM_PARTITION_RANGE, 'literal'];
    const query: QueryWithParams = [
      'SELECT * FROM test_table WHERE ts >= ? AND ts <= ? AND ts >= ? AND label = ?',
      [...originalParams],
      { renewalThreshold: 60 },
    ];

    test.each([
      { name: 'unclipped', precision: 3, buildRangeEnd: end, partitionInvalidateKeyQueries: [query], loadEnd: end, utcLoadEnd: utcEnd, sharesLoadSql: true, conversions: 2 },
      { name: 'clipped', precision: 3, buildRangeEnd: noon, partitionInvalidateKeyQueries: [query], loadEnd: noon, utcLoadEnd: utcNoon, sharesLoadSql: false, conversions: 4 },
      // Real-time pre-aggregations never clip, so the build range end is ignored.
      { name: 'real-time', precision: 3, buildRangeEnd: noon, partitionInvalidateKeyQueries: [], loadEnd: end, utcLoadEnd: utcEnd, sharesLoadSql: true, conversions: 2 },
      { name: 'microsecond-precision', precision: 6, buildRangeEnd: noon, partitionInvalidateKeyQueries: [query], loadEnd: noon, utcLoadEnd: utcNoon, sharesLoadSql: false, conversions: 4 },
    ])('shares converted boundaries across SQL queries of a $name partition', async ({ precision, buildRangeEnd, partitionInvalidateKeyQueries, loadEnd, utcLoadEnd, sharesLoadSql, conversions }) => {
      const at = (ts: string) => withPrecision(ts, precision);
      const loader = createLoader({
        timezone: 'America/New_York',
        timestampFormat: `YYYY-MM-DDTHH:mm:ss.${'S'.repeat(precision)}`,
        timestampPrecision: precision,
        loadSql: query,
        sql: query,
        invalidateKeyQueries: [query],
        partitionInvalidateKeyQueries,
        indexesSql: [{ indexName: 'test_index', sql: query }],
        previewSql: query,
      });
      jest.spyOn(loader, 'loadBuildRange').mockResolvedValue([at(start), at(buildRangeEnd)]);
      const convert = jest.spyOn(PreAggregationPartitionRangeLoader, 'inDbTimeZone');

      const [partition] = await loader.partitionPreAggregations();

      const sql = query[0].replace('test_table', 'test_table20240310');
      const loadTuple = [sql, [at(utcStart), at(utcLoadEnd), at(utcStart), 'literal'], { renewalThreshold: 60 }];
      const fullTuple = [sql, [at(utcStart), at(utcEnd), at(utcStart), 'literal'], { renewalThreshold: 60 }];
      expect(partition.loadSql).toEqual(loadTuple);
      expect(partition.sql).toEqual(loadTuple);
      expect(partition.structureVersionLoadSql).toEqual(fullTuple);
      expect(partition.invalidateKeyQueries).toEqual([fullTuple]);
      expect(partition.partitionInvalidateKeyQueries).toEqual(partitionInvalidateKeyQueries.map(() => fullTuple));
      expect(partition.indexesSql[0].sql).toEqual(fullTuple);
      expect(partition.previewSql).toEqual(fullTuple);
      expect(partition.buildRangeStart).toBe(at(start));
      expect(partition.buildRangeEnd).toBe(at(loadEnd));
      if (sharesLoadSql) {
        expect(partition.loadSql).toBe(partition.structureVersionLoadSql);
      } else {
        expect(partition.loadSql).not.toBe(partition.structureVersionLoadSql);
      }
      // Conversion cost depends on ranges, not the number of SQL queries or placeholders.
      expect(convert).toHaveBeenCalledTimes(conversions);
      expect(query[1]).toEqual(originalParams);
    });

    test('shares converted boundaries across invalidation key queries', async () => {
      const keyQueryResult = jest.fn().mockResolvedValue('refresh-key');
      const loader = createLoader({ timezone: 'America/New_York', invalidateKeyQueries: [query, query] }, {}, { keyQueryResult });
      const convert = jest.spyOn(PreAggregationPartitionRangeLoader, 'inDbTimeZone');

      const result = await loader.getInvalidationKeyValues([start, end]);

      expect(result).toEqual(['refresh-key', 'refresh-key']);
      expect(keyQueryResult).toHaveBeenCalledTimes(2);

      for (const [sql] of keyQueryResult.mock.calls) {
        expect(sql[0]).toContain('test_table20240310');
        expect(sql[1]).toEqual([utcStart, utcEnd, utcStart, 'literal']);
      }
      expect(convert).toHaveBeenCalledTimes(2);
    });

    test.each([[undefined], [[]]])('skips UTC conversion when invalidation queries are %p', async (invalidateKeyQueries) => {
      const loader = createLoader({ invalidateKeyQueries });
      const convert = jest.spyOn(PreAggregationPartitionRangeLoader, 'inDbTimeZone');

      await expect(loader.getInvalidationKeyValues([start, end])).resolves.toEqual([]);

      expect(convert).not.toHaveBeenCalled();
    });
  });

  describe('partitionPreAggregations', () => {
    test('uses local load boundaries for incremental renewal and sealing', async () => {
      jest.useFakeTimers({ now: new Date('2024-03-10T17:30:00.000Z') });

      try {
        const query: QueryWithParams = ['SELECT * FROM test_table WHERE ts BETWEEN ? AND ?', [FROM_PARTITION_RANGE, TO_PARTITION_RANGE], {
          incremental: true,
          updateWindowSeconds: 3600,
          renewalThreshold: 60,
          renewalThresholdOutsideUpdateWindow: 30,
        }];
        const loader = createLoader({
          timezone: 'America/New_York',
          loadSql: query,
          partitionInvalidateKeyQueries: [query],
          updateWindowSeconds: 3600,
        });
        jest.spyOn(loader, 'loadBuildRange').mockResolvedValue(['2024-03-10T00:00:00.000', '2024-03-10T12:00:00.000']);

        const [partition] = await loader.partitionPreAggregations();

        expect(partition.loadSql[2].renewalThreshold).toBe(30);
        expect(partition.structureVersionLoadSql[2].renewalThreshold).toBe(60);
        expect(partition.partitionInvalidateKeyQueries[0][2].renewalThreshold).toBe(60);
        expect(partition.sealAt).toBe('2024-03-10T17:00:00.000Z');
      } finally {
        jest.useRealTimers();
      }
    });

    test.each([
      ['UTC', '2024-01-03T00:00:00.000', '2024-01-03T12:00:00.000', '2024-01-03T23:59:59.999'],
      ['America/New_York', '2024-01-03T05:00:00.000', '2024-01-03T17:00:00.000', '2024-01-04T04:59:59.999'],
    ])('should keep separate load and structure SQL parameters for a clipped partition in %s', async (timezone, start, loadEnd, structureEnd) => {
      const loader = createLoader({
        timezone,
        partitionInvalidateKeyQueries: [['SELECT NOW()', [], {}]],
      });
      jest.spyOn(loader, 'loadBuildRange').mockResolvedValue([
        '2024-01-01T00:00:00.000',
        '2024-01-03T12:00:00.000',
      ]);

      const results = await loader.partitionPreAggregations();
      expect(results).toHaveLength(3);

      for (const partition of results.slice(0, -1)) {
        expect(partition.loadSql).toBe(partition.structureVersionLoadSql);
      }

      const lastPartition = results[2];
      expect(lastPartition.loadSql[1]).toEqual([start, loadEnd]);
      expect(lastPartition.structureVersionLoadSql[1]).toEqual([start, structureEnd]);
      expect(lastPartition.buildRangeEnd).toEqual('2024-01-03T12:00:00.000');
    });

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
    const rangeA: [string, string] = ['2024-01-01T00:00:00.000', '2024-01-02T12:00:00.000'];
    const rangeB: [string, string] = ['2024-01-01T00:00:00.000', '2024-01-02T12:00:01.000'];
    const cache = () => {
      const entries = new Map<string, unknown>();
      const compilerCacheFn = <T>(key: string[], fn: () => T): T => {
        const serializedKey = JSON.stringify(key);
        if (!entries.has(serializedKey)) {
          entries.set(serializedKey, fn());
        }
        return entries.get(serializedKey) as T;
      };
      return { entries, compilerCacheFn };
    };

    test('reuses the plan without generating a partition series on a hit', async () => {
      const { compilerCacheFn } = cache();
      const loader = createLoader({}, { compilerCacheFn });
      const series = jest.spyOn(PreAggregationPartitionRangeLoader, 'timeSeries');

      try {
        const first = await loader.partitionPreAggregations();
        expect(series).toHaveBeenCalledTimes(1);
        expect(await loader.partitionPreAggregations()).toBe(first);
        expect(series).toHaveBeenCalledTimes(1);
        expect(first.map(p => p.tableName)).toEqual(['test_table20240101', 'test_table20240102', 'test_table20240103']);
      } finally {
        series.mockRestore();
      }
    });

    test('replaces A → B → A, updates second-level bounds and preserves old arrays', async () => {
      const { compilerCacheFn, entries } = cache();
      const loader = createLoader({ partitionInvalidateKeyQueries: [['SELECT 1', []]] }, { compilerCacheFn });
      const bounds = jest.spyOn(loader, 'loadBuildRange').mockResolvedValue(rangeA);
      const first = await loader.partitionPreAggregations();
      const original = JSON.stringify(first);
      bounds.mockResolvedValue(rangeB);
      const second = await loader.partitionPreAggregations();
      expect(second).not.toBe(first);
      expect(second[1].loadSql[1][1]).toBe(rangeB[1]);
      expect(second[1].buildRangeEnd).toBe(rangeB[1]);
      expect(first[1].loadSql[1][1]).toBe(rangeA[1]);
      expect(JSON.stringify(first)).toBe(original);
      bounds.mockResolvedValue(rangeA);
      const third = await loader.partitionPreAggregations();
      expect(third).not.toBe(first);
      expect(third).toEqual(first);
      expect(entries.size).toBe(1);
      expect([...entries.values()]).toEqual([{ rangeKey: JSON.stringify(rangeA), descriptions: third }]);
    });

    test('checks the current limit on hits and keeps the previous plan after failures', async () => {
      const { compilerCacheFn } = cache();
      const loader = createLoader({}, { compilerCacheFn, maxPartitions: 2 });
      const bounds = jest.spyOn(loader, 'loadBuildRange').mockResolvedValue(rangeA);
      const first = await loader.partitionPreAggregations();
      const stricter = createLoader({}, { compilerCacheFn, maxPartitions: 1 });
      jest.spyOn(stricter, 'loadBuildRange').mockResolvedValue(rangeA);
      await expect(stricter.partitionPreAggregations()).rejects.toThrow('requested to build 2 partitions which exceeds the maximum number of partitions per pre-aggregation of 1');
      bounds.mockResolvedValue(['2024-01-01T00:00:00.000', '2024-01-03T00:00:00.000']);
      await expect(loader.partitionPreAggregations()).rejects.toThrow('requested to build 3 partitions');
      bounds.mockResolvedValue(rangeB);
      const descriptionSpy = jest.spyOn(loader, 'partitionPreAggregationDescription').mockImplementationOnce(() => { throw new Error('expansion failed'); });
      await expect(loader.partitionPreAggregations()).rejects.toThrow('expansion failed');
      descriptionSpy.mockRestore();
      bounds.mockResolvedValue(rangeA);
      expect(await loader.partitionPreAggregations()).toBe(first);
    });

    test.each([
      { preAggregationId: 'Other.byDay' },
      { tableName: 'other_table' },
      { dataSource: 'other' },
      { timezone: 'America/New_York' },
      { partitionGranularity: 'hour' },
      { timestampFormat: 'YYYY-MM-DDTHH:mm:ss.SSSSSS' },
      { timestampPrecision: 6 },
    ])('isolates identities with a shared dependency callback: %j', async (overrides) => {
      const { compilerCacheFn, entries } = cache();
      const firstLoader = createLoader({}, { compilerCacheFn });
      const otherLoader = createLoader(overrides, { compilerCacheFn });
      const first = await firstLoader.partitionPreAggregations();
      const other = await otherLoader.partitionPreAggregations();
      expect(other).not.toBe(first);
      expect(await firstLoader.partitionPreAggregations()).toBe(first);
      expect(await otherLoader.partitionPreAggregations()).toBe(other);
      expect(entries.size).toBe(2);
    });

    test('keys the plan by the effective intersection and falls back to the last partition', async () => {
      const { compilerCacheFn } = cache();
      const matchedTimeDimensionDateRange = ['2024-01-02T00:00:00.000', '2024-01-02T23:59:59.999'];
      const loader = createLoader({ matchedTimeDimensionDateRange }, { compilerCacheFn });
      const bounds = jest.spyOn(loader, 'loadBuildRange').mockResolvedValue([
        '2024-01-01T00:00:00.000', '2024-01-03T23:59:59.999',
      ]);
      const first = await loader.partitionPreAggregations();
      expect(first.map(p => p.tableName)).toEqual(['test_table20240102']);
      bounds.mockResolvedValue(['2024-01-01T01:00:00.000', '2024-01-03T23:59:59.999']);
      expect(await loader.partitionPreAggregations()).toBe(first);
      bounds.mockResolvedValue(['2024-01-04T00:00:00.000', '2024-01-05T12:00:00.000']);
      const fallback = await loader.partitionPreAggregations();
      expect(fallback.map(p => p.tableName)).toEqual(['test_table20240105']);
      // externalRefresh retries using the full build range, ignoring the unmatched query bounds.
      const full = await loader.partitionRanges(true);
      expect(full.partitionRanges).toHaveLength(2);
      expect(full.buildRange).toEqual(['2024-01-04T00:00:00.000', '2024-01-05T12:00:00.000']);
    });

    test.each([{ partitionGranularity: undefined }, { expandedPartition: true }])('passes through unpartitioned or expanded descriptions: %j', async overrides => {
      const compilerCacheFn = jest.fn((_key, fn) => fn());
      const loader = createLoader(overrides, { compilerCacheFn });
      const bounds = jest.spyOn(loader, 'loadBuildRange');
      expect((await loader.partitionPreAggregations())[0]).toBe((loader as any).preAggregation);
      expect(bounds).not.toHaveBeenCalled();
      expect(compilerCacheFn).not.toHaveBeenCalled();
    });

    test('ordinary query range generation remains uncached', async () => {
      const compilerCacheFn = jest.fn((_key, fn) => fn());
      const loader = createLoader({}, { compilerCacheFn });
      const first = await loader.partitionRanges();
      const second = await loader.partitionRanges();
      expect(second).toEqual(first);
      expect(second.partitionRanges).not.toBe(first.partitionRanges);
      expect(compilerCacheFn).not.toHaveBeenCalled();
    });

    test.each([undefined, (_key, fn) => fn()])('does not retain plans without persistent SQL caching (%p)', async compilerCacheFn => {
      const loader = createLoader({}, { compilerCacheFn });
      const first = await loader.partitionPreAggregations();
      expect(await loader.partitionPreAggregations()).toEqual(first);
      expect(await loader.partitionPreAggregations()).not.toBe(first);
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
              'CREATE INDEX m_x_c_actionable_hourly_agg_main_with_index_month1_device_tag_description_index ON prod_pre_aggregations_mxc.m_x_c_actionable_hourly_agg_main_with_index_month120260112 (`m_x_c_actionable_hourly_agg__device_name`, `m_x_c_actionable_hourly_agg__tag_name`, `m_x_c_actionable_hourly_agg__description`, `m_x_c_actionable_hourly_agg__timestamp_hour`)',
              [],
              {}
            ]
          },
          {
            indexName: 'm_x_c_actionable_hourly_agg_main_with_index_month1_tag_description_device_index',
            sql: [
              'CREATE INDEX m_x_c_actionable_hourly_agg_main_with_index_month1_tag_description_device_index ON prod_pre_aggregations_mxc.m_x_c_actionable_hourly_agg_main_with_index_month120260112 (`m_x_c_actionable_hourly_agg__tag_name`, `m_x_c_actionable_hourly_agg__description`, `m_x_c_actionable_hourly_agg__device_name`)',
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
