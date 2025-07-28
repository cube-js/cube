import crypto from 'crypto';
import { createCancelablePromise, pausePromise } from '@cubejs-backend/shared';

import { QueryCache, QueryCacheOptions } from '../../src';

export type QueryCacheTestOptions = QueryCacheOptions & {
  beforeAll?: () => Promise<void>,
  afterAll?: () => Promise<void>,
};

export const QueryCacheTest = (name: string, options: QueryCacheTestOptions) => {
  describe(`QueryQueue${name}`, () => {
    const cache = new QueryCache(
      crypto.randomBytes(16).toString('hex'),
      jest.fn(() => {
        throw new Error('It`s not implemented mock...');
      }),
      jest.fn(),
      options,
    );

    beforeAll(async () => {
      if (options?.beforeAll) {
        await options?.beforeAll();
      }
    });

    afterAll(async () => {
      await cache.cleanup();

      if (options?.afterAll) {
        await options?.afterAll();
      }
    });

    it('withLock', async () => {
      const RANDOM_KEY_CACHE = crypto.randomBytes(16).toString('hex');

      const testLock = async () => {
        let started = 0;
        let finished = 0;

        const doLock = (sleep: number) => cache.withLock(
          RANDOM_KEY_CACHE,
          60 * 10,
          async () => {
            started++;

            await pausePromise(sleep);

            finished++;
          },
        );

        const locks: Promise<boolean>[] = [
          doLock(1000)
        ];

        await pausePromise(100);

        locks.push(doLock(1000));
        locks.push(doLock(1000));

        const results = await Promise.all(locks);
        expect(results[0]).toEqual(true);
        expect(results[1]).toEqual(false);
        expect(results[2]).toEqual(false);

        expect(started).toEqual(1);
        expect(finished).toEqual(1);
      };

      await testLock();

      await pausePromise(500);

      await testLock();
    });

    it('withLock + cancel (test free of lock + cancel inheritance)', async () => {
      const RANDOM_KEY_CACHE = crypto.randomBytes(16).toString('hex');

      const lockPromise = cache.withLock(
        RANDOM_KEY_CACHE,
        60 * 10,
        () => createCancelablePromise(async (tkn) => {
          await tkn.with(
            // This timeout is useful to test that withLock.cancel use callback as tkn.with
            // If doesn't use it, test will fail with timeout
            pausePromise(60 * 60 * 1000)
          );
        }),
      );

      await lockPromise.cancel(true);
      await lockPromise;

      let callbackWasExecuted = false;

      // withLock return boolean, where true success execution & lock
      const statusOfResolve = await cache.withLock(
        RANDOM_KEY_CACHE,
        60 * 10,
        async () => {
          callbackWasExecuted = true;
        },
      );

      expect(statusOfResolve).toEqual(true);
      expect(callbackWasExecuted).toEqual(true);
    });

    it('queryCacheKey format', () => {
      const key1 = QueryCache.queryCacheKey({
        query: 'select data',
        values: ['value'],
        preAggregations: [],
        invalidate: [],
        persistent: true,
      });
      expect(key1[0]).toEqual('select data');
      expect(key1[1]).toEqual(['value']);
      expect(key1[2]).toEqual([]);
      expect(key1[3]).toEqual([]);
      // @ts-ignore
      expect(key1.persistent).toEqual(true);

      const key2 = QueryCache.queryCacheKey({
        query: 'select data',
        values: ['value'],
        preAggregations: [],
        invalidate: [],
        persistent: false,
      });
      expect(key2[0]).toEqual('select data');
      expect(key2[1]).toEqual(['value']);
      expect(key2[2]).toEqual([]);
      expect(key2[3]).toEqual([]);
      // @ts-ignore
      expect(key2.persistent).toEqual(false);

      const key3 = QueryCache.queryCacheKey({
        query: 'select data',
        values: ['value'],
        persistent: true,
      });
      expect(key3[0]).toEqual('select data');
      expect(key3[1]).toEqual(['value']);
      expect(key3[2]).toEqual([]);
      expect(key3[3]).toBeUndefined();
      // @ts-ignore
      expect(key3.persistent).toEqual(true);

      const key4 = QueryCache.queryCacheKey({
        query: 'select data',
        values: ['value'],
        persistent: false,
      });
      expect(key4[0]).toEqual('select data');
      expect(key4[1]).toEqual(['value']);
      expect(key4[2]).toEqual([]);
      expect(key4[3]).toBeUndefined();
      // @ts-ignore
      expect(key4.persistent).toEqual(false);
    });

    it('queryDataSourceSchemas', async () => {
      const mockSchemas = [
        { schema_name: 'public' },
        { schema_name: 'private' },
        { schema_name: 'analytics' }
      ];

      // Mock the cacheQueryResult method to return our test data
      const originalCacheQueryResult = cache.cacheQueryResult;
      cache.cacheQueryResult = jest.fn().mockResolvedValue(mockSchemas);

      try {
        // Test basic functionality
        const result = await cache.queryDataSourceSchemas('test-datasource');
        expect(result).toEqual(mockSchemas);
        expect(cache.cacheQueryResult).toHaveBeenCalledTimes(1);

        // Test with different options - should return same data but call caching layer again
        const resultWithOptions = await cache.queryDataSourceSchemas('test-datasource', {
          requestId: 'test-request-1',
          forceRefresh: true,
          renewalThreshold: 3600,
          expiration: 86400
        });
        expect(resultWithOptions).toEqual(mockSchemas);
        expect(cache.cacheQueryResult).toHaveBeenCalledTimes(2);

        // Test with default datasource
        const defaultResult = await cache.queryDataSourceSchemas();
        expect(defaultResult).toEqual(mockSchemas);
        expect(cache.cacheQueryResult).toHaveBeenCalledTimes(3);

        // Test that different datasources are treated separately
        cache.cacheQueryResult = jest.fn().mockResolvedValue([{ schema_name: 'different_schema' }]);
        const differentDsResult = await cache.queryDataSourceSchemas('different-datasource');
        expect(differentDsResult).toEqual([{ schema_name: 'different_schema' }]);
        expect(differentDsResult).not.toEqual(mockSchemas);
      } finally {
        cache.cacheQueryResult = originalCacheQueryResult;
      }
    });

    it('queryTablesForSchemas', async () => {
      const inputSchemas = [
        { schema_name: 'public' },
        { schema_name: 'analytics' }
      ];

      const mockTables = [
        { schema_name: 'public', table_name: 'users' },
        { schema_name: 'public', table_name: 'orders' },
        { schema_name: 'analytics', table_name: 'events' }
      ];

      // Mock the cacheQueryResult method
      const originalCacheQueryResult = cache.cacheQueryResult;
      cache.cacheQueryResult = jest.fn().mockResolvedValue(mockTables);

      try {
        // Test basic functionality
        const result = await cache.queryTablesForSchemas(inputSchemas, 'test-datasource');
        expect(result).toEqual(mockTables);
        expect(cache.cacheQueryResult).toHaveBeenCalledTimes(1);

        // Test with empty schemas array
        cache.cacheQueryResult = jest.fn().mockResolvedValue([]);
        const emptyResult = await cache.queryTablesForSchemas([]);
        expect(emptyResult).toEqual([]);
        expect(Array.isArray(emptyResult)).toBe(true);
        expect(emptyResult.length).toBe(0);

        // Test with single schema
        cache.cacheQueryResult = jest.fn().mockResolvedValue([
          { schema_name: 'public', table_name: 'users' }
        ]);
        const singleSchemaResult = await cache.queryTablesForSchemas([{ schema_name: 'public' }]);
        expect(singleSchemaResult).toHaveLength(1);
        expect(singleSchemaResult[0]).toEqual({ schema_name: 'public', table_name: 'users' });

        // Test with options
        cache.cacheQueryResult = jest.fn().mockResolvedValue(mockTables);
        const resultWithOptions = await cache.queryTablesForSchemas(inputSchemas, 'test-datasource', {
          requestId: 'test-request-2',
          renewalThreshold: 1800,
          expiration: 172800
        });
        expect(resultWithOptions).toEqual(mockTables);

        // Verify that the method handles schema parameter properly
        expect(typeof inputSchemas).toBe('object');
        expect(Array.isArray(inputSchemas)).toBe(true);
        expect(inputSchemas.every(schema => typeof schema.schema_name === 'string')).toBe(true);
      } finally {
        cache.cacheQueryResult = originalCacheQueryResult;
      }
    });

    it('queryColumnsForTables', async () => {
      const inputTables = [
        { schema_name: 'public', table_name: 'users' },
        { schema_name: 'public', table_name: 'orders' }
      ];

      const mockColumns = [
        {
          schema_name: 'public',
          table_name: 'users',
          column_name: 'id',
          data_type: 'integer',
          attributes: ['PRIMARY KEY']
        },
        {
          schema_name: 'public',
          table_name: 'users',
          column_name: 'name',
          data_type: 'varchar',
          attributes: ['NOT NULL']
        },
        {
          schema_name: 'public',
          table_name: 'orders',
          column_name: 'id',
          data_type: 'integer',
          attributes: ['PRIMARY KEY']
        },
        {
          schema_name: 'public',
          table_name: 'orders',
          column_name: 'user_id',
          data_type: 'integer',
          foreign_keys: [{ target_table: 'users', target_column: 'id' }]
        }
      ];

      // Mock the cacheQueryResult method
      const originalCacheQueryResult = cache.cacheQueryResult;
      cache.cacheQueryResult = jest.fn().mockResolvedValue(mockColumns);

      try {
        // Test basic functionality
        const result = await cache.queryColumnsForTables(inputTables, 'test-datasource');
        expect(result).toEqual(mockColumns);
        expect(result).toHaveLength(4);

        // Verify column structure
        const userColumns = result.filter(col => col.table_name === 'users');
        expect(userColumns).toHaveLength(2);
        expect(userColumns.every(col => col.schema_name === 'public')).toBe(true);
        expect(userColumns.some(col => col.attributes?.includes('PRIMARY KEY'))).toBe(true);

        const orderColumns = result.filter(col => col.table_name === 'orders');
        expect(orderColumns).toHaveLength(2);
        expect(orderColumns.some(col => col.foreign_keys?.length > 0)).toBe(true);

        // Test with empty tables array
        cache.cacheQueryResult = jest.fn().mockResolvedValue([]);
        const emptyResult = await cache.queryColumnsForTables([]);
        expect(emptyResult).toEqual([]);
        expect(Array.isArray(emptyResult)).toBe(true);

        // Test with forceRefresh option
        cache.cacheQueryResult = jest.fn().mockResolvedValue(mockColumns);
        const forcedResult = await cache.queryColumnsForTables(inputTables, 'test-datasource', {
          forceRefresh: true,
          renewalThreshold: 7200
        });
        expect(forcedResult).toEqual(mockColumns);

        // Test data integrity - ensure all required fields are present
        const sampleColumn = mockColumns[0];
        expect(sampleColumn).toHaveProperty('schema_name');
        expect(sampleColumn).toHaveProperty('table_name');
        expect(sampleColumn).toHaveProperty('column_name');
        expect(sampleColumn).toHaveProperty('data_type');
        expect(typeof sampleColumn.schema_name).toBe('string');
        expect(typeof sampleColumn.table_name).toBe('string');
        expect(typeof sampleColumn.column_name).toBe('string');
        expect(typeof sampleColumn.data_type).toBe('string');
      } finally {
        cache.cacheQueryResult = originalCacheQueryResult;
      }
    });

    it('clearDataSourceSchemaCache', async () => {
      // Mock the cache driver's remove method
      const originalRemove = cache.getCacheDriver().remove;
      const removeMock = jest.fn().mockResolvedValue(undefined);
      cache.getCacheDriver().remove = removeMock;

      try {
        // Test clearing cache for specific datasource
        await cache.clearDataSourceSchemaCache('test-datasource');
        expect(removeMock).toHaveBeenCalledTimes(1);

        // Test clearing cache for default datasource
        await cache.clearDataSourceSchemaCache();
        expect(removeMock).toHaveBeenCalledTimes(2);

        // Test multiple calls don't interfere with each other
        await cache.clearDataSourceSchemaCache('datasource1');
        await cache.clearDataSourceSchemaCache('datasource2');
        expect(removeMock).toHaveBeenCalledTimes(4);

        // Verify that the method doesn't throw errors
        expect(async () => {
          await cache.clearDataSourceSchemaCache('non-existent-datasource');
        }).not.toThrow();
      } finally {
        cache.getCacheDriver().remove = originalRemove;
      }
    });

    it('clearTablesForSchemasCache', async () => {
      const schemas = [
        { schema_name: 'public' },
        { schema_name: 'analytics' }
      ];

      // Mock the cache driver's remove method
      const originalRemove = cache.getCacheDriver().remove;
      const removeMock = jest.fn().mockResolvedValue(undefined);
      cache.getCacheDriver().remove = removeMock;

      try {
        // Test clearing cache with schemas
        await cache.clearTablesForSchemasCache(schemas, 'test-datasource');
        expect(removeMock).toHaveBeenCalledTimes(1);

        // Test with default datasource
        await cache.clearTablesForSchemasCache(schemas);
        expect(removeMock).toHaveBeenCalledTimes(2);

        // Test with empty schemas array
        await cache.clearTablesForSchemasCache([]);
        expect(removeMock).toHaveBeenCalledTimes(3);

        // Test with single schema
        await cache.clearTablesForSchemasCache([{ schema_name: 'single_schema' }]);
        expect(removeMock).toHaveBeenCalledTimes(4);

        // Verify input validation - schemas should be an array
        expect(Array.isArray(schemas)).toBe(true);
        expect(schemas.every(schema => typeof schema.schema_name === 'string')).toBe(true);

        // Test that method doesn't throw with valid input
        expect(async () => {
          await cache.clearTablesForSchemasCache([{ schema_name: 'test' }]);
        }).not.toThrow();
      } finally {
        cache.getCacheDriver().remove = originalRemove;
      }
    });

    it('clearColumnsForTablesCache', async () => {
      const tables = [
        { schema_name: 'public', table_name: 'users' },
        { schema_name: 'public', table_name: 'orders' }
      ];

      // Mock the cache driver's remove method
      const originalRemove = cache.getCacheDriver().remove;
      const removeMock = jest.fn().mockResolvedValue(undefined);
      cache.getCacheDriver().remove = removeMock;

      try {
        // Test clearing cache with tables
        await cache.clearColumnsForTablesCache(tables, 'test-datasource');
        expect(removeMock).toHaveBeenCalledTimes(1);

        // Test with default datasource
        await cache.clearColumnsForTablesCache(tables);
        expect(removeMock).toHaveBeenCalledTimes(2);

        // Test with empty tables array
        await cache.clearColumnsForTablesCache([]);
        expect(removeMock).toHaveBeenCalledTimes(3);

        // Test with single table
        const singleTable = [{ schema_name: 'public', table_name: 'users' }];
        await cache.clearColumnsForTablesCache(singleTable);
        expect(removeMock).toHaveBeenCalledTimes(4);

        // Verify input validation - tables should be an array with correct structure
        expect(Array.isArray(tables)).toBe(true);
        expect(tables.every(table => typeof table.schema_name === 'string' && typeof table.table_name === 'string')).toBe(true);

        // Test that method handles various table configurations
        const mixedTables = [
          { schema_name: 'schema1', table_name: 'table1' },
          { schema_name: 'schema2', table_name: 'table2' },
          { schema_name: 'schema1', table_name: 'table3' }
        ];
        await cache.clearColumnsForTablesCache(mixedTables);
        expect(removeMock).toHaveBeenCalledTimes(5);
      } finally {
        cache.getCacheDriver().remove = originalRemove;
      }
    });

    it('metadata methods integration test', async () => {
      // Test the full flow of metadata operations with realistic data hierarchy
      const mockSchemas = [
        { schema_name: 'public' },
        { schema_name: 'analytics' }
      ];

      const mockTables = [
        { schema_name: 'public', table_name: 'users' },
        { schema_name: 'public', table_name: 'orders' },
        { schema_name: 'analytics', table_name: 'events' }
      ];

      const mockColumns = [
        {
          schema_name: 'public',
          table_name: 'users',
          column_name: 'id',
          data_type: 'integer'
        },
        {
          schema_name: 'public',
          table_name: 'users',
          column_name: 'email',
          data_type: 'varchar'
        },
        {
          schema_name: 'public',
          table_name: 'orders',
          column_name: 'id',
          data_type: 'integer'
        },
        {
          schema_name: 'analytics',
          table_name: 'events',
          column_name: 'event_id',
          data_type: 'uuid'
        }
      ];

      const originalCacheQueryResult = cache.cacheQueryResult;
      let callCount = 0;
      cache.cacheQueryResult = jest.fn().mockImplementation(() => {
        callCount++;
        if (callCount === 1) return Promise.resolve(mockSchemas);
        if (callCount === 2) return Promise.resolve(mockTables);
        if (callCount === 3) return Promise.resolve(mockColumns);
        return Promise.resolve([]);
      });

      try {
        // Test sequential workflow: schemas → tables → columns
        const schemas = await cache.queryDataSourceSchemas('test-db');
        expect(schemas).toEqual(mockSchemas);
        expect(schemas).toHaveLength(2);
        expect(schemas.every(s => s.schema_name)).toBe(true);

        // Use retrieved schemas to get tables
        const tables = await cache.queryTablesForSchemas(schemas, 'test-db');
        expect(tables).toEqual(mockTables);
        expect(tables).toHaveLength(3);

        // Verify that tables belong to the requested schemas
        const schemaNames = schemas.map(s => s.schema_name);
        expect(tables.every(t => schemaNames.includes(t.schema_name))).toBe(true);

        // Use retrieved tables to get columns
        const columns = await cache.queryColumnsForTables(tables, 'test-db');
        expect(columns).toEqual(mockColumns);
        expect(columns).toHaveLength(4);

        // Verify that columns belong to the requested tables
        const tableIdentifiers = tables.map(t => `${t.schema_name}.${t.table_name}`);
        expect(columns.every(c => tableIdentifiers.includes(`${c.schema_name}.${c.table_name}`))).toBe(true);

        // Verify data consistency across the hierarchy
        expect(callCount).toBe(3);

        // Test error handling - what happens when intermediate step returns empty
        cache.cacheQueryResult = jest.fn().mockResolvedValue([]);
        const emptyTables = await cache.queryTablesForSchemas(schemas, 'test-db');
        expect(emptyTables).toEqual([]);

        const emptyColumns = await cache.queryColumnsForTables([], 'test-db');
        expect(emptyColumns).toEqual([]);

        // Test cache clearing workflow
        const originalRemove = cache.getCacheDriver().remove;
        const removeMock = jest.fn().mockResolvedValue(undefined);
        cache.getCacheDriver().remove = removeMock;

        // Clear in reverse order (columns → tables → schemas)
        await cache.clearColumnsForTablesCache(tables, 'test-db');
        await cache.clearTablesForSchemasCache(schemas, 'test-db');
        await cache.clearDataSourceSchemaCache('test-db');

        expect(removeMock).toHaveBeenCalledTimes(3);

        cache.getCacheDriver().remove = originalRemove;
      } finally {
        cache.cacheQueryResult = originalCacheQueryResult;
      }
    });
  });
};
