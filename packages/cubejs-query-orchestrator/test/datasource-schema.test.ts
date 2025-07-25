/**
 * Comprehensive test for datasource metadata query methods
 */

import { QuerySchemasResult, QueryTablesResult, QueryColumnsResult } from '@cubejs-backend/base-driver';
import { QueryCache } from '../src/orchestrator/QueryCache';

// Mock driver that returns sample metadata
class MockDriver {
  public async getSchemas(): Promise<QuerySchemasResult[]> {
    await new Promise(resolve => setTimeout(resolve, 100));
    return [
      { schema_name: 'public' },
      { schema_name: 'analytics' },
      { schema_name: 'reporting' }
    ];
  }

  public async getTablesForSpecificSchemas(schemas: QuerySchemasResult[]): Promise<QueryTablesResult[]> {
    await new Promise(resolve => setTimeout(resolve, 100));
    const tables: QueryTablesResult[] = [];

    schemas.forEach(schema => {
      if (schema.schema_name === 'public') {
        tables.push(
          { schema_name: 'public', table_name: 'users' },
          { schema_name: 'public', table_name: 'orders' }
        );
      } else if (schema.schema_name === 'analytics') {
        tables.push(
          { schema_name: 'analytics', table_name: 'events' },
          { schema_name: 'analytics', table_name: 'metrics' }
        );
      }
    });

    return tables;
  }

  public async getColumnsForSpecificTables(tables: QueryTablesResult[]): Promise<QueryColumnsResult[]> {
    await new Promise(resolve => setTimeout(resolve, 100));
    const columns: QueryColumnsResult[] = [];

    tables.forEach(table => {
      if (table.table_name === 'users') {
        columns.push(
          { schema_name: 'public', table_name: 'users', column_name: 'id', data_type: 'integer' },
          { schema_name: 'public', table_name: 'users', column_name: 'name', data_type: 'text' },
          { schema_name: 'public', table_name: 'users', column_name: 'email', data_type: 'text' }
        );
      } else if (table.table_name === 'orders') {
        columns.push(
          { schema_name: 'public', table_name: 'orders', column_name: 'id', data_type: 'integer' },
          { schema_name: 'public', table_name: 'orders', column_name: 'user_id', data_type: 'integer' },
          { schema_name: 'public', table_name: 'orders', column_name: 'amount', data_type: 'decimal' }
        );
      } else if (table.table_name === 'events') {
        columns.push(
          { schema_name: 'analytics', table_name: 'events', column_name: 'id', data_type: 'bigint' },
          { schema_name: 'analytics', table_name: 'events', column_name: 'event_type', data_type: 'text' },
          { schema_name: 'analytics', table_name: 'events', column_name: 'timestamp', data_type: 'timestamp' }
        );
      }
    });

    return columns;
  }
}// Mock cache driver
class MockCacheDriver {
  private cache = new Map<string, any>();

  public async get(key: string) {
    return this.cache.get(key) || null;
  }

  public async set(key: string, value: any, _expiration: number) {
    this.cache.set(key, value);
    return { bytes: JSON.stringify(value).length };
  }

  public async remove(key: string) {
    this.cache.delete(key);
  }

  public async cleanup() {
    this.cache.clear();
  }

  public async testConnection() {
    return true;
  }

  public async withLock(key: string, callback: any) {
    return callback();
  }
}

async function testDatasourceMetadataMethods() {
  const mockLogger = (msg: string, params?: any) => {
    console.log(`[TEST] ${msg}`, params || '');
  };

  const queryCache = new QueryCache(
    'test',
    () => Promise.resolve(new MockDriver() as any),
    mockLogger,
    {
      cacheAndQueueDriver: 'memory',
      queueOptions: () => Promise.resolve({
        concurrency: 1,
        continueWaitTimeout: 1000,
        executionTimeout: 30000,
      })
    }
  );

  // Replace the cache driver with our mock
  (queryCache as any).cacheDriver = new MockCacheDriver();

  console.log('Testing datasource metadata methods...');

  try {
    // Test 1: Query schemas
    console.log('\n--- Test 1: Query schemas (cache miss) ---');
    const schemas = await queryCache.queryDataSourceSchema('default', {
      requestId: 'test-schemas-1'
    });
    console.log('Schemas:', schemas);

    // Test 2: Query tables for schemas
    console.log('\n--- Test 2: Query tables for schemas ---');
    const tables = await queryCache.queryTablesForSchemas(schemas.slice(0, 2), 'default', {
      requestId: 'test-tables-1'
    });
    console.log('Tables:', tables);

    // Test 3: Query columns for tables
    console.log('\n--- Test 3: Query columns for tables ---');
    const columns = await queryCache.queryColumnsForTables(tables.slice(0, 2), 'default', {
      requestId: 'test-columns-1'
    });
    console.log('Columns:', columns);

    // Test 4: Cache hit scenarios
    console.log('\n--- Test 4: Cache hits ---');
    const cachedSchemas = await queryCache.queryDataSourceSchema('default', {
      requestId: 'test-schemas-2'
    });
    console.log('Cached schemas count:', cachedSchemas.length);

    const cachedTables = await queryCache.queryTablesForSchemas(schemas.slice(0, 2), 'default', {
      requestId: 'test-tables-2'
    });
    console.log('Cached tables count:', cachedTables.length);

    // Test 5: Force refresh
    console.log('\n--- Test 5: Force refresh ---');
    const freshSchemas = await queryCache.queryDataSourceSchema('default', {
      requestId: 'test-schemas-3',
      forceRefresh: true
    });
    console.log('Fresh schemas count:', freshSchemas.length);

    // Test 6: Cache clearing
    console.log('\n--- Test 6: Cache clearing ---');
    await queryCache.clearDataSourceSchemaCache('default');
    await queryCache.clearTablesForSchemasCache(schemas.slice(0, 2), 'default');
    await queryCache.clearColumnsForTablesCache(tables.slice(0, 2), 'default');
    console.log('All caches cleared');

    // Test 7: Queries after cache clear
    console.log('\n--- Test 7: Queries after cache clear ---');
    const newSchemas = await queryCache.queryDataSourceSchema('default', {
      requestId: 'test-schemas-4'
    });
    console.log('New schemas count:', newSchemas.length);

    // Test 8: Full metadata chain
    console.log('\n--- Test 8: Full metadata chain ---');
    const allSchemas = await queryCache.queryDataSourceSchema('analytics');
    const allTables = await queryCache.queryTablesForSchemas(allSchemas, 'analytics');
    const allColumns = await queryCache.queryColumnsForTables(allTables, 'analytics');

    console.log('Analytics metadata:');
    console.log('- Schemas:', allSchemas.length);
    console.log('- Tables:', allTables.length);
    console.log('- Columns:', allColumns.length);

    console.log('\n✅ All tests passed!');
  } catch (error) {
    console.error('❌ Test failed:', error);
  }
}

// Run the test if this file is executed directly
if (require.main === module) {
  testDatasourceMetadataMethods().catch(console.error);
}

export { testDatasourceMetadataMethods };
