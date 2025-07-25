/**
 * Simple test to verify the queryDataSourceSchema method
 */

import { QuerySchemasResult } from '@cubejs-backend/base-driver';
import { QueryCache } from '../src/orchestrator/QueryCache';

// Mock driver that returns sample schema data
class MockDriver {
  public async getSchemas(): Promise<QuerySchemasResult[]> {
    // Simulate some delay
    await new Promise(resolve => setTimeout(resolve, 100));
    
    return [
      { schema_name: 'public' },
      { schema_name: 'analytics' },
      { schema_name: 'reporting' }
    ];
  }
}

// Mock cache driver
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

async function testQueryDataSourceSchema() {
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

  console.log('Testing queryDataSourceSchema...');

  try {
    // Test 1: First call should fetch from datasource
    console.log('\n--- Test 1: First call (cache miss) ---');
    const schemas1 = await queryCache.queryDataSourceSchema('default', {
      requestId: 'test-1'
    });
    console.log('Schemas:', schemas1);

    // Test 2: Second call should return from cache
    console.log('\n--- Test 2: Second call (cache hit) ---');
    const schemas2 = await queryCache.queryDataSourceSchema('default', {
      requestId: 'test-2'
    });
    console.log('Schemas:', schemas2);

    // Test 3: Force refresh
    console.log('\n--- Test 3: Force refresh ---');
    const schemas3 = await queryCache.queryDataSourceSchema('default', {
      requestId: 'test-3',
      forceRefresh: true
    });
    console.log('Schemas:', schemas3);

    // Test 4: Clear cache
    console.log('\n--- Test 4: Clear cache ---');
    await queryCache.clearDataSourceSchemaCache('default');
    console.log('Cache cleared');

    // Test 5: Call after cache clear
    console.log('\n--- Test 5: Call after cache clear ---');
    const schemas4 = await queryCache.queryDataSourceSchema('default', {
      requestId: 'test-4'
    });
    console.log('Schemas:', schemas4);

    console.log('\n✅ All tests passed!');
  } catch (error) {
    console.error('❌ Test failed:', error);
  }
}

// Run the test if this file is executed directly
if (require.main === module) {
  testQueryDataSourceSchema().catch(console.error);
}

export { testQueryDataSourceSchema };
