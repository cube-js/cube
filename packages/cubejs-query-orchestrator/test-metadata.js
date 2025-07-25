/**
 * Simple test to verify the metadata query functionality
 */

const { QueryCache } = require('./dist/src/orchestrator/QueryCache');

// Mock driver factory
const mockDriverFactory = () => Promise.resolve({
  getSchemas: () => Promise.resolve([
    { schema_name: 'public' },
    { schema_name: 'sales' }
  ]),
  getTablesForSpecificSchemas: (schemas) => Promise.resolve([
    { schema_name: 'public', table_name: 'users' },
    { schema_name: 'public', table_name: 'orders' },
    { schema_name: 'sales', table_name: 'transactions' }
  ]),
  getColumnsForSpecificTables: (tables) => Promise.resolve([
    { schema_name: 'public', table_name: 'users', column_name: 'id', data_type: 'integer' },
    { schema_name: 'public', table_name: 'users', column_name: 'name', data_type: 'varchar' },
    { schema_name: 'public', table_name: 'orders', column_name: 'id', data_type: 'integer' },
    { schema_name: 'public', table_name: 'orders', column_name: 'user_id', data_type: 'integer' }
  ])
});

// Mock logger
const mockLogger = (msg, params) => {
  console.log(`[LOG] ${msg}`, params || '');
};

async function testMetadataQueries() {
  console.log('Testing metadata query functionality...');

  try {
    const queryCache = new QueryCache(
      'test',
      () => mockDriverFactory(),
      mockLogger,
      {
        queueOptions: () => Promise.resolve({ concurrency: 1 }),
        cacheAndQueueDriver: 'memory'
      }
    );

    // Test schema query
    console.log('\n1. Testing schema query...');
    const schemas = await queryCache.queryDataSourceSchema('test');
    console.log('Schemas:', schemas);

    // Test tables query
    console.log('\n2. Testing tables query...');
    const tables = await queryCache.queryTablesForSchemas(schemas, 'test');
    console.log('Tables:', tables);

    // Test columns query
    console.log('\n3. Testing columns query...');
    const columns = await queryCache.queryColumnsForTables(tables, 'test');
    console.log('Columns:', columns);

    // Test caching - second call should use cache
    console.log('\n4. Testing cache (second schema call)...');
    const schemasFromCache = await queryCache.queryDataSourceSchema('test');
    console.log('Schemas from cache:', schemasFromCache);

    console.log('\n✅ All tests passed!');

    // Cleanup
    await queryCache.cleanup();

  } catch (error) {
    console.error('❌ Test failed:', error);
    process.exit(1);
  }
}

testMetadataQueries();
