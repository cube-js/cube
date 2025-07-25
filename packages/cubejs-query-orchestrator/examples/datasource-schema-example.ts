/**
 * Example usage of the queryDataSourceSchema method
 */

import { QueryCache } from '../src/orchestrator/QueryCache';

// Example usage of the new queryDataSourceSchema method
async function exampleUsage() {
  // Assuming you have a QueryCache instance
  const queryCache = new QueryCache(
    'example',
    (_dataSource) => {
      // Your driver factory implementation
      throw new Error('Driver factory not implemented in example');
    },
    (msg, params) => console.log(msg, params), // logger
    {
      cacheAndQueueDriver: 'cubestore', // Use CubeStore for caching
      cubeStoreDriverFactory: async () => {
        // Your CubeStore driver factory implementation
        throw new Error('CubeStore driver factory not implemented in example');
      }
    }
  );

  try {
    // Query datasource schema with default options (24h cache)
    const schemas = await queryCache.queryDataSourceSchema('default', {
      requestId: 'example-request-1'
    });

    console.log('Available schemas:', schemas);

    // Force refresh the schema cache
    const freshSchemas = await queryCache.queryDataSourceSchema('default', {
      requestId: 'example-request-2',
      forceRefresh: true
    });

    console.log('Fresh schemas:', freshSchemas);

    // Query with custom cache settings (1 hour cache, 3 days expiration)
    const schemasCustomCache = await queryCache.queryDataSourceSchema('analytics', {
      requestId: 'example-request-3',
      renewalThreshold: 60 * 60, // 1 hour
      expiration: 3 * 24 * 60 * 60 // 3 days
    });

    console.log('Analytics schemas:', schemasCustomCache);

    // Clear cache for a specific datasource
    await queryCache.clearDataSourceSchemaCache('default');
    console.log('Cache cleared for default datasource');
  } catch (error) {
    console.error('Error querying datasource schema:', error);
  }
}

// The method returns an array of QuerySchemasResult objects:
// [
//   { schema_name: 'public' },
//   { schema_name: 'analytics' },
//   { schema_name: 'reporting' }
// ]

export { exampleUsage };
