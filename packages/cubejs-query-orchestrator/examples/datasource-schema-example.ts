/**
 * Example usage of the datasource metadata query methods
 */

import { QuerySchemasResult, QueryTablesResult, QueryColumnsResult } from '@cubejs-backend/base-driver';
import { QueryCache } from '../src/orchestrator/QueryCache';

// Example usage of the new datasource metadata query methods
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
    // 1. Query datasource schemas with default options (24h cache)
    const schemas: QuerySchemasResult[] = await queryCache.queryDataSourceSchemas('default', {
      requestId: 'example-request-1'
    });

    console.log('Available schemas:', schemas);
    // Output: [{ schema_name: 'public' }, { schema_name: 'analytics' }]

    // 2. Query tables for specific schemas
    const tables: QueryTablesResult[] = await queryCache.queryTablesForSchemas(schemas, 'default', {
      requestId: 'example-request-2'
    });

    console.log('Tables in schemas:', tables);
    // Output: [
    //   { schema_name: 'public', table_name: 'users' },
    //   { schema_name: 'public', table_name: 'orders' },
    //   { schema_name: 'analytics', table_name: 'events' }
    // ]

    // 3. Query columns for specific tables
    const columns: QueryColumnsResult[] = await queryCache.queryColumnsForTables(tables, 'default', {
      requestId: 'example-request-3'
    });

    console.log('Columns in tables:', columns);
    // Output: [
    //   { schema_name: 'public', table_name: 'users', column_name: 'id', data_type: 'integer' },
    //   { schema_name: 'public', table_name: 'users', column_name: 'name', data_type: 'text' },
    //   // ... more columns
    // ]

    // 4. Force refresh examples
    const freshSchemas = await queryCache.queryDataSourceSchemas('default', {
      requestId: 'example-request-4',
      forceRefresh: true
    });

    const _freshTables = await queryCache.queryTablesForSchemas(freshSchemas, 'default', {
      requestId: 'example-request-5',
      forceRefresh: true,
      renewalThreshold: 60 * 60, // 1 hour
      expiration: 2 * 24 * 60 * 60 // 2 days
    });

    // 5. Clear cache examples
    await queryCache.clearDataSourceSchemaCache('default');
    console.log('Schema cache cleared');

    await queryCache.clearTablesForSchemasCache(schemas, 'default');
    console.log('Tables cache cleared');

    await queryCache.clearColumnsForTablesCache(tables, 'default');
    console.log('Columns cache cleared');

    // 6. Chain queries for complete metadata discovery
    const allSchemas = await queryCache.queryDataSourceSchemas('analytics');
    const allTables = await queryCache.queryTablesForSchemas(allSchemas, 'analytics');
    const allColumns = await queryCache.queryColumnsForTables(allTables, 'analytics');

    console.log('Complete metadata for analytics datasource:');
    console.log('Schemas:', allSchemas.length);
    console.log('Tables:', allTables.length);
    console.log('Columns:', allColumns.length);
  } catch (error) {
    console.error('Error querying datasource metadata:', error);
  }
}

// Example helper function to build a complete metadata tree
async function buildCompleteMetadataTree(
  queryCache: QueryCache,
  dataSource: string = 'default'
) {
  const schemas = await queryCache.queryDataSourceSchemas(dataSource);
  const metadataTree: any[] = [];

  for (const schema of schemas) {
    const tables = await queryCache.queryTablesForSchemas([schema], dataSource);
    const schemaNode: any = {
      schema_name: schema.schema_name,
      tables: []
    };

    for (const table of tables) {
      const columns = await queryCache.queryColumnsForTables([table], dataSource);
      schemaNode.tables.push({
        table_name: table.table_name,
        columns: columns.map(col => ({
          column_name: col.column_name,
          data_type: col.data_type,
          attributes: col.attributes,
          foreign_keys: col.foreign_keys
        }))
      });
    }

    metadataTree.push(schemaNode);
  }

  return metadataTree;
}

export { exampleUsage, buildCompleteMetadataTree };
