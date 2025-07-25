# QueryCache DataSource Metadata Methods

This document describes the datasource metadata query methods added to the `QueryCache` class, which provide cached and queued access to datasource schema, table, and column information.

## Overview

The QueryCache class now includes three main methods for querying datasource metadata:

1. **`queryDataSourceSchema`** - Query available schemas in a datasource
2. **`queryTablesForSchemas`** - Query tables for specific schemas
3. **`queryColumnsForTables`** - Query columns for specific tables

All methods utilize a shared caching and queue infrastructure with automatic cache management, smart refresh, and comprehensive error handling.

## Features

- **Automatic Caching**: Results cached in CubeStore (or memory) with configurable expiration
- **Queue Management**: Uses existing queue system to prevent concurrent identical requests
- **Smart Refresh**: Automatically refreshes expired cache entries
- **Force Refresh**: Option to bypass cache and fetch fresh data
- **Fallback Support**: Returns cached data if refresh fails
- **Cache Management**: Methods to manually clear specific caches
- **DRY Implementation**: Shared generic caching logic to avoid code duplication

## Method Signatures

### queryDataSourceSchema

```typescript
public async queryDataSourceSchema(
  dataSource: string = 'default',
  options: {
    requestId?: string;
    forceRefresh?: boolean;
    renewalThreshold?: number;
    expiration?: number;
  } = {}
): Promise<QuerySchemasResult[]>
```

### queryTablesForSchemas

```typescript
public async queryTablesForSchemas(
  schemas: QuerySchemasResult[],
  dataSource: string = 'default',
  options: {
    requestId?: string;
    forceRefresh?: boolean;
    renewalThreshold?: number;
    expiration?: number;
  } = {}
): Promise<QueryTablesResult[]>
```

### queryColumnsForTables

```typescript
public async queryColumnsForTables(
  tables: QueryTablesResult[],
  dataSource: string = 'default',
  options: {
    requestId?: string;
    forceRefresh?: boolean;
    renewalThreshold?: number;
    expiration?: number;
  } = {}
): Promise<QueryColumnsResult[]>
```

## Parameters

- **dataSource** (string, optional): The datasource name to query. Defaults to 'default'.
- **schemas** (QuerySchemasResult[]): Array of schema objects for table queries
- **tables** (QueryTablesResult[]): Array of table objects for column queries
- **options** (object, optional): Configuration options:
  - **requestId** (string, optional): Request ID for logging and tracking
  - **forceRefresh** (boolean, optional): If true, bypasses cache and fetches fresh data. Defaults to false.
  - **renewalThreshold** (number, optional): Cache refresh threshold in seconds. Defaults to 24 hours (86400).
  - **expiration** (number, optional): Cache expiration time in seconds. Defaults to 7 days (604800).

## Return Values

### QuerySchemasResult[]
```typescript
[
  { schema_name: 'public' },
  { schema_name: 'analytics' },
  { schema_name: 'reporting' }
]
```

### QueryTablesResult[]
```typescript
[
  { schema_name: 'public', table_name: 'users' },
  { schema_name: 'public', table_name: 'orders' },
  { schema_name: 'analytics', table_name: 'events' }
]
```

### QueryColumnsResult[]
```typescript
[
  {
    schema_name: 'public',
    table_name: 'users',
    column_name: 'id',
    data_type: 'integer',
    attributes?: string[],
    foreign_keys?: ForeignKey[]
  },
  {
    schema_name: 'public',
    table_name: 'users',
    column_name: 'name',
    data_type: 'text'
  }
]
```

## Usage Examples

### Basic Schema Query

```typescript
const queryCache = new QueryCache(/* ... */);

// Get schemas for default datasource
const schemas = await queryCache.queryDataSourceSchema();
console.log('Available schemas:', schemas);
```

### Full Metadata Discovery Chain

```typescript
// Get all schemas
const schemas = await queryCache.queryDataSourceSchema('default', {
  requestId: 'metadata-discovery'
});

// Get all tables for those schemas
const tables = await queryCache.queryTablesForSchemas(schemas, 'default', {
  requestId: 'metadata-discovery'
});

// Get all columns for those tables
const columns = await queryCache.queryColumnsForTables(tables, 'default', {
  requestId: 'metadata-discovery'
});

console.log(`Found ${schemas.length} schemas, ${tables.length} tables, ${columns.length} columns`);
```

### Selective Metadata Queries

```typescript
// Get specific schemas only
const publicSchema = [{ schema_name: 'public' }];
const publicTables = await queryCache.queryTablesForSchemas(publicSchema, 'default');

// Get columns for specific tables only
const userTable = [{ schema_name: 'public', table_name: 'users' }];
const userColumns = await queryCache.queryColumnsForTables(userTable, 'default');
```

### Force Refresh Examples

```typescript
// Force refresh schemas
const freshSchemas = await queryCache.queryDataSourceSchema('analytics', {
  forceRefresh: true,
  requestId: 'force-refresh-schemas'
});

// Force refresh tables with custom cache settings
const freshTables = await queryCache.queryTablesForSchemas(freshSchemas, 'analytics', {
  forceRefresh: true,
  renewalThreshold: 60 * 60, // 1 hour
  expiration: 2 * 24 * 60 * 60, // 2 days
  requestId: 'force-refresh-tables'
});
```

### Cache Management

```typescript
// Clear individual caches
await queryCache.clearDataSourceSchemaCache('default');

const schemas = [{ schema_name: 'public' }, { schema_name: 'analytics' }];
await queryCache.clearTablesForSchemasCache(schemas, 'default');

const tables = [
  { schema_name: 'public', table_name: 'users' },
  { schema_name: 'public', table_name: 'orders' }
];
await queryCache.clearColumnsForTablesCache(tables, 'default');
```

## Cache Behavior

1. **First Call**: Fetches data from datasource and caches it
2. **Subsequent Calls**: Returns cached data if within renewal threshold
3. **Expired Cache**: Automatically fetches fresh data and updates cache
4. **Failed Refresh**: Returns cached data as fallback if refresh fails
5. **Force Refresh**: Always fetches fresh data and updates cache

## Implementation Details

### Queue Handlers

The methods add three new handlers to the QueryQueue:

```typescript
getSchemas: async (req) => {
  const client = await clientFactory();
  return client.getSchemas();
}

getTablesForSchemas: async (req) => {
  const client = await clientFactory();
  return client.getTablesForSpecificSchemas(req.schemas);
}

getColumnsForTables: async (req) => {
  const client = await clientFactory();
  return client.getColumnsForSpecificTables(req.tables);
}
```

### Cache Key Structure

- **Schemas**: `['DATASOURCE_SCHEMA', dataSource]`
- **Tables**: `['DATASOURCE_TABLES', '${dataSource}:${schemaNames}']`
- **Columns**: `['DATASOURCE_COLUMNS', '${dataSource}:${tableNames}']`

### DRY Implementation

All methods use a shared generic `queryDataSourceMetadata<T>()` method that handles:
- Cache key generation
- Queue execution
- Cache retrieval and updating
- Error handling and fallbacks
- Logging

This eliminates code duplication and ensures consistent behavior across all methods.

### Logging

Comprehensive logging includes:
- Cache hits and misses
- Fresh data fetching
- Cache updates
- Error conditions
- Metadata about schemas/tables/columns being queried

## Error Handling

- Network errors during refresh fall back to cached data if available
- Missing cache triggers fresh data fetch
- All errors are properly logged with context
- TypeScript types ensure type safety throughout

## Performance Considerations

- Default 24-hour cache renewal threshold balances freshness with performance
- Queue system prevents duplicate concurrent requests
- Cache keys include sorted parameter lists for consistency
- CubeStore provides persistent, scalable caching
- Intelligent cache key construction prevents cache pollution

## Testing

A comprehensive test suite is provided in `test/datasource-schema.test.ts` that validates:
- All three metadata query methods
- Cache miss and hit behavior
- Force refresh functionality
- Cache clearing for all types
- Full metadata discovery chains
- Error handling scenarios

Run the test with:
```bash
ts-node test/datasource-schema.test.ts
```

## Integration

These methods integrate seamlessly with the existing QueryCache infrastructure:
- Use the same queue system as other operations
- Follow the same caching patterns
- Compatible with all supported cache drivers (memory, CubeStore)
- Consistent logging and error handling
- Proper TypeScript typing throughout
