# QueryCache DataSource Schema Method

This document describes the new `queryDataSourceSchema` method added to the `QueryCache` class, which provides cached and queued access to datasource schema information.

## Overview

The `queryDataSourceSchema` method allows you to query the available schemas in a datasource with automatic caching and queue management. This is particularly useful for applications that need to frequently access schema information without overwhelming the database with repeated queries.

## Features

- **Automatic Caching**: Results are cached in CubeStore (or memory) with configurable expiration
- **Queue Management**: Uses the existing queue system to prevent concurrent identical requests
- **Smart Refresh**: Automatically refreshes expired cache entries
- **Force Refresh**: Option to bypass cache and fetch fresh data
- **Fallback Support**: Returns cached data if refresh fails
- **Cache Management**: Includes method to manually clear cache

## Method Signature

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

## Parameters

- **dataSource** (string, optional): The datasource name to query. Defaults to 'default'.
- **options** (object, optional): Configuration options:
  - **requestId** (string, optional): Request ID for logging and tracking
  - **forceRefresh** (boolean, optional): If true, bypasses cache and fetches fresh data. Defaults to false.
  - **renewalThreshold** (number, optional): Cache refresh threshold in seconds. Defaults to 24 hours (86400).
  - **expiration** (number, optional): Cache expiration time in seconds. Defaults to 7 days (604800).

## Return Value

Returns a Promise that resolves to an array of `QuerySchemasResult` objects:

```typescript
[
  { schema_name: 'public' },
  { schema_name: 'analytics' },
  { schema_name: 'reporting' }
]
```

## Usage Examples

### Basic Usage

```typescript
const queryCache = new QueryCache(/* ... */);

// Get schemas for default datasource
const schemas = await queryCache.queryDataSourceSchema();
console.log('Available schemas:', schemas);
```

### With Request ID

```typescript
const schemas = await queryCache.queryDataSourceSchema('default', {
  requestId: 'my-request-123'
});
```

### Force Refresh Cache

```typescript
const freshSchemas = await queryCache.queryDataSourceSchema('analytics', {
  forceRefresh: true,
  requestId: 'force-refresh-request'
});
```

### Custom Cache Settings

```typescript
const schemas = await queryCache.queryDataSourceSchema('reporting', {
  renewalThreshold: 60 * 60, // 1 hour
  expiration: 3 * 24 * 60 * 60, // 3 days
  requestId: 'custom-cache-request'
});
```

### Clear Cache

```typescript
// Clear cache for specific datasource
await queryCache.clearDataSourceSchemaCache('default');
```

## Cache Behavior

1. **First Call**: Fetches data from datasource and caches it
2. **Subsequent Calls**: Returns cached data if within renewal threshold
3. **Expired Cache**: Automatically fetches fresh data and updates cache
4. **Failed Refresh**: Returns cached data as fallback if refresh fails
5. **Force Refresh**: Always fetches fresh data and updates cache

## Implementation Details

### Queue Handler

The method adds a new `getSchemas` handler to the QueryQueue:

```typescript
getSchemas: async (req) => {
  const client = await clientFactory();
  return client.getSchemas();
}
```

### Cache Key Structure

Cache keys use the format: `['DATASOURCE_SCHEMA', dataSource]`

### Logging

The method provides comprehensive logging for:
- Cache hits and misses
- Fresh data fetching
- Cache updates
- Error conditions

## Error Handling

- Network errors during refresh fall back to cached data if available
- Missing cache triggers fresh data fetch
- All errors are properly logged with context
- TypeScript types ensure type safety

## Performance Considerations

- Default 24-hour cache renewal threshold balances freshness with performance
- Queue system prevents duplicate concurrent requests
- In-memory caching disabled for schema queries to prevent memory issues
- CubeStore provides persistent, scalable caching

## Testing

A test suite is provided in `test/datasource-schema.test.ts` that validates:
- Cache miss behavior
- Cache hit behavior
- Force refresh functionality
- Cache clearing
- Error handling

Run the test with:
```bash
ts-node test/datasource-schema.test.ts
```

## Integration

This method integrates seamlessly with the existing QueryCache infrastructure:
- Uses the same queue system as other operations
- Follows the same caching patterns
- Compatible with all supported cache drivers (memory, CubeStore)
- Consistent logging and error handling
