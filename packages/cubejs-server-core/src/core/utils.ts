/**
 * The key a data source's driver is cached and tracked under.
 *
 * A key of its own is earned only by a pre-aggregation request that resolves to
 * genuinely different credentials — `usePreAggregationCredentials`. Anything
 * else shares the data source's one driver, so it must share the one key:
 * caching a shared driver twice builds a second pool for the same connection,
 * and releasing it twice closes it twice.
 *
 * The caller supplies the predicate rather than this deriving it, so the key can
 * never disagree with the decision that actually picks the credentials.
 */
export function driverCacheKey(dataSource: string, usePreAggregationCredentials: boolean): string {
  return usePreAggregationCredentials ? `${dataSource}@pre_agg` : dataSource;
}
