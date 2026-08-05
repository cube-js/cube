/**
 * The key a data source's driver is filed under, given whether the request wants
 * pre-aggregation credentials.
 *
 * Both callers pass what *they* key on, and they key on different things. The
 * factory cache passes `usePreAggregationCredentials` — the decision that picks
 * the credentials — because a key of its own is earned only by a request that
 * resolves to a genuinely different connection: caching a shared driver twice
 * builds a second pool for it, and releasing it twice closes it twice. The
 * factory's caller supplies that predicate rather than this deriving it, so the
 * key can never disagree with which connection the driver was built for.
 * `OrchestratorApi`'s tracking passes the raw request instead and may therefore
 * hold two keys for one shared driver, which is harmless: it dedups on the
 * resolved instance.
 */
export function driverCacheKey(dataSource: string, usePreAggregationCredentials: boolean): string {
  return usePreAggregationCredentials ? `${dataSource}@pre_agg` : dataSource;
}
