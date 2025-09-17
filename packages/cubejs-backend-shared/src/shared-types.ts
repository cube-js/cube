/*
stale-if-slow (default) — equivalent to previously used renewQuery: false
  If refresh keys are up-to-date, returns the value from cache
  If refresh keys are expired, tries to return the value from the database
    Returns fresh value from the database if the query executed in the database until the first “Continue wait” interval is reached
    Returns stale value from cache otherwise

stale-while-revalidate — AKA “backgroundRefresh”
  If refresh keys are up-to-date, returns the value from cache
  If refresh keys are expired, returns stale data from cache
  Updates the cache in background

must-revalidate — equivalent to previously used renewQuery: true
  If refresh keys are up-to-date, returns the value from cache
  If refresh keys are expired, tries to return the value from the database
    Returns fresh value from the database even if it takes minutes and many “Continue wait” intervals

no-cache — AKA “forceRefresh”
  Skips refresh key checks
  Returns fresh data from the database, even if it takes minutes and many “Continue wait” intervals
*/
export type CacheMode = 'stale-if-slow' | 'stale-while-revalidate' | 'must-revalidate' | 'no-cache';
