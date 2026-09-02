import type { DryRunResponse, Query, SqlQuery } from '@cubejs-client/core';

import { useCubeFetch } from './cube-fetch';
import type { UseCubeFetchOptions, UseCubeFetchResult } from '../types';

/**
 * The hook resolves with a `SqlQuery`, while its `response` stays declared as a
 * dry-run response for backwards compatibility — `UseCubeSqlResult` spells the
 * real shape. `refetch` is declared, as it is on `useDryRun` and `useCubeMeta`,
 * because all three return the same object.
 *
 * @hidden
 */
export function useCubeSql(
  query: Query | Query[],
  options: UseCubeFetchOptions = {}
): UseCubeFetchResult<DryRunResponse> {
  return useCubeFetch<SqlQuery>('sql', {
    ...options,
    query
  }) as unknown as UseCubeFetchResult<DryRunResponse>;
}
