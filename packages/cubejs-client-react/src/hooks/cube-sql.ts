import type { Query, SqlQuery } from '@cubejs-client/core';

import { useCubeFetch } from './cube-fetch';
import type { UseCubeFetchOptions, UseCubeFetchResult } from '../types';

export function useCubeSql(
  query: Query | Query[],
  options: UseCubeFetchOptions = {}
): UseCubeFetchResult<SqlQuery> {
  return useCubeFetch<SqlQuery>('sql', {
    ...options,
    query
  });
}
