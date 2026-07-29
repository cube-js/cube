import type { DryRunResponse, Query } from '@cubejs-client/core';

import { useCubeFetch } from './cube-fetch';
import type { UseCubeFetchOptions, UseCubeFetchResult } from '../types';

/**
 * @hidden
 */
export function useDryRun(
  query: Query | Query[],
  options: UseCubeFetchOptions = {}
): UseCubeFetchResult<DryRunResponse> {
  return useCubeFetch<DryRunResponse>('dryRun', {
    ...options,
    query,
  });
}
