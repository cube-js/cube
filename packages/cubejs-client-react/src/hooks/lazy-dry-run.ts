import type { DryRunResponse, Query } from '@cubejs-client/core';

import { useCubeFetch } from './cube-fetch';
import type {
  CubeFetchResult,
  UseCubeFetchLoadOptions,
  UseCubeFetchOptions,
} from '../types';

/**
 * @hidden
 */
export function useLazyDryRun(
  query?: Query | Query[],
  options: UseCubeFetchOptions = {}
): [(loadOptions?: UseCubeFetchLoadOptions) => Promise<void>, CubeFetchResult<DryRunResponse>] {
  const { refetch, ...result } = useCubeFetch<DryRunResponse>('dryRun', {
    ...options,
    query,
    skip: true
  });

  return [refetch, result];
}
