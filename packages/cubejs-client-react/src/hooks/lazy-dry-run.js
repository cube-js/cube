import { useCubeFetch } from './cube-fetch';

export function useLazyDryRun(query, options = {}) {
  const { refetch, ...result } = useCubeFetch('dryRun', {
    ...options,
    query,
    skip: true
  });

  return [refetch, result];
}
