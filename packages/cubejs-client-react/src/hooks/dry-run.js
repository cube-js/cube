import { useCubeFetch } from './cube-fetch';

export function useDryRun(query, options = {}) {
  return useCubeFetch('dryRun', {
    ...options,
    query
  })
}
