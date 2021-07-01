import { useCubeFetch } from './cube-fetch';

export function useCubeSql(query, options = {}) {
  return useCubeFetch('sql', {
    ...options,
    query
  });
}
