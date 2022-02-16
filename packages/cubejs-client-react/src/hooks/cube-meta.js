import { useCubeFetch } from './cube-fetch';

export function useCubeMeta(options = {}) {
  return useCubeFetch('meta', options);
}
