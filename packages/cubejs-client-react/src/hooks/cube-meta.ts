import type { Meta } from '@cubejs-client/core';

import { useCubeFetch } from './cube-fetch';
import type { CubeMetaFetchOptions, UseCubeFetchResult } from '../types';

export function useCubeMeta(options: CubeMetaFetchOptions = {}): UseCubeFetchResult<Meta> {
  return useCubeFetch<Meta>('meta', options);
}
