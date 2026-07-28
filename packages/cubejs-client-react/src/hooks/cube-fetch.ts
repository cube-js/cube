import { useContext, useEffect, useState, useRef } from 'react';
import { isQueryPresent } from '@cubejs-client/core';
import type { MetaMethodOptions } from '@cubejs-client/core';

import CubeContext from '../CubeContext';
import useDeepCompareMemoize from './deep-compare-memoize';
import type {
  CubeFetchMethod,
  CubeFetchState,
  UseCubeFetchLoadOptions,
  UseCubeFetchOptions,
  UseCubeFetchResult,
} from '../types';

export function useCubeFetch<T>(
  method: CubeFetchMethod,
  options: UseCubeFetchOptions = {}
): UseCubeFetchResult<T> {
  const context = useContext(CubeContext);
  const mutexRef = useRef({});

  const [response, setResponse] = useState<CubeFetchState<T>>({
    isLoading: false,
    response: null,
  });
  const [error, setError] = useState<Error | null>(null);

  const { skip = false } = options;

  async function load(loadOptions: UseCubeFetchLoadOptions = {}, ignoreSkip = false) {
    const cubeApi = options.cubeApi || context?.cubeApi;
    const query = loadOptions.query || options.query;
    const onlyViews = 'onlyViews' in loadOptions ? loadOptions.onlyViews : options.onlyViews;

    const queryCondition = method === 'meta' ? true : query && isQueryPresent(query);

    if (cubeApi && (ignoreSkip || !skip) && queryCondition) {
      setError(null);
      setResponse({
        isLoading: true,
        response: null,
      });

      const coreOptions: MetaMethodOptions = {
        mutexObj: mutexRef.current,
        mutexKey: method,
        ...(options.baseRequestId ? { baseRequestId: options.baseRequestId } : {}),
        ...(method === 'meta' && onlyViews ? { onlyViews: true } : {})
      };
      const args = method === 'meta' ? [coreOptions] : [query, coreOptions];

      // `method` is a union of overloaded `CubeApi` methods with different
      // signatures, so the call is dispatched dynamically
      const api = cubeApi as any;

      try {
        const response: T = await api[method](...args);

        setResponse({
          response,
          isLoading: false,
        });
      } catch (error: any) {
        setError(error);
        setResponse({
          isLoading: false,
          response: null,
        });
      }
    }
  }

  useEffect(() => {
    load();
  }, useDeepCompareMemoize([Object.keys((options.query as any)?.order || {}), options, context]));

  // `response` is `null` until the first request resolves, which the public
  // `CubeFetchResult` type does not model
  return {
    ...response,
    error,
    refetch: (options?: UseCubeFetchLoadOptions) => load(options, true),
  } as UseCubeFetchResult<T>;
}
