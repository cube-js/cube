import { useContext, useEffect, useState, useRef } from 'react';
import { isQueryPresent } from '@cubejs-client/core';
import type { MetaMethodOptions, Query } from '@cubejs-client/core';

import CubeContext from '../CubeContext';
import useDeepCompareMemoize from './deep-compare-memoize';
import type {
  CubeFetchArgs,
  CubeFetchDispatch,
  CubeFetchMethod,
  CubeFetchState,
  MutexObj,
  UseCubeFetchInternalResult,
  UseCubeFetchLoadOptions,
  UseCubeFetchOptions,
  UseCubeFetchResult,
} from '../types';

export function useCubeFetch<T>(
  method: CubeFetchMethod,
  options?: UseCubeFetchOptions
): UseCubeFetchResult<T>;

export function useCubeFetch<T>(
  method: CubeFetchMethod,
  options: UseCubeFetchOptions = {}
): UseCubeFetchInternalResult<T> {
  const context = useContext(CubeContext);
  const mutexRef = useRef<MutexObj>({});

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
      const args: CubeFetchArgs = method === 'meta' ? [coreOptions] : [query!, coreOptions];
      // `method` picks between overloaded `CubeApi` methods, so the call is
      // dispatched dynamically while keeping `cubeApi` as the receiver
      const fetchMethod = cubeApi[method] as CubeFetchDispatch;

      try {
        const response = await fetchMethod.apply(cubeApi, args) as T;

        setResponse({
          response,
          isLoading: false,
        });
      } catch (error) {
        setError(error as Error);
        setResponse({
          isLoading: false,
          response: null,
        });
      }
    }
  }

  useEffect(() => {
    load();
    // `order` is read off a single query; an array of queries does not carry it
  }, useDeepCompareMemoize([
    Object.keys((options.query as Query | undefined)?.order || {}),
    options,
    context,
  ]));

  return {
    ...response,
    error,
    refetch: (options?: UseCubeFetchLoadOptions) => load(options, true),
  };
}
