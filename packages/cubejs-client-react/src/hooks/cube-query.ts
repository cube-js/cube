import { useContext, useEffect, useState, useRef } from 'react';
import { isQueryPresent, areQueriesEqual } from '@cubejs-client/core';
import type {
  ProgressResponse,
  QueryRecordType,
  ResultSet,
  UnsubscribeObj,
} from '@cubejs-client/core';

import CubeContext from '../CubeContext';
import useDeepCompareMemoize from './deep-compare-memoize';
import type {
  ProgressCallback,
  ProgressResultWithResponse,
  ReadonlyQueryInput,
  UseCubeQueryOptions,
  UseCubeQueryResult,
} from '../types';

export function useCubeQuery<
  Data,
  QueryInput extends ReadonlyQueryInput = ReadonlyQueryInput
>(
  query: QueryInput,
  options: UseCubeQueryOptions = {}
): UseCubeQueryResult<QueryInput, unknown extends Data ? QueryRecordType<QueryInput> : Data> {
  const mutexRef = useRef({});
  const [currentQuery, setCurrentQuery] = useState<QueryInput | null>(null);
  const [isLoading, setLoading] = useState(!options.skip);
  const [resultSet, setResultSet] = useState<ResultSet<any> | null>(null);
  const [progress, setProgress] = useState<ProgressResponse | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const context = useContext(CubeContext);

  let subscribeRequest: UnsubscribeObj | null = null;

  const progressCallback: ProgressCallback = (progressResult) => setProgress(
    (progressResult as unknown as ProgressResultWithResponse).progressResponse
  );

  async function fetch() {
    const { resetResultSetOnChange } = options;
    const cubeApi = options.cubeApi || context?.cubeApi;

    if (!cubeApi) {
      throw new Error('Cube API client is not provided');
    }

    if (resetResultSetOnChange) {
      setResultSet(null);
    }

    setError(null);
    setLoading(true);

    try {
      const response = await cubeApi.load(query, {
        mutexObj: mutexRef.current,
        mutexKey: 'query',
        progressCallback,
        castNumerics: Boolean(typeof options.castNumerics === 'boolean' ? options.castNumerics : context?.options?.castNumerics),
        ...(options.cache ? { cache: options.cache } : {}),
      });

      setResultSet(response);
      setProgress(null);
    } catch (error: any) {
      setError(error);
      setResultSet(null);
      setProgress(null);
    }

    setLoading(false);
  }

  useEffect(() => {
    const { skip = false, resetResultSetOnChange } = options;

    const cubeApi = options.cubeApi || context?.cubeApi;

    if (!cubeApi) {
      throw new Error('Cube API client is not provided');
    }

    async function loadQuery() {
      if (!skip && isQueryPresent(query)) {
        if (!areQueriesEqual(currentQuery as any, query as any)) {
          if (resetResultSetOnChange == null || resetResultSetOnChange) {
            setResultSet(null);
          }
          setCurrentQuery(query);
        }

        setError(null);
        setLoading(true);

        try {
          if (subscribeRequest) {
            await subscribeRequest.unsubscribe();
            subscribeRequest = null;
          }

          if (options.subscribe) {
            subscribeRequest = cubeApi.subscribe(
              query,
              {
                mutexObj: mutexRef.current,
                mutexKey: 'query',
                progressCallback,
                ...(options.cache ? { cache: options.cache } : {}),
              },
              (e, result) => {
                if (e) {
                  setError(e);
                } else {
                  setResultSet(result);
                }
                setLoading(false);
                setProgress(null);
              }
            );
          } else {
            await fetch();
          }
        } catch (e: any) {
          setError(e);
          setResultSet(null);
          setLoading(false);
          setProgress(null);
        }
      }
    }

    loadQuery();

    return () => {
      if (subscribeRequest) {
        subscribeRequest.unsubscribe();
        subscribeRequest = null;
      }
    };
  }, useDeepCompareMemoize([query, Object.keys((query as any)?.order || {}), options, context]));

  // `progress` is `null` until the first `Continue wait` message and
  // `previousQuery` until the first query runs, neither of which the public
  // result type models
  return {
    isLoading,
    resultSet,
    error,
    progress,
    previousQuery: currentQuery,
    refetch: fetch
  } as UseCubeQueryResult<QueryInput, unknown extends Data ? QueryRecordType<QueryInput> : Data>;
}
