import { useContext, useEffect, useState, useRef } from 'react';
import { isQueryPresent, areQueriesEqual } from '@cubejs-client/core';
import type {
  DeeplyReadonly,
  ProgressResponse,
  Query,
  QueryRecordType,
  ResultSet,
  UnsubscribeObj,
} from '@cubejs-client/core';

import CubeContext from '../CubeContext';
import useDeepCompareMemoize from './deep-compare-memoize';
import type {
  MutexObj,
  ProgressCallback,
  ProgressResultWithResponse,
  ReadonlyQueryInput,
  UseCubeQueryInternalResult,
  UseCubeQueryOptions,
  UseCubeQueryResult,
} from '../types';

/**
 * A React hook for executing Cube.js queries
 * ```js
 * import React from 'react';
 * import { Table } from 'antd';
 * import { useCubeQuery }  from '@cubejs-client/react';
 *
 * export default function App() {
 *   const { resultSet, isLoading, error, progress } = useCubeQuery({
 *     measures: ['Orders.count'],
 *     dimensions: ['Orders.createdAt.month'],
 *   });
 *
 *   if (isLoading) {
 *     return <div>{progress?.stage || 'Loading...'}</div>;
 *   }
 *
 *   if (error) {
 *     return <div>{error.toString()}</div>;
 *   }
 *
 *   if (!resultSet) {
 *     return null;
 *   }
 *
 *   const dataSource = resultSet.tablePivot();
 *   const columns = resultSet.tableColumns();
 *
 *   return <Table columns={columns} dataSource={dataSource} />;
 * }
 *
 * ```
 * @order 1
 * @stickyTypes
 */
export function useCubeQuery<
  Data,
  QueryInput extends ReadonlyQueryInput = ReadonlyQueryInput
>(
  query: QueryInput,
  options?: UseCubeQueryOptions
): UseCubeQueryResult<QueryInput, unknown extends Data ? QueryRecordType<QueryInput> : Data>;

export function useCubeQuery(
  query: ReadonlyQueryInput,
  options: UseCubeQueryOptions = {}
): UseCubeQueryInternalResult {
  const mutexRef = useRef<MutexObj>({});
  const [currentQuery, setCurrentQuery] = useState<ReadonlyQueryInput | null>(null);
  const [isLoading, setLoading] = useState(!options.skip);
  const [resultSet, setResultSet] = useState<ResultSet | null>(null);
  const [progress, setProgress] = useState<ProgressResponse | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const context = useContext(CubeContext);
  const requestIdRef = useRef(0);

  let subscribeRequest: UnsubscribeObj | null = null;

  function isCurrentRequest(requestId: number) {
    return requestId === requestIdRef.current;
  }

  function createProgressCallback(requestId: number): ProgressCallback {
    return (progressResult) => {
      if (isCurrentRequest(requestId)) {
        // `progressResponse` is not part of the public `ProgressResult` API
        setProgress(
          (progressResult as unknown as ProgressResultWithResponse).progressResponse
        );
      }
    };
  }

  async function fetchQuery(requestId: number) {
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
        progressCallback: createProgressCallback(requestId),
        castNumerics: Boolean(typeof options.castNumerics === 'boolean' ? options.castNumerics : context?.options?.castNumerics),
        ...(options.cache ? { cache: options.cache } : {}),
      });

      if (isCurrentRequest(requestId)) {
        setResultSet(response);
        setProgress(null);
      }
    } catch (loadError) {
      if (isCurrentRequest(requestId)) {
        setError(loadError as Error);
        setResultSet(null);
        setProgress(null);
      }
    }

    if (isCurrentRequest(requestId)) {
      setLoading(false);
    }
  }

  async function fetch() {
    const requestId = ++requestIdRef.current;

    await fetchQuery(requestId);
  }

  useEffect(() => {
    const { skip = false, resetResultSetOnChange } = options;

    const cubeApi = options.cubeApi || context?.cubeApi;

    if (!cubeApi) {
      throw new Error('Cube API client is not provided');
    }

    async function loadQuery() {
      if (!skip && isQueryPresent(query)) {
        const requestId = ++requestIdRef.current;

        // `areQueriesEqual` is declared for a single query, and reads no more
        // than `order` off one when given an array of queries
        const previousQuery = currentQuery as DeeplyReadonly<Query> | null;

        if (!areQueriesEqual(previousQuery, query as DeeplyReadonly<Query>)) {
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
                progressCallback: createProgressCallback(requestId),
                ...(options.cache ? { cache: options.cache } : {}),
              },
              (e, result) => {
                if (isCurrentRequest(requestId)) {
                  if (e) {
                    setError(e);
                  } else {
                    setResultSet(result);
                  }
                  setLoading(false);
                  setProgress(null);
                }
              }
            );
          } else {
            await fetchQuery(requestId);
          }
        } catch (e) {
          if (isCurrentRequest(requestId)) {
            setError(e as Error);
            setResultSet(null);
            setLoading(false);
            setProgress(null);
          }
        }
      }
    }

    loadQuery();

    return () => {
      requestIdRef.current += 1;

      if (subscribeRequest) {
        subscribeRequest.unsubscribe();
        subscribeRequest = null;
      }
    };
    // `order` is read off a single query; an array of queries does not carry it
  }, useDeepCompareMemoize([
    query,
    Object.keys((query as DeeplyReadonly<Query> | undefined)?.order || {}),
    options,
    context,
  ]));

  return {
    isLoading,
    resultSet,
    error,
    progress,
    previousQuery: currentQuery,
    refetch: fetch
  };
}
