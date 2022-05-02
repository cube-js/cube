import { useContext, useEffect, useState, useRef } from 'react';
import { isQueryPresent, areQueriesEqual } from '@cubejs-client/core';

import CubeContext from '../CubeContext';
import useDeepCompareMemoize from './deep-compare-memoize';

export function useCubeQuery(query, options = {}) {
  const mutexRef = useRef({});
  const isMounted = useRef(true);
  const [currentQuery, setCurrentQuery] = useState(null);
  const [isLoading, setLoading] = useState(false);
  const [resultSet, setResultSet] = useState(null);
  const [progress, setProgress] = useState(null);
  const [error, setError] = useState(null);
  const context = useContext(CubeContext);

  let subscribeRequest = null;

  const progressCallback = ({ progressResponse }) => setProgress(progressResponse);

  async function fetch() {
    const { resetResultSetOnChange } = options;
    const cubejsApi = options.cubejsApi || context?.cubejsApi;

    if (!cubejsApi) {
      throw new Error('Cube.js API client is not provided');
    }

    if (resetResultSetOnChange) {
      setResultSet(null);
    }

    setError(null);
    setLoading(true);

    try {
      const response = await cubejsApi.load(query, {
        mutexObj: mutexRef.current,
        mutexKey: 'query',
        progressCallback,
      });

      if (isMounted.current) {
        setResultSet(response);
        setProgress(null);
      }
    } catch (error) {
      if (isMounted.current) {
        setError(error);
        setResultSet(null);
        setProgress(null);
      }
    }

    if (isMounted.current) {
      setLoading(false);
    }
  }

  useEffect(() => {
    return () => {
      isMounted.current = false;
    };
  }, []);

  useEffect(() => {
    const { skip = false, resetResultSetOnChange } = options;

    const cubejsApi = options.cubejsApi || context?.cubejsApi;

    if (!cubejsApi) {
      throw new Error('Cube.js API client is not provided');
    }

    async function loadQuery() {
      if (!skip && isQueryPresent(query)) {
        if (!areQueriesEqual(currentQuery, query)) {
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
            subscribeRequest = cubejsApi.subscribe(
              query,
              {
                mutexObj: mutexRef.current,
                mutexKey: 'query',
                progressCallback,
              },
              (e, result) => {
                if (isMounted.current) {
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
            await fetch();
          }
        } catch (e) {
          if (isMounted.current) {
            setError(e);
            setResultSet(null);
            setLoading(false);
            setProgress(null);
          }
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
  }, useDeepCompareMemoize([query, Object.keys((query && query.order) || {}), options, context]));

  return {
    isLoading,
    resultSet,
    error,
    progress,
    previousQuery: currentQuery,
    refetch: fetch
  };
}
