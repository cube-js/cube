import { useContext, useEffect, useState, useRef } from 'react';
import { isQueryPresent, areQueriesEqual } from '@cubejs-client/core';

import CubeContext from '../CubeContext';
import useDeepCompareMemoize from './deep-compare-memoize';
import { useIsMounted } from './is-mounted';

export function useCubeQuery(query, options = {}) {
  const mutexRef = useRef({});
  const isMounted = useIsMounted();
  const [currentQuery, setCurrentQuery] = useState(null);
  const [isLoading, setLoading] = useState(false);
  const [resultSet, setResultSet] = useState(null);
  const [progress, setProgress] = useState(null);
  const [error, setError] = useState(null);
  const context = useContext(CubeContext);

  let subscribeRequest = null;

  const progressCallback = ({ progressResponse }) => setProgress(progressResponse);
  
  useEffect(() => {
    if (options.cubejsApi && !options.cubeApi) {
      console.warn('"cubejsApi" is deprecated and will be removed in the following version. Use "cubeApi" instead.');
    }
  }, [options.cubeApi, options.cubejsApi]);

  async function fetch() {
    const { resetResultSetOnChange } = options;
    const cubeApi = options.cubeApi || options.cubejsApi || context?.cubeApi || context?.cubejsApi;

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
        castNumerics: Boolean(typeof options.castNumerics === 'boolean' ? options.castNumerics : context?.options?.castNumerics)
      });

      if (isMounted()) {
        setResultSet(response);
        setProgress(null);
      }
    } catch (error) {
      if (isMounted()) {
        setError(error);
        setResultSet(null);
        setProgress(null);
      }
    }

    if (isMounted()) {
      setLoading(false);
    }
  }

  useEffect(() => {
    const { skip = false, resetResultSetOnChange } = options;

    const cubeApi = options.cubeApi || options.cubejsApi || context?.cubeApi || context?.cubejsApi;

    if (!cubeApi) {
      throw new Error('Cube API client is not provided');
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
            subscribeRequest = cubeApi.subscribe(
              query,
              {
                mutexObj: mutexRef.current,
                mutexKey: 'query',
                progressCallback,
              },
              (e, result) => {
                if (isMounted()) {
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
          if (isMounted()) {
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
