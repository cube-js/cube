import {
  useContext, useEffect, useState, useRef
} from 'react';
import { equals } from 'ramda';
import CubeContext from './CubeContext';
import isQueryPresent from './isQueryPresent';
import useDeepCompareMemoize from './useDeepCompareMemoize';

export default (query, options = {}) => {
  const mutexRef = useRef({});
  const [currentQuery, setCurrentQuery] = useState(null);
  const [isLoading, setLoading] = useState(false);
  const [resultSet, setResultSet] = useState(null);
  const [progress, setProgress] = useState(null);
  const [error, setError] = useState(null);
  const context = useContext(CubeContext);

  let subscribeRequest = null;

  const progressCallback = ({ progressResponse }) => setProgress(progressResponse);

  useEffect(() => {
    const { skip = false, resetResultSetOnChange } = options;

    async function loadQuery() {
      if (!skip && query && isQueryPresent(query)) {
        const hasOrderChanged = !equals(
          Object.keys(currentQuery && currentQuery.order || {}),
          Object.keys(query.order || {})
        );
        
        if (hasOrderChanged || !equals(currentQuery, query)) {
          if (resetResultSetOnChange == null || resetResultSetOnChange) {
            setResultSet(null);
          }
          setError(null);
          setCurrentQuery(query);
        }
        setLoading(true);
        try {
          if (subscribeRequest) {
            await subscribeRequest.unsubscribe();
            subscribeRequest = null;
          }
          const cubejsApi = options.cubejsApi || context && context.cubejsApi;
          
          if (!cubejsApi) {
            throw new Error('Cube.js API client is not provided');
          }
          
          if (options.subscribe) {
            subscribeRequest = cubejsApi.subscribe(query, {
              mutexObj: mutexRef.current,
              mutexKey: 'query',
              progressCallback
            }, (e, result) => {
              if (e) {
                setError(e);
              } else {
                setResultSet(result);
              }
              setLoading(false);
              setProgress(null);
            });
          } else {
            setResultSet(await cubejsApi.load(query, {
              mutexObj: mutexRef.current,
              mutexKey: 'query',
              progressCallback
            }));
            setLoading(false);
            setProgress(null);
          }
        } catch (e) {
          setError(e);
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
  }, useDeepCompareMemoize([
    query,
    Object.keys(query && query.order || {}),
    options,
    context
  ]));

  return {
    isLoading,
    resultSet,
    error,
    progress
  };
};
