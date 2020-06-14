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
  const [error, setError] = useState(null);
  const context = useContext(CubeContext);

  let subscribeRequest = null;

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
          if (options.subscribe) {
            subscribeRequest = cubejsApi.subscribe(query, {
              mutexObj: mutexRef.current,
              mutexKey: 'query'
            }, (e, result) => {
              if (e) {
                setError(e);
              } else {
                setResultSet(result);
              }
              setLoading(false);
            });
          } else {
            setResultSet(await cubejsApi.load(query, {
              mutexObj: mutexRef.current,
              mutexKey: 'query'
            }));
            setLoading(false);
          }
        } catch (e) {
          setError(e);
          setLoading(false);
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

  return { isLoading, resultSet, error };
};
