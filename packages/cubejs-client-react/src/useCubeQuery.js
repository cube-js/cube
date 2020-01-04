import {
  useContext, useEffect, useState
} from 'react';
import { equals } from 'ramda';
import CubeContext from './CubeContext';
import isQueryPresent from './isQueryPresent';

export default (query, options = {}) => {
  const [mutexObj] = useState({});
  const [currentQuery, setCurrentQuery] = useState(null);
  const [isLoading, setLoading] = useState(false);
  const [resultSet, setResultSet] = useState(null);
  const [error, setError] = useState(null);
  const context = useContext(CubeContext);

  let subscribeRequest = null;

  useEffect(() => {
    async function loadQuery() {
      if (query && isQueryPresent(query)) {
        if (!equals(currentQuery, query)) {
          setResultSet(null);
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
              mutexObj,
              mutexKey: 'query'
            }, (e, result) => {
              setLoading(false);
              if (e) {
                setError(e);
              } else {
                setResultSet(result);
              }
            });
          } else {
            setResultSet(await cubejsApi.load(query, {
              mutexObj,
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
  }, [JSON.stringify(query), options.cubejsApi, context]);

  return { isLoading, resultSet, error };
};
