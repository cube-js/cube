import {
  useContext, useEffect, useState, useRef
} from 'react';
import { equals } from 'ramda';
import CubeContext from '../CubeContext';
import isQueryPresent from '../isQueryPresent';
import useDeepCompareMemoize from './deep-compare-memoize';

export default function useDryRun(query, options = {}) {
  const context = useContext(CubeContext);
  const mutexRef = useRef({});
  const [response, setResponse] = useState(null);
  const [currentQuery, setCurrentQuery] = useState(null);
  const [isLoading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    const { skip = false } = options;

    async function loadQuery() {
      if (!skip && query && isQueryPresent(query)) {
        if (!equals(currentQuery, query)) {
          setError(null);
          setCurrentQuery(query);
        }
        setLoading(true);
        try {
          const cubejsApi = options.cubejsApi || (context && context.cubejsApi);

          if (!cubejsApi) {
            throw new Error('Cube.js API client is not provided');
          }
          
          setResponse(
            await cubejsApi.dryRun(query, {
              mutexObj: mutexRef.current,
              mutexKey: 'dry-run',
            })
          );
          setLoading(false);
        } catch (err) {
          setError(err);
          setLoading(false);
        }
      }
    }

    loadQuery();
  }, useDeepCompareMemoize([query, Object.keys((query && query.order) || {}), options, context]));

  return {
    isLoading,
    error,
    response
  };
}
