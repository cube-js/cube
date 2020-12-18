import {
  useContext, useEffect, useState, useRef
} from 'react';
import CubeContext from '../CubeContext';
import isQueryPresent from '../isQueryPresent';
import useDeepCompareMemoize from './deep-compare-memoize';

export default function useDryRun(query, options = {}) {
  const context = useContext(CubeContext);
  const mutexRef = useRef({});
  const [response, setResponse] = useState(null);
  const [isLoading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    const { skip = false } = options;

    const cubejsApi = options.cubejsApi || (context && context.cubejsApi);
    if (!cubejsApi) {
      throw new Error('Cube.js API client is not provided');
    }
    
    async function loadQuery() {
      if (!skip && query && isQueryPresent(query)) {
        setError(null);
        setLoading(true);

        try {
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
