import { useEffect, useState, useRef } from 'react';
import { isQueryPresent } from '@cubejs-client/core';

import useDeepCompareMemoize from './deep-compare-memoize';
import { useIsMounted } from './is-mounted';
import { useCubeApi } from './cube-api';

export function useCubeFetch(method, options = {}) {
  const isMounted = useIsMounted();
  const cubejsApi = useCubeApi(options.cubejsApi);
  const mutexRef = useRef({});

  const [response, setResponse] = useState({
    isLoading: false,
    response: null,
  });
  const [error, setError] = useState(null);

  const { skip = false } = options;

  async function load(loadOptions = {}, ignoreSkip = false) {
    const query = loadOptions.query || options.query;

    const queryCondition = method === 'meta' ? true : query && isQueryPresent(query);

    if (cubejsApi && (ignoreSkip || !skip) && queryCondition) {
      setError(null);
      setResponse({
        isLoading: true,
        response: null,
      });

      const coreOptions = {
        mutexObj: mutexRef.current,
        mutexKey: method,
      };
      const args = method === 'meta' ? [coreOptions] : [query, coreOptions];

      try {
        const response = await cubejsApi[method](...args);

        if (isMounted()) {
          setResponse({
            response,
            isLoading: false,
          });
        }
      } catch (error) {
        if (isMounted()) {
          setError(error);
          setResponse({
            isLoading: false,
            response: null,
          });
        }
      }
    }
  }

  useEffect(() => {
    load();
  }, useDeepCompareMemoize([Object.keys(options.query?.order || {}), options, context]));

  return {
    ...response,
    error,
    refetch: (options) => load(options, true),
  };
}
