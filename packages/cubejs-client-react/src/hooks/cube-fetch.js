import { useContext, useEffect, useState, useRef } from 'react';
import { isQueryPresent } from '@cubejs-client/core';

import CubeContext from '../CubeContext';
import useDeepCompareMemoize from './deep-compare-memoize';
import { useIsMounted } from './is-mounted';

export function useCubeFetch(method, options = {}) {
  const isMounted = useIsMounted();
  const context = useContext(CubeContext);
  const mutexRef = useRef({});

  const [response, setResponse] = useState({
    isLoading: false,
    response: null,
  });
  const [error, setError] = useState(null);

  const { skip = false } = options;

  async function load(loadOptions = {}, ignoreSkip = false) {
    const cubejsApi = options.cubejsApi || context?.cubejsApi;
    const query = loadOptions.query || options.query;

    if (!cubejsApi) {
      throw new Error('Cube.js API client is not provided');
    }

    if ((ignoreSkip || !skip) && query && isQueryPresent(query)) {
      setError(null);
      setResponse({
        isLoading: true,
        response: null,
      });

      try {
        const response = await cubejsApi[method](query, {
          mutexObj: mutexRef.current,
          mutexKey: method,
        });

        if (isMounted()) {
          setResponse({
            response,
            isLoading: false,
          });
        }
      } catch (err) {
        if (isMounted()) {
          setError(err);
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
