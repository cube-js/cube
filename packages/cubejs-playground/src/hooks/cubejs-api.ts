import { useMemo } from 'react';
import cubejs from '@cubejs-client/core';

export function useCubejsApi(apiUrl, token) {
  const cubejsApiInstance = useMemo(() => {
    if (!token || !apiUrl || token === 'undefined') {
      return null;
    }

    return cubejs(token, {
      apiUrl,
    });
  }, [apiUrl, token]);

  return cubejsApiInstance;
}
