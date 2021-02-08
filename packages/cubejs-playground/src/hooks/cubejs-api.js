import cubejs from '@cubejs-client/core';
import { useMemo } from 'react';

export default function useCubejsApi(apiUrl, token) {
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
