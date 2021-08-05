import { useMemo } from 'react';
import cubejs from '@cubejs-client/core';

export function useCubejsApi(apiUrl: string | null, token: string | null) {
  return useMemo(() => {
    if (!token || !apiUrl || token === 'undefined') {
      return null;
    }

    return cubejs(token, {
      apiUrl,
    });
  }, [apiUrl, token]);
}
