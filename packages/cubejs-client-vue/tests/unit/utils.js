import cubejsApi from '@cubejs-client/core';

export function createCubejsApi() {
  return cubejsApi('token', {
    apiUrl: 'http://localhost:4000'
  });
}
