import cubejsApi from '@cubejs-client/core';

export function createCubeApi() {
  return cubejsApi('token', {
    apiUrl: 'http://localhost:4000'
  });
}
