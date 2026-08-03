import cubeApi from '@cubejs-client/core';

export function createCubeApi() {
  return cubeApi('token', {
    apiUrl: 'http://localhost:4000'
  });
}
