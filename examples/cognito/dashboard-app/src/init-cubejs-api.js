import cubejs from '@cubejs-client/core';

const API_URL = 'http://localhost:4000';

export const initCubeClient = (accessToken) => {
 return cubejs({
  apiUrl: `${API_URL}/cubejs-api/v1`,
  headers: {
    Authorization: `Bearer ${accessToken}`
  },
 });
};
