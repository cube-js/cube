import cubejs from '@cubejs-client/core';

export const initCubeClient = (accessToken) => {
 return cubejs({
  apiUrl: `${process.env.REACT_APP_API_URL}/cubejs-api/v1`,
  headers: {
    Authorization: `Bearer ${accessToken}`
  },
 });
};
